import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import localClasses.ClassifierTrainer;
import localClasses.ResultsEvaluator;
import localClasses.TxtToCsvConverter;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateKeyPairRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;


public class Main {

    private static String inputPath = "inputPath";
    static final String csvPath = "vectors.csv";

    private static String jarfileNme = "jarfileNme";


    private static String txtHypernymPath = "hypernym.txt";

    private static final String txtVectorsPath = "vectors.txt";

    private static  String dpmin = "5";
    private static S3Client s3;
    private static final Region region = Region.US_EAST_1;
    private static final AWSCredentials credentials=new DefaultAWSCredentialsProviderChain().getCredentials();

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("please provide all the args - input, hypmernym, DPMin, jarfile, name of bucket");
            exit(1);
        }
        jarfileNme =args[3];
        String jarBucketName = args[4];
        txtHypernymPath = args[1];
        inputPath = args[0];
        dpmin = args[2];
        uploadJar("shell.sh", jarBucketName +"4");
        uploadJar("hello.py", jarBucketName +"5");
        uploadJar(jarfileNme, jarBucketName);
        uploadJar(txtHypernymPath, jarBucketName +"2");
        uploadJar(inputPath, jarBucketName +"3");

        System.out.println("init cluster");
        if(initHadoopJar(jarBucketName)) {

            //after those two jobs finish, we have S3 bucket containing the data we would like to learn from

            deleteBucketAndContents(jarBucketName + "2");
            deleteBucketAndContents(jarBucketName + "3");
            deleteBucketAndContents(jarBucketName + "4");
            deleteBucketAndContents(jarBucketName + "5");
            downloadFromS3(jarBucketName,"jobs/" + "Create Vectors/" + "part-r-00000","output");
//            Instances data = downloadAndConvert(jarBucketName);
            ArrayList<String> orderedNounPairs = new TxtToCsvConverter().convert("output",csvPath);
            ConverterUtils.DataSource source = new ConverterUtils.DataSource(csvPath);
            Instances  data = source.getDataSet();
            data.setClassIndex(data.numAttributes() - 1);
            StringToWordVector filter = new StringToWordVector();
            filter.setInputFormat(data);
            data = Filter.useFilter(data, filter);
            ClassifierTrainer trainer = new ClassifierTrainer();
            Classifier nb = trainer.train(data);
            new ResultsEvaluator(orderedNounPairs).evaluateResults(data, nb);
        }
        else
        {
            System.out.println("Error during cluster job, please get to the buckets to investigate");
        }
    }

    private static void downloadFromS3(String bucketName,String key,String outputPath) throws IOException {
        //here we are downloading the map-reduce jobs result and saving it as the data for the classifier

        s3 = S3Client.builder().region(Region.US_EAST_1).build();

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        try {
            File f = new File(txtVectorsPath);
            if(f.delete()) {
                System.out.println("output file replaced with new one");
            }
        }
        catch(Exception e) {
            //No file, all good

        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(String.valueOf(region))
                .build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        File file = new File(outputPath);
        file.createNewFile();
        FileUtils.copyInputStreamToFile(s3Object.getObjectContent(), file);
    }

    private static void uploadJar(String jarFilePath, String bucketName) {

        s3 = S3Client.builder().region(region).build();
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

        try {
            s3.headBucket(headBucketRequest);


        } catch (Exception e) {
            File managerJar = new File(jarFilePath);
            SetupS3(s3, bucketName);
            PutObjectRequest requestManager = PutObjectRequest.builder()
                    .bucket(bucketName).key(jarFilePath).build();
            s3.putObject(requestManager, RequestBody.fromFile(managerJar));
        }

    }

    public static void SetupS3(S3Client s3Client, String bucketName) {
        try {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucketName).build();
            s3Client.createBucket(request);
            System.out.println("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName + " is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            exit(1);
        }
    }

    public static void createKeyPair(String keyName) {
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        try {
            CreateKeyPairRequest request = CreateKeyPairRequest.builder()
                    .keyName(keyName).build();

            ec2.createKeyPair(request);
            System.out.printf(
                    "Successfully created key pair named %s",
                    keyName);

        } catch (Ec2Exception e) {

        }
    }

    public static boolean initHadoopJar(String JarBucketName) throws InterruptedException {

        HadoopJarStepConfig hadoopJarSteppip = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("pip", "install", "nltk");

        StepConfig stepConfigpip = new StepConfig()
                .withName("Install NLTK package")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(hadoopJarSteppip);

        List<StepConfig> steps = new ArrayList<>();

        ScriptBootstrapActionConfig bootstrapAction = new ScriptBootstrapActionConfig()
                .withPath("s3://"+JarBucketName+"4/shell.sh")
                ;

        BootstrapActionConfig bootstrapConfig = new BootstrapActionConfig()
                .withName("My Bootstrap Action")
                .withScriptBootstrapAction(bootstrapAction);

        List<BootstrapActionConfig> bootstrapActions = new ArrayList<>();
        bootstrapActions.add(bootstrapConfig);


        ///end of the bootstrapaddition


        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        createKeyPair("yourkey2");
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://"+JarBucketName+"/"+jarfileNme) // This should be a full map reduce application.
                .withMainClass("jobs.JobsRunnable");


        hadoopJarStep =  hadoopJarStep.withArgs("s3://" + JarBucketName+"3/"+inputPath,"s3://"+JarBucketName,"s3://" + JarBucketName+"2/"+txtHypernymPath,dpmin);
        StepConfig stepConfig = new StepConfig()
                .withName("3gram")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        steps.add(stepConfig);
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)

                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.10.1").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("3gramNewInputLine")
                .withInstances(instances)
                .withSteps(steps).withReleaseLabel("emr-5.36.0")
                .withBootstrapActions(bootstrapActions)
                .withLogUri("s3n://" + JarBucketName + "/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
        return waiterJob(jobFlowId,mapReduce);
    }

    public static boolean waiterJob(String jobFlowId,AmazonElasticMapReduce mapReduce) throws InterruptedException {
        boolean finished = false;
        while (!finished) {
            DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(jobFlowId);
            DescribeClusterResult describeClusterResult = mapReduce.describeCluster(describeClusterRequest);
            Cluster cluster = describeClusterResult.getCluster();

            ClusterStatus status = cluster.getStatus();
            if (status.getState().equals(ClusterState.TERMINATED.toString())) {
                System.out.println("Job flow terminated with state: " + status.getState());
                return true;
            } else {
                if(status.getState().equals(ClusterState.TERMINATED_WITH_ERRORS.toString()))
                {
                    return false;
                }
                else {
                    System.out.println("Job flow is still running with state: " + status.getState());
                }
            }

            // Wait for a few seconds before checking the status again
            Thread.sleep(5000);
        }
        return true;

    }
    public static void deleteBucketAndContents( String bucketName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region.toString())
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        // Delete all objects in the bucket
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName);
        ListObjectsV2Result listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
        List<S3ObjectSummary> objectSummaries = listObjectsResult.getObjectSummaries();
        while (!objectSummaries.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(objectSummaries.stream().map(S3ObjectSummary::getKey).toArray(String[]::new));
            s3Client.deleteObjects(deleteObjectsRequest);
            objectSummaries.clear();
            listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
            objectSummaries.addAll(listObjectsResult.getObjectSummaries());
        }

        // Delete the bucket
        s3Client.deleteBucket(new DeleteBucketRequest(bucketName));
    }
}
