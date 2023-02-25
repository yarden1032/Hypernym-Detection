import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import java.io.File;
import java.io.IOException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3Object;
import localClasses.ResultsEvaluator;
import localClasses.ClassifierTrainer;
import localClasses.TxtToArffConverter;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import software.amazon.awssdk.services.s3.model.*;
import org.python.core.Options;
import static java.lang.System.exit;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateKeyPairRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.s3.S3Client;
import org.python.core.Options;

import javax.script.*;
import java.io.*;
import java.nio.file.Paths;
import java.util.List;
import java.io.StringWriter;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.ArrayList;
import java.util.List;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


//
//    public static void runEMRCluster(String accessKey, String secretKey, String region, String serviceRole, String jobFlowRole, String logUri) {
//
//        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
//
//        AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//                .withRegion(region)
//                .build();
//
//        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
//                .withJar("command-runner.jar")
//                .withArgs("pip", "install", "nltk");
//
//        StepConfig stepConfig = new StepConfig()
//                .withName("Install NLTK package")
//                .withActionOnFailure("TERMINATE_JOB_FLOW")
//                .withHadoopJarStep(hadoopJarStep);
//
//        List<StepConfig> steps = new ArrayList<>();
//        steps.add(stepConfig);
//
//        ScriptBootstrapActionConfig bootstrapAction = new ScriptBootstrapActionConfig()
//                .withPath("s3://my-bucket/my-script.sh")
//                .withArgs("arg1", "arg2");
//
//        BootstrapActionConfig bootstrapConfig = new BootstrapActionConfig()
//                .withName("My Bootstrap Action")
//                .withScriptBootstrapAction(bootstrapAction);
//
//        List<BootstrapActionConfig> bootstrapActions = new ArrayList<>();
//        bootstrapActions.add(bootstrapConfig);
//
//        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
//                .withInstanceCount(2)
//                .withMasterInstanceType("m5.xlarge")
//                .withSlaveInstanceType("m5.xlarge")
//                .withHadoopVersion("3.2.1")
//                .withEc2KeyName("my-key-pair")
//                .withKeepJobFlowAliveWhenNoSteps(true)
//                .withTerminationProtected(false)
//                .withEc2SubnetId("subnet-12345678")
//                .withEmrManagedMasterSecurityGroup("sg-12345678")
//                .withEmrManagedSlaveSecurityGroup("sg-12345678");
////
////        Configuration configuration = new Configuration()
////                .withClassification("spark-defaults")
////                .withProperties(new Configuration().withProperty("spark.executor.memory", "4g"));
//
//        List<Application> applications = new ArrayList<>();
//        applications.add(new Application().withName("Spark"));
//
//        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//                .withName("My EMR Cluster")
//                .withReleaseLabel("emr-6.2.0")
//                .withApplications(applications)
//                .withServiceRole(serviceRole)
//                .withJobFlowRole(jobFlowRole)
//                .withLogUri(logUri)
//                .withSteps(steps)
//                .withBootstrapActions(bootstrapActions)
////                .withConfigurations(configuration)
//                .withInstances(instances)
//               ;
//        RunJobFlowResult runFlowResult = emrClient.runJobFlow(runFlowRequest);
//        System.out.println("EMR Cluster ID: " + runFlowResult.getJobFlowId());
//    }


public class Main {

    private static String JarBucketName = "jarbucket";

    private static String inputPath = "inputPath";

    private static String jarfileNme = "jarfileNme";


    private static String txtHypernymPath = "hypernym.txt";

    private static final String txtVectorsPath = "vectors.txt";

    private static final String arffVectorsPath = "vectors.arff";
    private static  String dpmin = "5";
    private static S3Client s3;
    private static Region region = Region.US_EAST_1;
    private static final AWSCredentials credentials=new DefaultAWSCredentialsProviderChain().getCredentials();
    public static void printResults(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }
    public static void main(String[] args) throws Exception {

//        Process process = Runtime.getRuntime().exec("pip install nltk");
//        printResults(process);
//        we should recieve as input the DPmin value, input of the biarcs data set and the location of the
        //hypernym.txt file (can be local)
        if (args.length < 4) {
            System.out.println("please provide all the args - input, hypmernym, DPMin, jarfile, name of bucket");
            exit(1);
        }
        jarfileNme =args[3];
        JarBucketName=args[4];
        txtHypernymPath = args[1];
         inputPath = args[0];
         dpmin = args[2];
        uploadJar("shell.sh", JarBucketName+"4");
        uploadJar("hello.py", JarBucketName+"5");
        uploadJar(jarfileNme, JarBucketName);
        uploadJar(jarfileNme, JarBucketName);
        uploadJar(txtHypernymPath, JarBucketName+"2");
        uploadJar(inputPath, JarBucketName+"3");

        System.out.println("init cluster");
        initHadoopJar(JarBucketName);

        //after those two jobs finish, we have S3 bucket containing the data we would like to learn from
        downloadFromS3(JarBucketName,"jobs/" + "Create Vectors/" + "part-r-00000","output");
        new TxtToArffConverter().convert("output",arffVectorsPath);
        ConverterUtils.DataSource source = new ConverterUtils.DataSource(arffVectorsPath);
        Instances data = source.getDataSet();
        ClassifierTrainer trainer = new ClassifierTrainer();
        Classifier nb = trainer.train(data);
        new ResultsEvaluator().evaluateResults(data, nb);
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


    public static void downloadS3Folder(String bucketName, String folderKey, String downloadPath) throws IOException {


        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(String.valueOf(region))
                .build();

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(folderKey);

        ListObjectsV2Result objects = s3Client.listObjectsV2(String.valueOf(listObjectsRequest));

        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
            String key = objectSummary.getKey();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));
            File file = new File(downloadPath + key);
            file.getParentFile().mkdirs();
            file.createNewFile();
            FileUtils.copyInputStreamToFile(s3Object.getObjectContent(), file);
        }

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
            SetupS3(s3, bucketName, region);
            PutObjectRequest requestManager = PutObjectRequest.builder()
                    .bucket(bucketName).key(jarFilePath).build();
            s3.putObject(requestManager, RequestBody.fromFile(managerJar));
        }

    }

    public static void SetupS3(S3Client s3Client, String bucketName, Region region) {
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

    public static void initHadoopJar(String JarBucketName) throws InterruptedException {

        HadoopJarStepConfig hadoopJarSteppip = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("pip", "install", "nltk");

        StepConfig stepConfigpip = new StepConfig()
                .withName("Install NLTK package")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(hadoopJarSteppip);

        List<StepConfig> steps = new ArrayList<>();
//        steps.add(stepConfigpip);

        ScriptBootstrapActionConfig bootstrapAction = new ScriptBootstrapActionConfig()
                .withPath("s3://scriptbucketton/shell.sh")
//                .withArgs("arg1", "arg2")
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
//        TERMINATE_JOB_FLOW
                .withActionOnFailure("CANCEL_AND_WAIT");
        steps.add(stepConfig);
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)

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
        waiterJob(jobFlowId,mapReduce);
    }

    public static void waiterJob(String jobFlowId,AmazonElasticMapReduce mapReduce) throws InterruptedException {
        boolean finished = false;
        while (!finished) {
            DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(jobFlowId);
            DescribeClusterResult describeClusterResult = mapReduce.describeCluster(describeClusterRequest);
            Cluster cluster = describeClusterResult.getCluster();

            ClusterStatus status = cluster.getStatus();
            if (status.getState().equals(ClusterState.TERMINATED.toString())) {
                System.out.println("Job flow terminated with state: " + status.getState());
                finished = true;
            } else {
                System.out.println("Job flow is still running with state: " + status.getState());
            }

            // Wait for a few seconds before checking the status again
            Thread.sleep(5000);
        }
    }
    public static void cleanUp( String bucketName, String keyName) {
      /*  System.out.println("Cleaning up...");
        try {
            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(bucketName).build();
            ListObjectsResponse res = s3.listObjects(listObjectsRequest);
            List<S3Object> objects = res.contents();
            for (S3Object s3Object : objects) {
                System.out.println("Deleting object: " + keyName);
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key())
                        .build();
                s3.deleteObject(deleteObjectRequest);
            }
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n"); */
    }
    public static void runpythonScript(String[] args) throws Exception {
        StringWriter writer = new StringWriter();
        ScriptContext context = new SimpleScriptContext();

        context.setWriter(writer);

        ScriptEngineManager manager = new ScriptEngineManager();
        System.out.println(manager.getEngineFactories().toString());
        Options.importSite = false;

//        ScriptEngine engine = manager.getEngineFactories().stream().filter(x-> Objects.equals(x.getEngineName(), "jython")).findFirst().get().getScriptEngine();
        ScriptEngine engine = manager.getEngineByName("python");
        Bindings bindings = engine.createBindings();
        bindings.put("args", args);
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        engine.eval(new FileReader(resolvePythonScriptPath("hello.py")), context);
        System.out.println(writer.toString().trim());
    }
    private static String resolvePythonScriptPath(String path){
        File file = new File(path);
        return file.getAbsolutePath();
    }
}


