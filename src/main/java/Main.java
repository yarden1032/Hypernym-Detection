import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3Object;
import localClasses.ResultsEvaluator;
import localClasses.ClassifierTrainer;
import localClasses.TxtToArffConverter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import software.amazon.awssdk.services.s3.model.*;

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
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

public class Main {

    private static String JarBucketName ;

    private static final String txtHypernymPath = "hypernym.txt";

    private static final String txtVectorsPath = "vectors.txt";

    static final String csvPath = "vectors.csv";

    private static final String arffVectorsPath = "vectors.arff";
    private static S3Client s3;

    public static void main(String[] args) throws Exception {
        //we should recieve as input the DPmin value, input of the biarcs data set and the location of the
        //hypernym.txt file (can be local)
        /*  if (args.length < 3) {
            System.out.println("please provide all the args - input, output, jarfile");
            exit(1);
        }
        JarBucketName=args[0];

        uploadJar(args[2], JarBucketName);
        uploadJar(txtHypernymPath, JarBucketName+"2");
        System.out.println("init cluster");
        initHadoopJar(JarBucketName);
        //after those two jobs finish, we have S3 bucket containing the data we would like to learn from
        downloadFromS3(); */
        new TxtToArffConverter().convert(txtVectorsPath,csvPath);
        DataSource source = new DataSource("vectors.csv");
        Instances data = source.getDataSet();
        data.setClassIndex(data.numAttributes() -1);
        StringToWordVector filter = new StringToWordVector();
        filter.setInputFormat(data);
        data = Filter.useFilter(data,filter);
        ClassifierTrainer trainer = new ClassifierTrainer();
        Classifier nb = trainer.train(data);
        new ResultsEvaluator().evaluateResults(data, nb);
    }

    private static void downloadFromS3() {
        //here we are downloading the map-reduce jobs result and saving it as the data for the classifier

            s3 = S3Client.builder().region(Region.US_EAST_1).build();

           /* GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(); */
            try {
                File f = new File(txtVectorsPath);
                if(f.delete()) {
                    System.out.println("output file replaced with new one");
                    }
                }
            catch(Exception e){
                //No file, all good
            }
         //   s3.getObject(getObjectRequest, Paths.get(outputPath));
        }

    private static void uploadJar(String jarFilePath, String bucketName) {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();
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

    public static void initHadoopJar(String JarBucketName) {

     /*   AWSCredentials credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        createKeyPair("yourkey");
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://"+JarBucketName+"/jarfile.jar") // This should be a full map reduce application.
                .withMainClass("jobs.KnowledgeBaseRunnable");

        hadoopJarStep=  bool1?hadoopJarStep.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/3gram/data","s3://" + JarBucketName+"/output/","s3://" + JarBucketName + "2/"+stopwords, "shouldOperateLocalAggregation"):
                hadoopJarStep.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/3gram/data","s3://" + JarBucketName+"/output/","s3://" + JarBucketName + "2/"+stopwords );
        StepConfig stepConfig = new StepConfig()
                .withName("3gram")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(9)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.10.1").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("3gramNewInputLine")
                .withInstances(instances)
                .withSteps(stepConfig).withReleaseLabel("emr-5.36.0")
                .withLogUri("s3n://" + JarBucketName + "/logs/");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId); */
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
}


