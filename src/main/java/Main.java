import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateKeyPairRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.util.List;

import static java.lang.System.exit;

public class Main {
    private static String JarBucketName ;
    private static String stopwords ;

    private static S3Client s3;
    private static Boolean bool1 ;
    public static void main(String[] args) {

        //arg[0] => bucketname
        //arg[1] => eng-stopwords.txt
        //arg[2] => jarfile


        if (args.length < 3) {
            System.out.println("please provide all the args - input, output, jarfile");
            exit(1);
        }
        JarBucketName=args[0];
        stopwords=args[1];
        bool1 = args.length >3 ;
        System.out.println("args are:");
//        System.out.println("s3://bucketonation111/input3");
        System.out.println("s3://" + JarBucketName+"/");
        System.out.println("s3://" + JarBucketName + "2/"+stopwords);

        uploadJar(args[2], JarBucketName);
        uploadJar(stopwords, JarBucketName+"2");
//        initHadoopJar need more work please be advised!!!
//        TODO: make all the config in it as it should
        //notice we can do it here natively the decision of main file,
        // just create another file for main for the reduce app
        System.out.println("init cluster");
        initHadoopJar(JarBucketName);
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
//            s3Client.createBucket(CreateBucketRequest
//                    .builder()
//                    .bucket(bucketName)
//                    .createBucketConfiguration(
//                            CreateBucketConfiguration.builder()
//                                    .locationConstraint(region.id())
//                                    .build())
//                    .build());
            System.out.println("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println(bucketName + " is ready.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
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
// System.out.println(e.awsErrorDetails().errorMessage());
            // System.exit(1);
        }
    }

    public static void initHadoopJar(String JarBucketName) {

        AWSCredentials credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        createKeyPair("yourkey");
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://"+JarBucketName+"/jarfile.jar") // This should be a full map reduce application.

                .withMainClass("jobs.KnowledgeBaseRunnable");
//                corpus out eng-stopwords.txt


//
//                .withArgs("s3://bucketonation111/input3","s3://" + JarBucketName+"/output/","s3://" + JarBucketName + "2/"+stopwords);




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
        System.out.println("Ran job flow with id: " + jobFlowId);
//        while (true) {
//            DescribeJobFlowsRequest request = DescribeJobFlowsRequest.builder()
//                    .jobFlowIds(jobFlowId)
//                    .build();
//            DescribeJobFlowsResponse response = emrClient.describeJobFlows(request);
//            JobFlowExecutionStatus status = response.jobFlows().get(0).executionStatusDetail().state();
//            if (status == JobFlowExecutionStatus.TERMINATED) {
//                System.out.println("Job flow has completed");
//                break;
//            } else {
//                System.out.println("Job flow is still in progress. Current status: " + status);
//                Thread.sleep(60 * 1000); // sleep for 1 minute
//            }
//        }
    }
    public static void cleanUp( String bucketName, String keyName) {
        System.out.println("Cleaning up...");
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
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }
}
//Release label:emr-5.36.0
//        Hadoop distribution:Amazon 2.10.1

