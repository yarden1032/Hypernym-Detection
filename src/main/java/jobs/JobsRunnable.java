package jobs;

import DataTypes.CounterType;
import DataTypes.DependencyPath;
import DataTypes.NounPair;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.python.core.Options;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import javax.script.*;
import java.io.*;

public class JobsRunnable {

    private static String bucketPath;
    private static String corpusPath;

    private static String hypernymPath;
    private static final String LOG_PATH = "/log-files/";
    private static final Region region = Region.US_EAST_1;
    private static long DPmin;
    private static final AWSCredentials credentials=new DefaultAWSCredentialsProviderChain().getCredentials();
    private static final String script = "from nltk.stem import *\n" +
            "import sys\n" +
            "stemmer = PorterStemmer()\n" +
            "plural = sys.argv[1]\n" +
            "single = stemmer.stem(plural)\n" +
            "print(single)";

    private static long featureLexiconSize;

    public static void main(String[] args) throws Exception {
        downloadFromS3("scriptbucketton","hello.py","hello.py");
//        System.out.println( runpythonScript(starr));
        if (args.length < 4) {
            System.err.println(
                    "Wrong argument count received.\nExpected <corpus-path> <bucket path> <stop words path>.");
            System.exit(1);
        }

        System.out.println("args length is "+args.length);
        corpusPath = args[0];
        System.out.println("arg number 1: "+corpusPath);
        bucketPath = args[1];
        System.out.println("arg number 2: "+bucketPath);
        System.out.println(bucketPath);
        hypernymPath = args[2];
        DPmin = Long.parseLong(args[3]);
        System.out.println("arg number 2: "+DPmin);
        System.out.println(DPmin);
        //if run locally, comment this line and have hello.py file.
//        downloadFromS3(bucketPath+"5","hello.py","hello.py");


        // Split Corpus
        Configuration parseCorpusLinesConfig = new Configuration();
        parseCorpusLinesConfig.setLong("DPmin",DPmin);
        final Job parseCorpus = Job.getInstance(parseCorpusLinesConfig, "Parse Corpus");
        String parseCorpusPath = createParseCorpusJob(parseCorpus, corpusPath);
        waitForJobCompletion(parseCorpus, parseCorpusPath);

        Counters counters = parseCorpus.getCounters();
        Counter counter = counters.findCounter(CounterType.FEATURE_LEXICON);
        featureLexiconSize = counter.getValue();


        //Create Vectors
         Configuration createVectorsConfig = new Configuration();
        createVectorsConfig.setLong("featureLexiconSize",featureLexiconSize);
        final Job createVectors = Job.getInstance(createVectorsConfig, "Create Vectors");
        String createVectorsPath = createCreateVectorsJob(createVectors, parseCorpusPath,hypernymPath);
        waitForJobCompletion(createVectors, createVectorsPath);
    }


    private static String setInputOutput(Job job, String inputPath, boolean finished)
            throws IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String outputPath =
                finished
                        ? String.format("%s/result", bucketPath)
                        : String.format("%s/jobs/%s", bucketPath, job.getJobName());
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return outputPath;
    }
    private static void downloadFromS3(String bucketName,String key,String outputPath) throws IOException {
        System.out.println("downloading from:"+bucketName);
        //here we are downloading the map-reduce jobs result and saving it as the data for the classifier

        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        try {
            File f = new File("/");
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

    private static String setOutput(Job job) {
        String outputPath = String.format("%s/jobs/%s", bucketPath, job.getJobName());
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return outputPath;
    }

    private static String createParseCorpusJob(Job job, String corpusPath) throws
            IOException {
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(ParseCorpus.class);
        job.setMapperClass(ParseCorpus.MapperClass.class);
        job.setReducerClass(ParseCorpus.ReducerClass.class);
        job.setMapOutputKeyClass(DependencyPath.class);
        job.setMapOutputValueClass(NounPair.class);
        job.setOutputKeyClass(DependencyPath.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        return setInputOutput(job, corpusPath, false);
    }

    private static String createCreateVectorsJob(Job job, String corpusPath, String hypernymPath) throws IOException {

            job.setJarByClass(CreateVectors.class);
            job.setReducerClass(CreateVectors.ReducerClass.class);
            job.setMapOutputKeyClass(NounPair.class);
            job.setMapOutputValueClass(DependencyPath.class);
            //job.setCombinerClass(CreateVectors.CombinerClass.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputValueClass(Boolean.class);
            MultipleInputs.addInputPath(
                    job,
                    new Path(corpusPath),
                    TextInputFormat.class,
                    CreateVectors.CorpusMapperClass.class);
            MultipleInputs.addInputPath(
                    job,
                    new Path(hypernymPath),
                    TextInputFormat.class,
                    CreateVectors.HypernymMapperClass.class);

            return setOutput(job);
    }


    private static void waitForJobCompletion(final Job job, String outputPath) {
        String description = job.getJobName();
        System.out.printf("Started %s job.%n", description);
        try {
            if (job.waitForCompletion(true)) {
                System.out.printf(
                        "%s finished successfully, output in S3 bucket %s.%n", description, outputPath);
            } else {
                System.out.printf("%s failed!, logs in S3 bucket at %s.%n", description, LOG_PATH);
                System.exit(1);
            }
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            System.err.printf(
                    "Exception caught! EXCEPTION: %s\nLogs in S3 bucket at %s.%n", e.getMessage(), LOG_PATH);
            System.exit(1);
        }
    }



    public static String executeScript(String input) throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(
                        "python3 hello.py "+input);
        p.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        StringBuilder lines = new StringBuilder();
        String line = "";
        while ((line = reader.readLine()) != null) {
            lines.append(line);
        }
        return lines.toString();
    }

}
