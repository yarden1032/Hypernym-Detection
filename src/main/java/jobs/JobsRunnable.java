package jobs;

import DataTypes.LongsPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobsRunnable {

    private static String bucketPath;
    private static String corpusPath;
    private static final String LOG_PATH = "/log-files/";

    private static long N;

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println(
                    "Wrong argument count received.\nExpected <corpus-path> <bucket path> <stop words path>.");
            System.exit(1);
        }

        System.out.println("args length is "+args.length);
        corpusPath = args[0];
        System.out.println("arg number 0: "+corpusPath);
        bucketPath = args[1];
        System.out.println("arg number 1: "+bucketPath);
        System.out.println(bucketPath);

        // Split Corpus
        Configuration parseCorpusLinesConfig = new Configuration();
        final Job parseCorpus = Job.getInstance(parseCorpusLinesConfig, "Parse Corpus Lines");
        String parseCorpusPath = createParseCorpusJob(parseCorpus, corpusPath);
        waitForJobCompletion(parseCorpus, parseCorpusPath);

        //Counters counters = parseCorpusLines.getCounters();
        //Counter counter = counters.findCounter(CounterType.NGRAMS_COUNTER);
        //N = counter.getValue();


        //construst Calculation variables
     /*   Configuration constructCalculationVariableConfig = new Configuration();
        //constructCalculationVariableConfig.setEnum("operation", Operation.NR);
        final Job constructCalculationVariables = Job.getInstance(constructCalculationVariableConfig, "Construct calculation variables");
        String constructCalculationVariablePath = createConstructCalculationVariablesJob(constructCalculationVariables, parseCorpusPath);
        waitForJobCompletion(constructCalculationVariables, constructCalculationVariablePath); */

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

    private static String setOutput(Job job) {
        String outputPath = String.format("%s/jobs/%s", bucketPath, job.getJobName());
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return outputPath;
    }

    private static String createParseCorpusJob(Job job, String corpusPath) throws
            IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(ParseCorpus.class);
        job.setMapperClass(ParseCorpus.MapperClass.class);
        //job.addCacheFile(new Path(stopWordsPath).toUri());
        /*if (shouldOperateLocalAggregation) {
            job.setCombinerClass(ParseCorpusLines.CombinerClass.class);
        } */
        job.setReducerClass(ParseCorpus.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongsPairWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongsPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return setInputOutput(job, corpusPath, false);
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

}
