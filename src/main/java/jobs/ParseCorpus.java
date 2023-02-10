package jobs;

import DataTypes.LongsPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ParseCorpus {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongsPairWritable> {

        private Set<String> stopWordsToSkip = new HashSet<>();


        protected void setup(Mapper.Context context) throws IOException {

            URI[] localPaths = context.getCacheFiles();

            if (localPaths.length == 1) {
                URI stopWordsUri = localPaths[0];
                try {
                    BufferedReader fis = new BufferedReader(new FileReader(new File(stopWordsUri.getPath()).getName()));
                    String pattern;
                    while ((pattern = fis.readLine()) != null) {
                        stopWordsToSkip.add(pattern);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '"
                            + stopWordsUri + "' : " + StringUtils.stringifyException(ioe));
                }

            }
        }


        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) throws IOException, InterruptedException {

        }
    }


        public static class CombinerClass extends Reducer<Text, LongsPairWritable, Text, LongsPairWritable> {

            @Override
            public void reduce(Text threeGram, Iterable<LongsPairWritable> occurrencesList, Context context) throws IOException, InterruptedException {

            }
        }


        public static class ReducerClass extends Reducer<Text, LongsPairWritable, Text, LongsPairWritable> {

            @Override
            public void setup(Context context) {

            }


            @Override
            public void reduce(Text threeGram, Iterable<LongsPairWritable> occurrencesList, Context context)
                    throws IOException, InterruptedException {

            }
        }

    }

