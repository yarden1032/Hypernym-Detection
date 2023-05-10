package jobs;

import DataTypes.DependencyPath;
import DataTypes.NounPair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.Vector;

public class CreateVectors {

    //this mapper receives the output of the previous job. for each line it will switch between the key and value
    //so we will get - <noun pair>,<path>
    public static class CorpusMapperClass extends Mapper<LongWritable, Text, NounPair, DependencyPath> {

        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) throws IOException, InterruptedException {

            String[] splitLine = line.toString().split("\\t");
            if (splitLine.length > 2) {
                String path = splitLine[0];
                String numOfOccurrencesPerPath = splitLine[1];
                String pathId = splitLine[2];
                for (int i = 3; i < splitLine.length; i++) {
                    String[] nouns = splitLine[i].split(",");
                    if (nouns.length == 2)
                        //not final - need fix according to changes in DependencyPath class
                        try {
                            context.write(new NounPair(nouns[0], nouns[1]), new DependencyPath(new LongWritable(Long.parseLong(pathId)), new Text(path), new LongWritable(Long.parseLong(numOfOccurrencesPerPath))));
                        } catch (NumberFormatException ex) {
                            Logger.error("failed to parse to long: " + ex.getMessage());
                        }
                }
            }
        }
    }

    /**
     * the mapper will send the reduce the pair and the boolean classification
     **/
    public static class HypernymMapperClass extends Mapper<LongWritable, Text, NounPair, DependencyPath> {

        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) throws IOException, InterruptedException {

            String[] splitLine = line.toString().split("\\t");
            if (splitLine.length == 3) {
                String noun1 = splitLine[0];
                String noun2 = splitLine[1];
                String isHypernym = splitLine[2];

                context.write(new NounPair(noun1, noun2, (isHypernym.equals("True"))), new DependencyPath());
            }
        }
    }

    public static class ReducerClass extends Reducer<NounPair, DependencyPath, Text, BooleanWritable> {

        private long featureLexiconSize;

        @Override
        public void setup(Context context) {
            featureLexiconSize = context.getConfiguration().getInt("featureLexiconSize", 100);
        }

        @Override
        public void reduce(NounPair nounPair, Iterable<DependencyPath> paths, Context context)
                throws IOException, InterruptedException {

            Vector<Integer> featureVector = new Vector();

            boolean isFromHypernymFile = false;

            for (DependencyPath currPath : paths) {
                try {
                int vectorId = (int) currPath.idInVector.get();
                if (vectorId != 0) {
                    featureVector.setSize((int) featureLexiconSize);
                    try {
                        Integer curr = featureVector.get(vectorId);
                        if (curr != null) {
                            featureVector.set(vectorId - 1, curr + (int) currPath.numOfOccurrences.get());
                        } else {
                            featureVector.set(vectorId - 1, (int) currPath.numOfOccurrences.get());
                        }
                    } catch (Exception ex) {
                        featureVector.set(vectorId - 1, (int) currPath.numOfOccurrences.get());
                    }
                } else {
                    isFromHypernymFile = true;
                      }
                     } catch (ClassCastException ex) {
                      Logger.error("failed to cast to int: " + ex.getMessage());
                     }
                }
                if (featureVector.size() > 0 && isFromHypernymFile) {
                    for (int i = 0; i < featureVector.size(); i++) {
                        if (featureVector.get(i) == null) {
                            featureVector.set(i, 0);
                        }
                    }
                    context.write(new Text(nounPair.word1 + "," + nounPair.word2 + "\t" + featureVector), nounPair.isHypernym);
                }
            }
        }
    }

