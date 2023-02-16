package jobs;

import DataTypes.DependencyPath;
import DataTypes.NounPair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

public class CreateVectors {

    //this mapper receives the output of the previous job. for each line it will switch between the key and value
    //so we will get - <noun pair>,<path>
    public static class CorpusMapperClass extends Mapper<LongWritable, Text, NounPair, DependencyPath> {

        protected void setup(Mapper context) throws IOException {

        }

        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) throws IOException, InterruptedException {

            String[] splitLine = line.toString().split("\\t");
            String path = splitLine[0];
            String[] splitPath = path.split(" ");
            for (int i=1;i<splitLine.length;i++){
                String [] nouns = splitLine[i].split(",");
                if (nouns.length  == 3 )
                    //not final - need fix according to changes in DependencyPath class
                context.write(new NounPair(nouns[0],nouns[1],Long.parseLong(nouns[2])),new DependencyPath(lineId.get(),splitPath[0],splitPath[1],new LongWritable(Long.parseLong(splitPath[3]))));
            }
        }
    }


    //need to sort so the result of this job will appear first in the reducer
    //the mapper will send the reduce the pair and the boolean classification
    public static class HypernymMapperClass extends Mapper<LongWritable, Text, NounPair, DependencyPath> {

        protected void setup(Mapper context) throws IOException {

        }

        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) throws IOException, InterruptedException {

            String[] splitLine = line.toString().split("\\t");
            if (splitLine.length == 3){
                String noun1 = splitLine[0];
                String noun2 = splitLine[1];
                String isHypernym = splitLine[2];

                context.write(new NounPair(noun1,noun2,0L,(isHypernym.equals("True"))),new DependencyPath());
            }
        }
    }


    public static class CombinerClass extends Reducer<NounPair, DependencyPath,NounPair, DependencyPath> {

        //should sum up together all the occurrences of same noun pair;
        @Override
        public void reduce(NounPair nounPair, Iterable<DependencyPath> paths, Context context) throws IOException, InterruptedException {
            //think about it...
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

            Vector<Integer> featureVector = new Vector((int)featureLexiconSize);

            if (paths.iterator().hasNext() && paths.iterator().next().isFake() && nounPair.isHypernym != null){

                for (DependencyPath path : paths){
                    Integer curr = featureVector.get(path.idInVector.intValue());
                    featureVector.set(path.idInVector.intValue(),curr + nounPair.numOfOccurrences.intValue());
                }

                context.write(new Text(featureVector.toString()),new BooleanWritable(nounPair.isHypernym));
            }
        }
    }
}
