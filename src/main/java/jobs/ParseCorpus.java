package jobs;

import DataTypes.CounterType;
import DataTypes.DependencyPath;
import DataTypes.NounPair;
import DataTypes.SyntacticNgram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ParseCorpus {


    public static class MapperClass extends Mapper<LongWritable, Text, DependencyPath, NounPair> {


        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) {
            //Todo: split by tab like assignment2
            String[] words = line.toString().split("\\t");
            String head_word = words[0];
            //  String[] arrayString = new String[1];
            // arrayString[0] = head_word;
            try {
                //    head_word =  runpythonScript(arrayString);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            String syntactic_ngram_String = words[1];
            long total_count;
            try {
                total_count = Long.parseLong(words[2]);
            } catch (NumberFormatException e) {
                return;
            }
            String[] syntactic_ngram_String_array = syntactic_ngram_String.split(" ");

            List<SyntacticNgram> synArray = new ArrayList<>();
            for (int i = 0; i < syntactic_ngram_String_array.length; i++) {
                String[] splitter = syntactic_ngram_String_array[i].split("/");
                //add here num of occurrences
               try {
                   if(splitter.length>=4)
                   {
                       synArray.add(new SyntacticNgram(splitter[0], splitter[1], splitter[2], Long.parseLong(splitter[3]), total_count));
                   }
               }
               catch (NumberFormatException ignored)
               {

               }
            }

            // this is a sort by using the default java interface
            SyntacticNgramComparator comparator = new SyntacticNgramComparator();
            synArray.sort(comparator);
            //here we are going to create a path and on the fly to send it if it's relevant (two nouns)
            List<List<SyntacticNgram>> typeInSentencesTree = new ArrayList<>();
            if(synArray.size()!=0) {
                for (int i = 0; i < (synArray.get(synArray.size() - 1).position.intValue() + 1); i++) {
                    typeInSentencesTree.add(new ArrayList<>());
                }
            }
            for (int i = 0; i < synArray.size(); i++) {
                if(synArray.get(i).position.intValue()<0)
                    continue;
                typeInSentencesTree.get(synArray.get(i).position.intValue()).add(synArray.get(i));
            }
            for (int i = 0; i < typeInSentencesTree.size(); i++) {
                for (int k = 0 ; k < typeInSentencesTree.get(i).size(); k++) {
                        for (int j = i + 1; j < typeInSentencesTree.size(); j++) {
                            if (!typeInSentencesTree.get(j).isEmpty()) {
                                //for each first word possible in path, we want to create a path from it to what is under
                                ArrayList<SyntacticNgram> path = new ArrayList<>();
                                try {
                                    DFSSyntatticNgram(typeInSentencesTree, path, j, i, typeInSentencesTree.get(i).get(k),true,context,total_count);
                                    /*context.write(new DependencyPath(CreateText(path), new LongWritable(total_count)),
                                            new NounPair(path.get(0).head_word, path.get(path.size()-1).head_word)); */
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                }
            }
        }

        private static Text CreateText(ArrayList<SyntacticNgram> path) {
            String stToText = "";
            for (int i = 0; i < path.size(); i++) {
                stToText += path.get(i).type + "/" + path.get(i).typeInSentence + " ";
            }
            return (new Text(stToText));
        }

        //private static void DFSSyntatticNgram(List<List<SyntacticNgram>> typeInSentencesTree, ArrayList<SyntacticNgram> path, int lastLevel, int CurrentindexInLevel, int currentLevel, SyntacticNgram lastSyn, Mapper.Context context, long numOfOccurrences) throws Exception {
        private static void DFSSyntatticNgram(List<List<SyntacticNgram>> typeInSentencesTree, ArrayList<SyntacticNgram> path, int lastLevel,int currentLevel, SyntacticNgram firstSyn,boolean isFirstRound, Mapper.Context context,long numOfOccurrences) throws Exception {
            if (currentLevel > lastLevel && !path.isEmpty()) {
                    String[] arrayString = new String[1];
                    arrayString[0] = path.get(0).head_word;
                    String[] arrayString2 = new String[1];
                    arrayString2[0] = path.get(path.size() - 1).head_word;
                    //return path;
                    context.write(new DependencyPath(CreateText(path), new LongWritable(numOfOccurrences)),
                            new NounPair(path.get(0).head_word, path.get(path.size()-1).head_word));
                    return;
            }
            if (isFirstRound){
                path.add(firstSyn);
                DFSSyntatticNgram(typeInSentencesTree, path, lastLevel, currentLevel +1, firstSyn,false,context,numOfOccurrences);
            }
            else {
                if (!typeInSentencesTree.get(currentLevel).isEmpty()){
                    for (int i=0; i< typeInSentencesTree.get(currentLevel).size(); i++) {
                        if (!typeInSentencesTree.get(currentLevel).isEmpty()) {
                            ArrayList <SyntacticNgram> newPath = new ArrayList<>(path);
                            newPath.add(typeInSentencesTree.get(currentLevel).get(i));
                            DFSSyntatticNgram(typeInSentencesTree, newPath, lastLevel, currentLevel + 1, firstSyn, false, context, numOfOccurrences);
                        }
                        }
                    }
                else{
                    DFSSyntatticNgram(typeInSentencesTree, path, lastLevel, currentLevel +1, firstSyn,false,context,numOfOccurrences);
                }
            }

        }


    public static class PartitionerClass extends Partitioner<DependencyPath, NounPair> {

        @Override
        public int getPartition(DependencyPath key, NounPair value, int numPartitions) {
            return (key.hashCode() & 0xFFFFFFF) % numPartitions; // Make sure that equal occurrences will end up in same reducer
        }
    }

    public static class ReducerClass extends Reducer<DependencyPath, NounPair, DependencyPath, Text> {
        private long DPmin;

            private Counter featureLexiconSizeCounter ;

            @Override
            public void setup(Context context) {
                DPmin = context.getConfiguration().getLong("DPmin", 5);
                featureLexiconSizeCounter = context.getCounter(CounterType.FEATURE_LEXICON);
            }


        @Override
        public void reduce(DependencyPath path, Iterable<NounPair> occurrencesList, Context context)
                throws IOException, InterruptedException {
            StringBuilder valueString = new StringBuilder();
            long counter = 0;
            for (NounPair value : occurrencesList) {
                counter++;
                valueString.append(value.toString()).append("\t");

                }
                if(counter >=  DPmin) {
                    featureLexiconSizeCounter.increment(1);
                    path.setIdInVector(featureLexiconSizeCounter.getValue());
                    context.write(path, new Text(valueString.substring(0, valueString.length() - 1)));
                }
        }
    }

}






