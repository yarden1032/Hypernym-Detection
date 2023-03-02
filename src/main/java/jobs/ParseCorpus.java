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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ParseCorpus {


    public static class MapperClass extends Mapper<LongWritable, Text, DependencyPath, NounPair> {

static Mapper.Context context;
        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) {
            this.context = context;
            String[] words = line.toString().split("\\t");
            if(!Objects.equals(line.toString(), "") || words.length < 3) {

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
               try {
                   if(splitter.length>=4)
                   {
                       synArray.add(new SyntacticNgram(splitter[0], splitter[1], splitter[2], Long.parseLong(splitter[3]), total_count));
                   }
               }
               catch (NumberFormatException ignored)
               {
                   System.out.println("failed to convert to long");
               }
            }

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
                                    DFSSyntatticNgram(typeInSentencesTree, path, j, i, typeInSentencesTree.get(i).get(k),true,total_count);
                                } catch (Exception e) {
                                    System.out.println("null stuff");
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

        private static void DFSSyntatticNgram(List<List<SyntacticNgram>> typeInSentencesTree, ArrayList<SyntacticNgram> path, int lastLevel,int currentLevel, SyntacticNgram firstSyn,boolean isFirstRound,long numOfOccurrences) throws IOException, InterruptedException {
            if (currentLevel > lastLevel && !path.isEmpty()) {
                String[] arrayString = new String[1];
                arrayString[0] = path.get(0).head_word;
                String[] arrayString2 = new String[1];
                arrayString2[0] = path.get(path.size() - 1).head_word;
                //return path;
                if(path.get(0).type.contains("NN") && path.get(path.size() - 1).type.contains("NN"))
                {
                NounPair nounPair = null;
                nounPair = new NounPair(JobsRunnable.executeScript(path.get(0).head_word),JobsRunnable.executeScript(path.get(path.size() - 1).head_word));
                try {
                    context.write(new DependencyPath(CreateText(path), new LongWritable(numOfOccurrences)),
                            nounPair);
                } catch (IOException e) {
                    System.out.println("IO exception");
                } catch (InterruptedException e) {
                    System.out.println("InterruptedException");
                }
            }
                return;
            }
            if (isFirstRound){
                path.add(firstSyn);
                DFSSyntatticNgram(typeInSentencesTree, path, lastLevel, currentLevel +1, firstSyn,false,numOfOccurrences);
            }
            else {
                if (!typeInSentencesTree.get(currentLevel).isEmpty()){
                    for (int i=0; i< typeInSentencesTree.get(currentLevel).size(); i++) {
                        if (!typeInSentencesTree.get(currentLevel).isEmpty()) {
                            ArrayList <SyntacticNgram> newPath = new ArrayList<>(path);
                            newPath.add(typeInSentencesTree.get(currentLevel).get(i));
                            DFSSyntatticNgram(typeInSentencesTree, newPath, lastLevel, currentLevel + 1, firstSyn, false, numOfOccurrences);
                        }
                    }
                }
                else{
                    try{DFSSyntatticNgram(typeInSentencesTree, path, lastLevel, currentLevel +1, firstSyn,false,numOfOccurrences);
                    }
                    catch(StackOverflowError e){
                        System.out.println("stackoverflow Was here: "+typeInSentencesTree);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, DependencyPath, NounPair>.Context context) throws IOException, InterruptedException {
            System.out.println("finished map");
        }
    }


    public static class ReducerClass extends Reducer<DependencyPath, NounPair, DependencyPath, Text> {
        private long DPmin;

            private Counter featureLexiconSizeCounter ;

            @Override
            public void setup(Context context) {
                System.out.println("setupReducer");
                DPmin = context.getConfiguration().getLong("DPmin", 5);
                featureLexiconSizeCounter = context.getCounter(CounterType.FEATURE_LEXICON);
            }


        @Override
        public void reduce(DependencyPath path, Iterable<NounPair> occurrencesList, Context context)
                throws IOException, InterruptedException {
          //  System.out.println("reduce:"+path);
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






