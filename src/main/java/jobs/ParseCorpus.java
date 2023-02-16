package jobs;

import DataTypes.LineSyntacticNgramKey;
import DataTypes.LineSyntacticNgramValue;
import DataTypes.SyntacticNgram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LineSyntacticNgramKey> {

        private Set<String> NounWords = new HashSet<>();
        //this file will be a file of all the types we want to have
        //https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html
        //from there we will ask if the value (description) have the word "noun", if so go for it, else, ignore


        protected void setup(Mapper.Context context) throws IOException {

            URI[] localPaths = context.getCacheFiles();

            if (localPaths.length == 1) {
                URI stopWordsUri = localPaths[0];
                try {
                    BufferedReader fis = new BufferedReader(new FileReader(new File(stopWordsUri.getPath()).getName()));
                    String pattern;
                    while ((pattern = fis.readLine()) != null) {
                        NounWords.add(pattern);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '"
                            + stopWordsUri + "' : " + StringUtils.stringifyException(ioe));
                }

            }
        }


        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) {
            //Todo: split by tab like assignment2
            String [] words = line.toString().split("\\t");
            String head_word = words[0];
            String syntactic_ngram_String = words[1];
            try{
                long total_count = Long.parseLong(words[2]);
            }
            catch (NumberFormatException e)
            {
                return;
            }
           String [] syntactic_ngram_String_array= syntactic_ngram_String.split(" ");

            SyntacticNgram [] synArray = (SyntacticNgram[]) Arrays.stream(syntactic_ngram_String_array).map(x ->{
               String [] splitter =   x.split("/");
               return new SyntacticNgram(splitter[0],splitter[1],splitter[2],new LongWritable(Long.parseLong(splitter[3])));
           }).toArray();
            for (SyntacticNgram syntacticNgram : synArray) {
                try {
                    if((syntacticNgram.type.contains("NN")))
                    //TODO: do it better with the NounWords abocve
                    {
                        //if the other word is noun - add it, else, ignore
                        context.write(new LineSyntacticNgramKey(syntacticNgram.type,syntacticNgram.typeInSentence,syntacticNgram.position),
                                new LineSyntacticNgramValue(head_word,syntacticNgram.head_word));
                        //now we can "create" an  output with the splitter object and the head_word
                    }



                } catch (IOException ignored) {
                    //this exception is actually good one, if we have any issue with the structure of the line, ignore the line.
                } catch (InterruptedException e) {
                    continue;
                    //TODO:not sure about this exception - be advised
                }


            }

            //filter by what go to context which lines go and which not

        }
    }


        public static class CombinerClass extends Reducer<Text, LineSyntacticNgramKey, Text, LineSyntacticNgramKey> {

            @Override
            public void reduce(Text threeGram, Iterable<LineSyntacticNgramKey> occurrencesList, Context context) throws IOException, InterruptedException {

            }
        }


        public static class ReducerClass extends Reducer<Text, LineSyntacticNgramKey, Text, LineSyntacticNgramKey> {

            @Override
            public void setup(Context context) {

            }


            @Override
            public void reduce(Text threeGram, Iterable<LineSyntacticNgramKey> occurrencesList, Context context)
                    throws IOException, InterruptedException {

            }
        }

    }

