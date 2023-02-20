package jobs;

import DataTypes.CounterType;
import DataTypes.DependencyPath;
import DataTypes.NounPair;
import DataTypes.SyntacticNgram;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.*;
import java.io.*;

public class ParseCorpus {


    public static class MapperClass extends Mapper<LongWritable, Text, DependencyPath, NounPair> {
        public static String runpythonScript(String[] args) throws Exception {
            StringWriter writer = new StringWriter();
            ScriptContext context = new SimpleScriptContext();

            context.setWriter(writer);

            ScriptEngineManager manager = new ScriptEngineManager();
            System.out.println(manager.getEngineFactories().toString());
            ScriptEngine engine = manager.getEngineByName("python");
            Bindings bindings = engine.createBindings();
            bindings.put("args", args);
            context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
            engine.eval(new FileReader(resolvePythonScriptPath("hello.py")), context);
            return writer.toString().trim();
        }
        private static String resolvePythonScriptPath(String path){
            File file = new File(path);
            return file.getAbsolutePath();
        }
        @Override
        public void map(LongWritable lineId, Text line, Mapper.Context context) {
            //Todo: split by tab like assignment2
            String [] words = line.toString().split("\\t");
            String head_word = words[0];
            String[] arrayString = new String[1];
            arrayString[0] = head_word;
            try {
                head_word =  runpythonScript(arrayString);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            String syntactic_ngram_String = words[1];
            long total_count;
            try{
                total_count = Long.parseLong(words[2]);
            }
            catch (NumberFormatException e)
            {
                return;
            }
           String [] syntactic_ngram_String_array= syntactic_ngram_String.split(" ");

            SyntacticNgram [] synArray = new SyntacticNgram[syntactic_ngram_String_array.length];
             for (int i=0 ; i< synArray.length; i++){
                String [] splitter = syntactic_ngram_String_array[i].split("/");
                //add here num of occurrences
                synArray[i] = new SyntacticNgram(splitter[0],splitter[1],splitter[2],Long.parseLong(splitter[3]),total_count);
            }
            for (SyntacticNgram syntacticNgram : synArray) {
                try {
                    if((syntacticNgram.type.contains("NN")))
                    //TODO: do it better with the NounWords abocve
                    {
                        String syntacticNgram_head_Word;
                        String[] arrayToString = new String[1];
                        arrayToString[0] = syntacticNgram.head_word;
                        try {
                            syntacticNgram_head_Word =  runpythonScript(arrayString);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        //if the other word is noun - add it, else, ignore
                        context.write(new DependencyPath(new Text(syntacticNgram.typeInSentence)),
                                new NounPair(head_word,syntacticNgram_head_Word,syntacticNgram.numOfOccurrences));
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


     /*   public static class CombinerClass extends Reducer<Text, dependencyPath, Text, dependencyPath> {

            @Override
            public void reduce(Text threeGram, Iterable<dependencyPath> occurrencesList, Context context) throws IOException, InterruptedException {

            }
        } */

  /*  public static String removeLastChar(String s) {
        return (s == null || s.length() == 0)
                ? null
                : (s.substring(0, s.length() - 1));
    } */
        public static class ReducerClass extends Reducer<DependencyPath, NounPair, DependencyPath,Text> {
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
                    context.write(path, new Text(valueString.substring(0, valueString.length() - 1)));
                }
                /*Roni - not sure if the output type should be Text or something else, but we want to create a list of all the noun pairs.
                 the format should be - key: <dependency path> value: <noun pair<TAB>noun pair<TAB>noun pair<TAB>....>
                the pairs would be split by tab and the nouns inside the pairs would be split by comma "," .
                remember to check here th DPmin - if a path does not appear more than DPmin times there is no need
                to write it to the context...
                */


            }
        }

    }

