package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Objects;

public class SyntacticNgram implements WritableComparable<SyntacticNgram>  {

    public LongWritable position ;
    public String type ;
    public String head_word;

    public  String typeInSentence ;

    public SyntacticNgram(String head_word,   String type, String typeInSentence,LongWritable position ){

        this.head_word = head_word;
        this.type = type;
        this.typeInSentence = typeInSentence;
        this.position = position;

    }

    public SyntacticNgram(){
        this.head_word="";
        this.type = "";
        this.typeInSentence = "";
        this.position=new LongWritable(0);


    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


    }

    @Override
    public int compareTo(SyntacticNgram other) {

        return Objects.equals(other.head_word, this.head_word) ? 0 : (other.position.get() < this.position.get() ? 1 : -1);
    }



    @Override
    public String toString() {
        return head_word+ '\t'+type + '\t' + typeInSentence +  '\t' +position.toString();
    }
}


