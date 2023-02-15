package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineSyntacticNgramValue
        implements WritableComparable<LineSyntacticNgramValue> {

    public String word1 ;
    public  String word2 ;


    public LineSyntacticNgramValue(String word1, String word2){


        this.word1 = word1;
        this.word2 = word2;

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


    }

    @Override
    public int compareTo(LineSyntacticNgramValue other) {


        return other.word1.compareTo(this.word1)==0?other.word2.compareTo(this.word2):other.word1.compareTo(this.word1);
    }

    @Override
    public String toString() {
        return word1 + '\t' + word2;
    }
}


