package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LineSyntacticNgramKey
        implements WritableComparable<LineSyntacticNgramKey> {

    public String type ;
    public  String typeInSentence ;

    public  LongWritable direction ;
    public LineSyntacticNgramKey(String type,String typeInSentence,LongWritable direction){


        this.type = type;
        this.typeInSentence = typeInSentence;
        this.direction = direction;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


    }

    @Override
    public int compareTo(LineSyntacticNgramKey other) {


        return other.type.compareTo(this.type)==0?(other.typeInSentence.compareTo(this.typeInSentence)==0?direction.compareTo(other.direction):other.typeInSentence.compareTo(this.typeInSentence)):other.type.compareTo(this.type) ;
    }

    @Override
    public String toString() {
        return type + '\t' + typeInSentence + '\t'+ direction.toString();
    }
}


