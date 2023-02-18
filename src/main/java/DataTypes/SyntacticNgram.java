package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Objects;

public class SyntacticNgram implements WritableComparable<SyntacticNgram>  {

    public Long position ;
    public String type ;
    public String head_word;

    public  String typeInSentence ;

    public Long numOfOccurrences;

    public SyntacticNgram(String head_word,   String type, String typeInSentence,Long position, Long numOfOccurrences ){

        this.head_word = head_word;
        this.type = type;
        this.typeInSentence = typeInSentence;
        this.position = position;
        this.numOfOccurrences = numOfOccurrences;

    }

    public SyntacticNgram(){
        this.head_word="";
        this.type = "";
        this.typeInSentence = "";
        this.position=0L;
        this.numOfOccurrences = 0L;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(position);
        dataOutput.writeChars(type);
        dataOutput.writeChars(head_word);
        dataOutput.writeChars(typeInSentence);
        dataOutput.writeLong(numOfOccurrences);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        position = dataInput.readLong();
        type = dataInput.readLine();
        head_word = dataInput.readLine();
        typeInSentence = dataInput.readLine();
        numOfOccurrences = dataInput.readLong();

    }

    @Override
    public int compareTo(SyntacticNgram other) {

        return Objects.equals(other.head_word, this.head_word) ? 0 : (other.position < this.position ? 1 : -1);
    }



    @Override
    public String toString() {
        return head_word+ '\t'+type + '\t' + typeInSentence +  '\t' +position.toString();
    }
}


