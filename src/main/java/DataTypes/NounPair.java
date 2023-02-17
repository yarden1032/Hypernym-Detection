package DataTypes;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NounPair
        implements WritableComparable<NounPair> {

    public String word1 ;
    public  String word2 ;
    public Long numOfOccurrences;
    public Boolean isHypernym;

    public NounPair(String word1, String word2,Long numOfOccurrences){

        this.word1 = word1;
        this.word2 = word2;
        this.numOfOccurrences = numOfOccurrences;

    }

    public NounPair(String word1, String word2,Long numOfOccurrences,boolean isHypernym){

        this.word1 = word1;
        this.word2 = word2;
        this.numOfOccurrences = numOfOccurrences;
        this.isHypernym = isHypernym;

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeChars(word1);
        dataOutput.writeChars(word2);
        dataOutput.writeLong(numOfOccurrences);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        word1 = dataInput.readLine();
        word2 = dataInput.readLine();
        numOfOccurrences = dataInput.readLong();

    }

    @Override
    public int compareTo(NounPair other) {

        return other.word1.compareTo(this.word1)==0 ? other.word2.compareTo(this.word2):other.word1.compareTo(this.word1);
    }

    @Override
    public String toString() {
        return word1 + ' ' + word2;
    }
}


