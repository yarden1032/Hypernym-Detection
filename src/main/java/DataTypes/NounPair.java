package DataTypes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NounPair
        implements WritableComparable<NounPair> {

    public Text word1;

    public Text word2;

    public BooleanWritable isHypernym;

    public NounPair(String word1, String word2){

        this.word1 = new Text(word1);

        this.word2 = new Text(word2);

        isHypernym = new BooleanWritable(false);

    }

    public NounPair(String word1, String word2 ,boolean isHypernym){

        this.word1 = new Text(word1);

        this.word2 = new Text(word2);

        this.isHypernym = new BooleanWritable(isHypernym);

    }

    public NounPair() {

        word1 = new Text();

        word2 = new Text();

        isHypernym = new BooleanWritable();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

        word1.write(dataOutput);

        word2.write(dataOutput);

        isHypernym.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        word1.readFields(dataInput);

        word2.readFields(dataInput);

        isHypernym.readFields(dataInput);

    }

    @Override
    public int compareTo(NounPair other) {

        return other.word1.compareTo(this.word1)==0 ? other.word2.compareTo(this.word2):other.word1.compareTo(this.word1);
    }

    @Override
    public int hashCode() {
        return word1.hashCode() + word2.hashCode();
    }

    @Override
    public String toString() {
        return word1.toString() + ',' + word2.toString();
    }
}


