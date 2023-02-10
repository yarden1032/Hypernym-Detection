package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongsPairWritable implements WritableComparable<LongsPairWritable> {

    public LongWritable num1;

    public LongWritable num2;

    public LongsPairWritable (LongWritable num1,LongWritable num2){

        this.num1 = num1;

        this.num2 = num2;

    }

    public LongsPairWritable(){

        this.num1 = new LongWritable();

        this.num2 = new LongWritable();

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        num1.write(dataOutput);

        num2.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        num1.readFields(dataInput);

        num2.readFields(dataInput);

    }

    @Override
    public int compareTo(LongsPairWritable other) {

        return other.num2 == this.num2 ? 0 : (other.num2.get() < this.num2.get() ? 1 : -1);
    }

    @Override
    public String toString() {
        return num1.toString() + '\t' + num2.toString();
    }
}


