package DataTypes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//Roni: should be changed here so the path can have dynamic length;
public class DependencyPath implements WritableComparable<DependencyPath> {

    public LongWritable idInVector;

    public Text typeInSentence;

    public BooleanWritable isReal;

    public LongWritable numOfOccurrences;


    public DependencyPath(Text typeInSentence,LongWritable numOfOccurrences) {

        this.typeInSentence = typeInSentence;
        this.idInVector = new LongWritable();
        this.numOfOccurrences = numOfOccurrences;
        isReal = new BooleanWritable(true);
    }

    public DependencyPath(LongWritable idInVector, Text typeInSentence, LongWritable numOfOccurrences) {

        this.idInVector = idInVector;
        this.typeInSentence = typeInSentence;
        isReal = new BooleanWritable(true);
        this.numOfOccurrences = numOfOccurrences;
    }


    //this constructor builds fake dependency path
    public DependencyPath() {
        idInVector = new LongWritable();
        typeInSentence = new Text("");
        isReal = new BooleanWritable(false);
        numOfOccurrences = new LongWritable ();
    }

    public DependencyPath (Text path) {
        idInVector = new LongWritable();
        typeInSentence = path;
        isReal = new BooleanWritable(false);
        numOfOccurrences = new LongWritable ();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(idInVector.get());

        dataOutput.writeLong(numOfOccurrences.get());

        typeInSentence.write(dataOutput);

        isReal.write(dataOutput);



    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        idInVector = new LongWritable(dataInput.readLong());

        numOfOccurrences = new LongWritable(dataInput.readLong());

        typeInSentence.readFields(dataInput);

        isReal.readFields(dataInput);


    }

    @Override
    public int compareTo(DependencyPath other) {

        //edit here the comparison so that the fake path will appear first in the reducer and the other will be orderd
        //by their id
        int firstComparison = insideComparison(other);
        if (firstComparison == 0) {
            if (isFake() && !other.isFake()) {
                return 1;
            } else if (!isFake() && other.isFake()) {
                return -1;
            }
            else{
                return 0;
            }
        }
        else {
            return firstComparison;
        }
    }

    private int insideComparison(DependencyPath other) {
        return typeInSentence.compareTo(other.typeInSentence);
    }
    public boolean isFake(){
        return !isReal.get();
    }

    @Override
    public int hashCode() {
        return typeInSentence.hashCode();
    }

    @Override
    public String toString() { return typeInSentence.toString() + "\t" + numOfOccurrences + "\t" + idInVector.get(); }

    public void setIdInVector (long id){
        idInVector = new LongWritable(id);
    }

}



