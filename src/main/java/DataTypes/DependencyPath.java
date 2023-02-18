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

    public DependencyPath(Text typeInSentence) {

//        this.type = type;
        this.typeInSentence = typeInSentence;
 //       this.direction = direction;
        this.idInVector = new LongWritable(-1L);
        isReal = new BooleanWritable(true);
    }

    public DependencyPath(LongWritable idInVector, Text typeInSentence) {

        this.idInVector = idInVector;
        this.typeInSentence = typeInSentence;
        isReal = new BooleanWritable(true);
    }


    //this constructor builds fake dependency path
    public DependencyPath() {
        idInVector = new LongWritable(-1L);
        typeInSentence = new Text("");
        isReal = new BooleanWritable(false);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {


        idInVector.write(dataOutput);


        typeInSentence.write(dataOutput);

        //direction.write(dataOutput);

        isReal.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


        idInVector.readFields(dataInput);

        typeInSentence.readFields(dataInput);

        isReal.readFields(dataInput);

    }

    @Override
    public int compareTo(DependencyPath other) {

        //edit here the comparison so that the fake path will appear first in the reducer and the other will be orderd
        //by their id
        if (idInVector != null) {
            if (isFake() && !isFake()) {
                return 1;
            } else if (!isFake() && isFake()) {
                return -1;
            }
        }
        return insideComparison(other);
    }

    private int insideComparison(DependencyPath other) {
        return typeInSentence.compareTo(other.typeInSentence);
    }
    public boolean isFake(){
        return !isReal.get();
    }

    @Override
    public String toString() { return typeInSentence.toString(); }

}



