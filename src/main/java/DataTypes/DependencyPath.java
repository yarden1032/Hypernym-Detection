package DataTypes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//Roni: should be changed here so the path can have dynamic length;
public class DependencyPath
        implements WritableComparable<DependencyPath> {

    public Long idInVector;
    public String type;
    public String typeInSentence;

    public LongWritable direction;

    private boolean isReal = true;

    public DependencyPath(String type, String typeInSentence, LongWritable direction) {

        this.type = type;
        this.typeInSentence = typeInSentence;
        this.direction = direction;
    }

    //this constructor builds fake dependency path
    public DependencyPath(Long idInVector,String type, String typeInSentence, LongWritable direction) {

        this.idInVector = idInVector;
        this.type = type;
        this.typeInSentence = typeInSentence;
        this.direction = direction;
    }

    public DependencyPath() {

        isReal = false;

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {


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
        return other.type.compareTo(this.type)==0?(other.typeInSentence.compareTo(this.typeInSentence)==0?direction.compareTo(other.direction):other.typeInSentence.compareTo(this.typeInSentence)):other.type.compareTo(this.type);
    }
    public boolean isFake(){
        return !isReal;
    }

    @Override
    public String toString() {
        return type + ' ' + typeInSentence + ' '+ direction.toString();
    }
}


