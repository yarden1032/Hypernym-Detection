package localClasses;

import java.io.*;
import java.util.ArrayList;

//this class receives as input the data set for training and testing as txt and return arff file
public class TxtToCsvConverter {

    static final String csvPath = "vectors.csv";

    public ArrayList<String> convert(String inputPath,String outputPath) throws IOException {
        ArrayList<String> nounPairs = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String [] splitToClassifierData = line.split("\t");
                    if (splitToClassifierData.length == 3) {
                        String nounPair = splitToClassifierData[0];
                        String vector = splitToClassifierData [1];
                        String isHypernym = splitToClassifierData [2];
                        vector = vector.replace(",", "");
                        String dataToClassifier  = vector + "," + isHypernym;
                        bw.write(dataToClassifier + "\n");
                        nounPairs.add(nounPair);
                    }
                }
            }
        }

        return nounPairs;
    }
}



