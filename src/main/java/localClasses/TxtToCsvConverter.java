package localClasses;

import java.io.*;

//this class receives as input the data set for training and testing as txt and return arff file
public class TxtToCsvConverter {

    static final String csvPath = "vectors.csv";

    public void convert(String inputPath,String outputPath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.replace(",", "");
                    line = line.replace("\t", ",");
                    bw.write(line);
                }
            }
        }
    }
}



