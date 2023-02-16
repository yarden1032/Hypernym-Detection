package localClasses;

import java.io.*;

//this class receives as input the data set for training and testing as txt and return arff file
public class TxtToArffConverter {

    public void convert(String inputPath,String outputPath) throws IOException {
        File fe = new File(inputPath);
        FileInputStream fis = new FileInputStream(fe);
        byte bt[] = new byte[fis.available()];
        fis.read(bt);
        fis.close();
        String st = new String(bt);
        String s1[] = st.trim().split("\n");
        String ar = "@relation hypernym\n";
        ar = ar + "\n";
        ar = ar + "@attribute vector string\n";
        ar = ar + "@attribute class {True,False}\n";
        ar = ar + "\n";
        ar = ar + "@data\n";
        for (int i = 0; i < s1.length; i++) {
            System.out.println("processing " + s1[i]);
            ar = ar + s1[i].replace(",", "") + "\n";
        }

        File fe1 = new File(outputPath);
        FileOutputStream fos1 = new FileOutputStream(fe1);
        fos1.write(ar.getBytes());
        fos1.close();
    }
}
