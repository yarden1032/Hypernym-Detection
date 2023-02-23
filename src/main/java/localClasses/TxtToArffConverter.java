package localClasses;

import java.io.*;

//this class receives as input the data set for training and testing as txt and return arff file
public class TxtToArffConverter {

    static final String csvPath = "vectors.csv";

  /*  public void convert(String inputPath,String outputPath) throws IOException {

        convertTxtToCsv(inputPath,csvPath);
    } */

    public void convert(String inputPath,String outputPath) throws IOException {
        //first need to remove the commas
        File fe = new File(inputPath);
        FileInputStream fis = new FileInputStream(fe);
        byte bt[] = new byte[fis.available()];
        fis.read(bt);
        fis.close();
        String st = new String(bt);
        String s1[] = st.trim().split("\n");
        String ar = new String();
        for (int i = 0; i < s1.length; i++) {
            //System.out.println("processing " + s1[i]);
            s1[i] = s1[i].replace(",", "");
            s1[i] = s1[i].replace("\t",",");
            ar = ar + s1[i] + "\n";
        }

        File fe1 = new File(outputPath);
        FileOutputStream fos1 = new FileOutputStream(fe1);
        fos1.write(ar.getBytes());
        fos1.close();

    }

  /*  private void convertCsvToArff(String inputPath,String outputPath)  throws IOException{
        File fe = new File(inputPath);
        FileInputStream fis = new FileInputStream(fe);
        byte bt[] = new byte[fis.available()];
        fis.read(bt);
        fis.close();
        String st = new String(bt);
        String s1[] = st.trim().split("\n");
        String ar = "@relation hypernym\n";
        ar = ar + "@attribute vector string\n";
        ar = ar + "@attribute class {True,False}\n";
        ar = ar + "@data\n";
        for (int i = 0; i < s1.length; i++) {
            System.out.println("processing " + s1[i]);
            ar = ar + s1[i] + "\n";
        }

        File fe1 = new File(outputPath);
        FileOutputStream fos1 = new FileOutputStream(fe1);
        fos1.write(ar.getBytes());
        fos1.close();
    } */

    }



