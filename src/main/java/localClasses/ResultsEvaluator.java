package localClasses;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;

import java.util.Random;

public class ResultsEvaluator {
    public static void evaluateResults(Instances data, Classifier classifier) throws Exception {
        System.out.println("Evaluating results");
        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(classifier, data, 2, new Random(1));
        /**Precision - how many true positive from all positive (TP)/(TP + FP)
        Recall - how many true positive from  (TP) / (TP + FN)
        F1 Score = (2 x Precision x Recall) / (Precision + Recall) **/
        double precision = eval.precision(data.classIndex());
        double recall = eval.recall(data.classIndex());
        double f1 = eval.fMeasure(data.classIndex());
        System.out.println("Precision: " + precision +"\n" + "Recall: "+ recall +"\n" + "F1 Score: " + f1 );
    }
}
