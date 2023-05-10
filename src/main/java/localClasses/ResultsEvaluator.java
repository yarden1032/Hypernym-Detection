package localClasses;

import org.tinylog.Logger;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.Prediction;
import weka.core.Instances;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ResultsEvaluator {

    ArrayList<String> nounPairs;

    public ResultsEvaluator(ArrayList nounPairs) {

        this.nounPairs = nounPairs;

    }

    public void evaluateResults(Instances data, Classifier classifier) throws Exception {
        Logger.info("Evaluating results");
        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(classifier, data, 10, new Random(1));
        /**Precision - how many true positive from all positive (TP)/(TP + FP)
         Recall - how many true positive from  (TP) / (TP + FN)
         F1 Score = (2 x Precision x Recall) / (Precision + Recall) **/
        double precision = eval.precision(data.classIndex());
        double recall = eval.recall(data.classIndex());
        double f1 = eval.fMeasure(data.classIndex());
        Logger.info("Precision: " + precision + "\n" + "Recall: " + recall + "\n" + "F1 Score: " + f1 + "\n");
        //resultExample(eval);
    }

    private void resultExample(Evaluation evaluation) {
        List<Prediction> predictionList = evaluation.predictions();
        List<String> truePositives = new ArrayList<>();
        List<String> falsePositives = new ArrayList<>();
        List<String> trueNegatives = new ArrayList<>();
        List<String> falseNegatives = new ArrayList<>();
        boolean complete = false;
        for (int i = 0; !complete && i < predictionList.size(); i++) {
            // 1 is true 0 is false
            Prediction currPrediction = predictionList.get(i);
            String currNounPair = nounPairs.get(i);
            if (truePositives.size() < 5 && currPrediction.predicted() == 1 && currPrediction.actual() == 1) {
                truePositives.add(currNounPair);
            } else {
                if (falsePositives.size() < 5 && currPrediction.predicted() == 1 && currPrediction.actual() == 0) {
                    falsePositives.add(currNounPair);
                } else {
                    if (trueNegatives.size() < 5 && currPrediction.predicted() == 0 && currPrediction.actual() == 0) {
                        trueNegatives.add(currNounPair);
                    } else {
                        if (falseNegatives.size() < 5 && currPrediction.predicted() == 0 && currPrediction.actual() == 1) {
                            falseNegatives.add(currNounPair);
                        }
                    }
                }
            }

            if (truePositives.size() == 5 && falsePositives.size() == 5 && trueNegatives.size() == 5 && falseNegatives.size() == 5) {
                complete = true;
            }
        }

        Logger.info("True Positives: " + truePositives + "\n");
        Logger.info("False Positives: " + falsePositives + "\n");
        Logger.info("True Negatives: " + trueNegatives + "\n");
        Logger.info("False Negatives: " + falseNegatives + "\n");
    }
}
