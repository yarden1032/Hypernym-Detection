package localClasses;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.trees.M5P;
import weka.core.Instances;

/** this class recieves as input a converted data in the format of Json File. the data structure would be Vector +  Boolean
 * representing if the vector is true or false for hypernym
 **/
public class ClassifierTrainer {

    public Classifier train(Instances data) throws Exception {
        if (data.classIndex() == -1)
            data.setClassIndex(data.numAttributes() - 1);

        //should my classifier be incremental or batch?
        return buildClassifier(data, false);

    }

    public static Classifier buildClassifier(Instances data, boolean isIncremental) throws Exception {

        NaiveBayes classifier = new NaiveBayes();
        classifier.buildClassifier(data);
        return classifier;
    }

}
