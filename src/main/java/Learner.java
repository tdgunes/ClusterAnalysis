
import org.bson.Document;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;

import java.util.ArrayList;

/**
 * ClusterAnalysis
 * Learner
 *
 * Created by Taha Doğan Güneş on 08/09/15.
 * Copyright (c) 2015. All rights reserved.
 */

public class Learner extends MongoAdaptor {
    Classifier classifier = new J48();
    DataSet data = new DataSet();


    public Learner() throws Exception {

        for(Document document: this.eventClusters.find().sort(new Document("timestamp", 1))) {
            Cluster cluster = new Cluster(document);
            Earthquake earthquake = Earthquake.getMostRelevantReport(cluster, this.mongoClient);
            if (earthquake != null) {
                System.out.println("Label: "+earthquake.getLabel());
                data.addData(cluster, "" + earthquake.getLabel());
                System.out.println("C:"+earthquake.title);
            }
            else {
                data.addData(cluster, "0");
            }
        }

        classifier.buildClassifier(data.getInstances());

        Evaluation eval = new Evaluation(data.getInstances());
        eval.evaluateModel(classifier, data.getInstances());
        System.out.println(eval.toSummaryString("\nResults\n======\n", false));
    }

    public static void main(String args[]) throws Exception {
        Learner learner = new Learner();

    }
}
