
import org.bson.Document;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;


import java.util.Random;

/**
 * ClusterAnalysis
 * Learner
 *
 * Created by Taha Doğan Güneş on 08/09/15.
 * Copyright (c) 2015. All rights reserved.
 */

public class Learner extends MongoAdaptor {
    Classifier classifier = new J48();
    DataSet train = new DataSet();


    public Learner() throws Exception {


        DataSet set = train;
        for(Document document: this.eventClusters.find().sort(new Document("timestamp", 1))) {
            Cluster cluster = new Cluster(document);
            Earthquake earthquake = Earthquake.getMostRelevantReport(cluster, this.mongoClient);
            if (earthquake != null) {
//                System.out.println("Label "+ earthquake.getLabel());
                set.addData(cluster, "1");
                System.out.println("C:"+earthquake.title +" uuid: "+cluster.uuid + " link:" + earthquake.link);
            }
            else {
                set.addData(cluster, "0");
            }


        }

        classifier.buildClassifier(train.getInstances());

        Evaluation eval = new Evaluation(train.getInstances());
        eval.crossValidateModel(classifier, train.getInstances(), 10, new Random(1));
        System.out.println(eval.toSummaryString("\nResults\n======\n", true));

        System.out.println(eval.toClassDetailsString());
        System.out.println(eval.toMatrixString());
    }

    public static void main(String args[]) throws Exception {
        Learner learner = new Learner();

    }
}
