/**
 * ClusterAnalysis
 * DataSet
 *
 * Created by Taha Doğan Güneş on 08/09/15.
 * Copyright (c) 2015. All rights reserved.
 *
 * Note: Courtesy of Murat Sensoy
 */


import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class DataSet {
    private int counter=0;
    Instances instances;
    FastVector fvWekaAttributes;
    double limit=5;
    public DataSet(){
        Attribute Attribute1 = new Attribute("num_items");
        Attribute Attribute2 = new Attribute("sum_prob");
        Attribute Attribute3 = new Attribute("H");
        Attribute Attribute4 = new Attribute("W");

        // Declare the class attribute along with its values
        FastVector fvClassVal = new FastVector(7);
//        fvClassVal.addElement("0");
        fvClassVal.addElement("1");
        fvClassVal.addElement("2");
        fvClassVal.addElement("3");
        fvClassVal.addElement("4");
        fvClassVal.addElement("5");
        fvClassVal.addElement("6");
        fvClassVal.addElement("7");
        fvClassVal.addElement("8");
        Attribute ClassAttribute = new Attribute("Mag", fvClassVal);

        // Declare the feature vector
        fvWekaAttributes = new FastVector(14);
        fvWekaAttributes.addElement(Attribute1);
        fvWekaAttributes.addElement(Attribute2);
        fvWekaAttributes.addElement(Attribute3);
        fvWekaAttributes.addElement(Attribute4);
        for(int i=0;i<9;i++){
            fvWekaAttributes.addElement(new Attribute("cov"+i));
        }
        fvWekaAttributes.addElement(ClassAttribute);

        instances = new Instances("Rel", fvWekaAttributes, 100);
        instances.setClassIndex(13);
    }

    public Instance getInstance(Cluster c, String label) {
        double f[]= c.getFeatures();
        // Create the instance
        Instance instance = new Instance(14);
        for(int i=0;i<13;i++){
            instance.setValue((Attribute)fvWekaAttributes.elementAt(i), f[i]);
        }
        if(label!=null) instance.setValue((Attribute)fvWekaAttributes.elementAt(13), label);
        instance.setDataset(instances);
        return instance;
    }

    public void addData(Cluster c, String label) {
        Instance ins = getInstance(c,label);
        instances.add(ins);
        counter++;
    }
    public void resetCounter(){
        counter=0;
    }
    public Instances getInstances() {
        return instances;
    }

    public boolean shouldTrain() {
        boolean t=counter>=limit;
        return t;
    }
}
