import org.bson.Document;

import java.util.ArrayList;

/**
 *
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */

public class Cluster {

    public int id;

    private ArrayList<Tweet> tweets = new ArrayList<Tweet>();

    public Cluster(ArrayList<Tweet> tweets) {

    }

    public Document toDocument() {
        Document cluster = new Document();
        cluster.append("tweets", tweets);
        return cluster;
    }

    //FIXME
    public int getCount() {
        return 0;
    }

    //FIXME
    public Location getCenter() {
        return null;
    }

    //FIXME
    public Tweet getNewestTweet() {
        return null;
    }
}
