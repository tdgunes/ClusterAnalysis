import com.mongodb.Block;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import static com.mongodb.client.model.Filters.eq;

/**
 *
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */

public class ClusterUpdater extends MongoAdaptor {

    public final int operationIntervalSeconds = 5;
    public final int thresholdDistanceMeters = 1000;
    public final int maxNewestTweetLifespanSeconds = 48 * 60 * 60;
    public final int maxTweetsPerCluster = 50; // n
    public final int timeRangeOfMergeSeconds = 60 * 60;

    public void start() throws InterruptedException {
        // eventCandidates = gather all of the EventCandidates(Clusters) from database
        MongoIterable<Document> iterable = eventCandidates.find();

        iterable.forEach(new Block<Document>() {
            public void apply(Document document) {
                process(document);
            }
        });
    }

    private void process(Document document) {
        // merge event candidates if their center is above a threshold (like distance),
        // and if their result should not exceed n tweets, temporally + spatially
        Cluster cluster = new Cluster(document);
        ArrayList<Cluster> clusters = this.getPossibleCandidates(cluster); // for merging
        ArrayList<Cluster> combinableClusters = new ArrayList<Cluster>(); // final selected clusters for merging

        if (clusters.size() > 0) {
            for (Cluster relevantCluster : clusters) {
                if (this.combinable(cluster, relevantCluster)) {
                    combinableClusters.add(relevantCluster);
                }
            }
            if (combinableClusters.size() > 0) {
                Cluster combined = this.merge(cluster, combinableClusters);
                this.store(combined);
            }
        }
    }

    private void store(Cluster combined) {
        this.eventClusters.insertOne(combined.toDocument());
    }

    private boolean combinable(Cluster cluster, Cluster relevantCluster) {
        // from article, resulting cluster tweets must not exceed n limit
        return cluster.getCount() + relevantCluster.getCount() <= maxTweetsPerCluster;
    }

    private ArrayList<Cluster> getPossibleCandidates(Cluster cluster) {
        Document query = this.prepareQuery(cluster);
        MongoCursor<Document> cursor = eventCandidates.find(query).iterator();
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();

        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                clusters.add(new Cluster(document));
            }
        } finally {
            cursor.close();
        }

        return clusters;
    }

    private long getCurrentTime() { // returns as seconds
        return Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() / 1000;
    }


    private Cluster merge(Cluster origin, ArrayList<Cluster> clusters) {
        Cluster combined = new Cluster(origin.getTweets());

        for (Cluster cluster : clusters) {
            for (Tweet tweet : cluster.getTweets()) {
                combined.addTweet(tweet);
            }
        }

        return combined;
    }

    private Document prepareQuery(Cluster cluster) {
        Document locationQuery = new Document();
        Document timestampQuery = new Document();
        Document nearQuery = new Document();
        Document finalQuery = new Document();

        locationQuery.append("$maxDistance", thresholdDistanceMeters);
        locationQuery.append("$geometry", cluster.getCenter().toGeoJSON());
        timestampQuery.append("$gte", cluster.getTimestamp() - timeRangeOfMergeSeconds);
        timestampQuery.append("$lte", cluster.getTimestamp() + timeRangeOfMergeSeconds);

        nearQuery.append("$near", locationQuery);

        finalQuery.append("center", nearQuery);
        finalQuery.append("timestamp", timestampQuery);

        return finalQuery;
    }

}
