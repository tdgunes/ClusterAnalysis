import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;


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
    public final int maxTweetsPerCluster = 300; // n
    public final int timeRangeOfMergeSeconds = 60 * 60 * 24 ;
    public final int ITERATION = 30;
    public MongoCollection<Document> sourceCollection = null;
    public MongoCollection<Document> storageCollection = null;

    public static void main(String[] args) throws InterruptedException {
        ClusterUpdater updater = new ClusterUpdater();
        updater.clear();
        updater.start();
    }

    public void clear() {
        System.out.println("Dropping clusters");
        this.eventClusters.drop();
        for (int i = 1; i < ITERATION; i++) {
            System.out.println("Dropping clusters " + i + ".");
            this.analysisDatabase.getCollection("clusters" + i).drop();
        }

    }

    public void createIndexes(MongoCollection<Document> collection){
        collection.createIndex(new Document("count", 1));
        collection.createIndex(new Document("center", "2dsphere")); // FIXME: db.eventCandidates.createIndex( {"center": "2dsphere"} )
        collection.createIndex(new Document("uuid", 1));
    }

    public void start() throws InterruptedException {
        // eventCandidates = gather all of the EventCandidates(Clusters) from database
        this.sourceCollection = this.eventCandidates;
        this.storageCollection = this.eventClusters;
        this.createIndexes(this.storageCollection);

        MongoIterable<Document> iterable = this.sourceCollection.find().sort(new Document("timestamp", 1));
        iterable.forEach(new Block<Document>() {
            public void apply(Document document) {
                process(document);
            }
        });

        this.sourceCollection = this.eventClusters;
        this.storageCollection = this.analysisDatabase.getCollection("clusters1");
        this.createIndexes(this.storageCollection);

        for (int i = 1; i < ITERATION; i++) {
            System.out.println("Updating i = " + i + ".");

            MongoCursor<Document> it = this.sourceCollection.find().sort(new Document("timestamp", 1)).iterator();
            while (it.hasNext()) {
                Document document = it.next();
                process(document);
            }

            this.sourceCollection = this.analysisDatabase.getCollection("clusters" + i);
            this.storageCollection = this.analysisDatabase.getCollection("clusters" + (i+1));
            this.createIndexes(this.storageCollection);
        }


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
//                System.out.println("Reduced from "+ combinableClusters.size() + " to 1.");
                this.store(combined);
            }
        }
        else {
            this.store(cluster);
        }
    }

    private void store(Cluster combined) {
        System.out.println("["+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format((long) combined.getTimestamp() * 1000) + "] Saving a cluster with "+ combined.getTweets().size()+" tweets: " + combined.uuid);

        this.storageCollection.insertOne(combined.toDocument());
    }

    private boolean combinable(Cluster cluster, Cluster relevantCluster) {
        // from article, resulting cluster tweets must not exceed n limit
        return cluster.getCount() + relevantCluster.getCount() <= maxTweetsPerCluster && Math.abs(cluster.getTimestamp() - relevantCluster.getTimestamp()) < timeRangeOfMergeSeconds;
    }

    private ArrayList<Cluster> getPossibleCandidates(Cluster cluster) {
        Document query = this.prepareQuery(cluster);
        MongoCursor<Document> cursor = this.sourceCollection.find(query).iterator();
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
