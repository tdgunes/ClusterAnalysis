import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

/**
 *
 * ClusterCreator.java
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */

public class ClusterCreator {

    //These are from M.Walther and M.Kaiser
    public static final int minimumTweetCount = 3; // x
    public static final int lastYSeconds = 30 * 60; // y
    public static final int onRadiusMeters = 200; // z
    private MongoClient mongoClient = new MongoClient();
    private MongoDatabase db = this.mongoClient.getDatabase("data");
    private MongoDatabase analysisDatabase = this.mongoClient.getDatabase("analysis");
    private MongoCollection<Document> eventCandidates = this.analysisDatabase.getCollection("EventCandidate");
    private MongoCollection<Document> geocodedClean = this.db.getCollection("geocodedClean");

    public static void main(String[] args) {
        ClusterCreator creator = new ClusterCreator();
        creator.consumeAll();
    }

    public void consumeAll() {
        // get cursor from MongoDB and createEventCandidates
        FindIterable<Document> iterable = this.geocodedClean.find().sort(new Document("timestamp", -1));
        iterable.forEach(new Block<Document>() {

             public void apply(final Document document) {
                 Tweet tweet = new Tweet(document);
                 check(tweet);
             }
        });
    }

    public void check(Tweet tweet) {
        // "check if there are more than x other tweets issued in the last y minutes in a radius of z meters"
        ArrayList<Tweet> tweets = this.fetchRelevantTweets(tweet);
        if (tweets.size() > minimumTweetCount)  {
            tweets.add(tweet);
            this.store(tweets);
        }
    }

    private void store(ArrayList<Tweet> tweets) {
        // make a MongoDB Query to store EventCandidates
        Cluster cluster = new Cluster(tweets);
        this.eventCandidates.insertOne(cluster.toDocument());
    }

    private ArrayList<Tweet> fetchRelevantTweets(Tweet tweet) {
        ArrayList<Tweet> tweets = new ArrayList<Tweet>();
        System.out.println("> Preparing query:");
        Document query = this.prepareQuery(tweet);


        System.out.println("> Getting cursor:");
        MongoCursor<Document> cursor = geocodedClean.find(query).iterator();

        System.out.println("> Iterating...");
        try {
            while (cursor.hasNext()) {

                Document document = cursor.next();
                tweets.add(new Tweet(document));
            }
        } finally {
            cursor.close();
        }

        return tweets;
    }

    private Document prepareQuery(Tweet tweet){
        Document locationQuery = new Document();
        Document timestampQuery = new Document();
        Document nearQuery = new Document();
        Document finalQuery = new Document();

        locationQuery.append("$maxDistance", onRadiusMeters);
        locationQuery.append("$geometry", tweet.location.toGeoJSON());
        timestampQuery.append("$gte", tweet.timestamp - lastYSeconds);

        nearQuery.append("$near", locationQuery);

        finalQuery.append("loc", nearQuery);
        finalQuery.append("timestamp", timestampQuery);

        return finalQuery;
    }


}
