import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.SimpleDateFormat;
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

public class ClusterCreator extends MongoAdaptor {

    //These are from M.Walther and M.Kaiser
    public static final int minimumTweetCount = 3; // x
    public static final int lastYSeconds =  30; // y
    public static final int onRadiusMeters = 500; // z

    public static void main(String[] args) {
        ClusterCreator creator = new ClusterCreator();
        creator.clear();
        creator.consumeAll();
        creator.createIndexes();
    }

    public void clear(){
        eventCandidates.drop();
    }

    public void createIndexes(){
        eventCandidates.createIndex(new Document("count", 1));
        eventCandidates.createIndex(new Document("center","2dsphere"));
        eventCandidates.createIndex(new Document("uuid", 1));
    }

    public void consumeAll() {
        // get cursor from MongoDB and createEventCandidates
        FindIterable<Document> iterable = this.geocodedClean.find().sort(new Document("timestamp", 1));
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
        System.out.println("["+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(tweets.get(tweets.size()-1).timestamp * 1000) + "] Saving a cluster with "+tweets.size()+" tweets.");
        this.eventCandidates.insertOne(cluster.toDocument());
    }

    private ArrayList<Tweet> fetchRelevantTweets(Tweet tweet) {
        final ArrayList<Tweet> tweets = new ArrayList<Tweet>();
        Document query = this.prepareQuery(tweet);
        MongoCursor cursor = geocodedClean.find(query).iterator();

        try {
            while (cursor.hasNext()) {
                Tweet t = new Tweet((Document) cursor.next());

                if (tweet.location.distanceToAsMeters(t.location) > 1.0) {
                    boolean distinct = true;
                    for (Tweet old:tweets) {
                        if (old.text.equalsIgnoreCase(t.text)){
                            distinct = false;
                            break;
                        }
                    }
                    if (distinct) {
                        tweets.add(t);
                    }

                }

            }
        } finally {
            cursor.close();
        }


        return tweets;
    }

    private Document prepareQuery(Tweet tweet) {
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
