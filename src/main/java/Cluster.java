import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;

/**
 *
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */

public class Cluster {

    public String uuid;
    private final ArrayList<Tweet> tweets;
    private final HashSet<Long> tweetIDs = new HashSet<Long>();

    public Cluster(ArrayList<Tweet> tweets) {
        this.tweets = tweets;
        this.uuid = UUID.randomUUID().toString();
    }

    public Cluster(Document document) {
        ArrayList documents = (ArrayList) document.get("tweets");
        this.uuid = document.getString("uuid");
        this.tweets = new ArrayList<Tweet>();

        for(Object object: documents) {
            Document tweetDocument = (Document) object;
            this.tweets.add(new Tweet(tweetDocument));
        }

    }

    public Document toDocument() {
        Document cluster = new Document();
        ArrayList<Document> documents = getTweetsAsDocuments();

        cluster.append("uuid", this.uuid);
        cluster.append("tweets", documents);

        // not stored, used for database queries
        cluster.append("center", this.getCenter().toGeoJSON());
        cluster.append("count", this.getCount());
        cluster.append("timestamp", this.getTimestamp());

        return cluster;
    }

    public ArrayList<Tweet> getTweets() {
        return this.tweets;
    }

    public void addTweet(Tweet tweet) {
        if (!tweetIDs.contains(tweet.id)) {
            tweetIDs.add(tweet.id);
            this.tweets.add(tweet);
        }
    }

    public ArrayList<Document> getTweetsAsDocuments() {
        ArrayList<Document> documents = new ArrayList<Document>();
        for (Tweet tweet: this.tweets) {
            documents.add(tweet.toDocument());
        }
        return documents;
    }

    public int getCount() {
        return this.tweets.size();
    }

    public Location getCenter() {
        int count = this.getCount();
        Location[] locations = new Location[count];
        for (int i = 0; i < count; i++) {
            locations[i] = this.tweets.get(i).location;
        }
        return Location.findCenter(locations);
    }

    public long getTimestamp() {
        long timestamp = 0;
        for (Tweet tweet : tweets) {
            timestamp += tweet.timestamp;
        }
        return timestamp / this.getCount();
    }

    public void save() throws FileNotFoundException, UnsupportedEncodingException {
        String text = this.toDocument().toJson(new JsonWriterSettings(JsonMode.SHELL, true));
        PrintWriter writer = new PrintWriter(getTimestamp()+"-"+ this.uuid.substring(0,20) +".txt", "UTF-8");
        writer.println(text);
        writer.close();
    }

    //FIXME
    public Tweet getNewestTweet() {
        return null;
    }
}
