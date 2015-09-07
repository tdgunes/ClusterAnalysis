import org.apache.commons.math3.stat.correlation.Covariance;
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
    double minX=Double.MAX_VALUE,minY=Double.MAX_VALUE, maxX,maxY;

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

    private void calculateMinMax() {
        for (Tweet i: tweets) {
            if(minX > i.location.getLongitude()) minX=i.location.getLongitude();
            if(minY > i.location.getLatitude()) minY=i.location.getLatitude();
            if(maxX < i.location.getLongitude()) maxX=i.location.getLongitude();
            if(maxY < i.location.getLatitude()) maxY=i.location.getLatitude();
        }
    }

    public double[] getFeatures() {
        this.calculateMinMax();
        double[] f = new double[14];
        f[0] = this.getTweets().size();
        double mean[] = {0,0,0};
        for (Tweet tweet : tweets) {
            f[1] += tweet.probability;
            mean[0] += tweet.location.getLongitude();
            mean[1] += tweet.location.getLatitude();
            mean[2] += tweet.timestamp;
        }
        f[2] = maxX-minX;
        f[3] = maxY-minY;

        int count = tweets.size();
        if (count > 1) {
            mean[0] /= count;
            mean[1] /= count;
            mean[2] /= count;
            double data[][] = new double[count][3];
            int i = 0;
            for (Tweet tweet : tweets) {
                data[i][0] = tweet.location.getLongitude() - mean[0];
                data[i][1] = tweet.location.getLatitude() - mean[1];
                data[i][2] = tweet.timestamp - mean[2];
                i++;
            }

            Covariance covariance = new Covariance(data);
            double cov[][] = covariance.getCovarianceMatrix().getData();
            i=4;

            for (int j = 0; j < 3; j++) {
                for (int k = 0; k < 3; k++) {
                    f[i] = cov[j][k];
                    i++;
                }
            }


        }
        return f;

    }

}
