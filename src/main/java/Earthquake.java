import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * ClusterAnalysis
 * Earthquake
 *
 * Created by Taha Doğan Güneş on 08/09/15.
 * Copyright (c) 2015. All rights reserved.
 */

public class Earthquake {
    public static final String[] COLLECTIONS = {"bgs","emsc","gdacs","geofon","geonet","usgs"};
    public static final int THRESHOLD_RADIUS_METERS = 100 * 1000;
    public static final int TIME_RANGE_SECONDS = 60 * 60;

    public final Location location;
    public final int timestamp;
    public final double magnitude;
    public final String title;
    public final String link;
    public final String provider;
    public final String id;


    public Earthquake(Document document, String provider) {
        this.location = new Location(document);
        this.timestamp = document.getInteger("timestamp");
        this.provider = provider;

        Object link = document.get("link");
        if (link instanceof String) {
            this.link = (String) link;
        }
        else {
            this.link = "N/A";
        }

        Object magnitude = document.get("magnitude");
        if (magnitude instanceof Integer) {
            this.magnitude = (Integer) magnitude;
        }
        else {
            this.magnitude = (Double) magnitude;
        }


        this.title = document.getString("title");
        this.id = document.getObjectId("_id").toString();
    }

    public int getLabel() {
        return (int) Math.round(this.magnitude);
    }

    public static Earthquake getMostRelevantReport(Cluster cluster, MongoClient mongoClient) {
        MongoDatabase db = mongoClient.getDatabase("earthquakeSources");
        Document query = Query.prepareQuery(cluster, Earthquake.THRESHOLD_RADIUS_METERS, Earthquake.TIME_RANGE_SECONDS);

        ArrayList<Earthquake> earthquakes = new ArrayList<Earthquake>();

        for (String collectionName: Earthquake.COLLECTIONS) {
            MongoCollection<Document> collection = db.getCollection(collectionName);

            for (Document document: collection.find(query)) {
                Earthquake earthquake = new Earthquake(document, collectionName);
                earthquakes.add(earthquake);
            }

        }

        if (!earthquakes.isEmpty()) {
            //getting biggest earthquake report from the list
            Collections.sort(earthquakes, new Comparator<Earthquake>() {
                public int compare(Earthquake o1, Earthquake o2) {
                    return Double.compare(o1.magnitude, o2.magnitude);
                }
            });
            Collections.reverse(earthquakes);
            return earthquakes.get(0);
        }
        return null;
    }


}
