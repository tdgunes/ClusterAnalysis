import org.bson.Document;

/**
 * ClusterAnalysis
 * <p/>
 * Created by Taha Doğan Güneş on 08/09/15.
 * Copyright (c) 2015. All rights reserved.
 */
public class Query {



    public static Document prepareQuery(Cluster cluster, int thresholdDistanceMeters, int timeRangeOfMergeSeconds) {
        Document locationQuery = new Document();
        Document timestampQuery = new Document();
        Document nearQuery = new Document();
        Document finalQuery = new Document();

        locationQuery.append("$maxDistance", thresholdDistanceMeters);
        locationQuery.append("$geometry", cluster.getCenter().toGeoJSON());
        timestampQuery.append("$gte", cluster.getTimestamp() - timeRangeOfMergeSeconds);
        timestampQuery.append("$lte", cluster.getTimestamp() + timeRangeOfMergeSeconds);

        nearQuery.append("$near", locationQuery);

        finalQuery.append("loc", nearQuery);
        finalQuery.append("timestamp", timestampQuery);

        return finalQuery;
    }
}
