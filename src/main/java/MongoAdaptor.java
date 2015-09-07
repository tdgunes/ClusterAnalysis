import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 21/08/15.
 * Copyright (c) 2015. All rights reserved.
 */

public class MongoAdaptor {
    protected MongoClient mongoClient = new MongoClient();
    protected MongoDatabase db = this.mongoClient.getDatabase("data");
    protected MongoDatabase analysisDatabase = this.mongoClient.getDatabase("analysis");
    protected MongoCollection<Document> eventCandidates = this.analysisDatabase.getCollection("eventCandidate");
    protected MongoCollection<Document> eventClusters = this.analysisDatabase.getCollection("clusters");
    protected MongoCollection<Document> geocodedClean = this.db.getCollection("geocodedClean");

}
