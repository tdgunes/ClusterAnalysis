import org.bson.Document;

/**
 *
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */

public class Tweet {

    // category: 0 for text, 1 for location, 2 for withLoc
    enum Category {
        WITH_GPS, LOCATION, TEXT, UNKNOWN;

        public static Category find(Document document){
            int category = document.getInteger("category");
            return Category.find(category);
        }

        public static Category find(int category) {
            switch (category) {
                case 0:
                    return Category.TEXT;
                case 1:
                    return Category.LOCATION;
                case 2:
                    return Category.WITH_GPS;
            }
            return Category.UNKNOWN;
        }

        public int rawValue() {
            switch (this) {
                case TEXT:
                    return 0;
                case LOCATION:
                    return 1;
                case WITH_GPS:
                    return 2;
                case UNKNOWN:
                    return -1; // must not happen
            }
            return -1; // must not happen
        }
    }

    public final int timestamp;
    public final long id;
    public final double probability;  // that this tweet belongs to (x,y)
    public final Category category;
    public final Location location;
    public final String text;

    public Tweet(Document document) {
        this.text = document.getString("text");
        this.timestamp = document.getInteger("timestamp");
        this.id = document.getLong("id");
        Object probability =  document.get("probability");

        if (probability instanceof Integer) {
            Integer integer = (Integer) probability;
            this.probability = (double) integer;
        }
        else if (probability instanceof Double) {
            this.probability = (Double) probability;
        }
        else {
            System.out.println("Unable load probability of tweet");
            this.probability = 0;
        }
        this.category = Category.find(document);
        this.location = new Location(document);
    }

    public Document toDocument() {
        Document tweet = new Document();
        tweet.append("text", this.text);
        tweet.append("timestamp", this.timestamp);
        tweet.append("id", this.id);
        tweet.append("probability", this.probability);
        tweet.append("category", this.category.rawValue());
        tweet.append("loc", this.location.toGeoJSON());
        return tweet;
    }




}
