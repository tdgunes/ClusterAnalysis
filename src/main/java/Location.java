
import org.bson.Document;
import java.util.ArrayList;

/**
 *
 * ClusterAnalysis
 *
 * Created by Taha Doğan Güneş on 20/08/15.
 * Copyright (c) 2015. All rights reserved.
 *
 */
public class Location {
    public static final double R = 6372.8; // In kilometers
    private final double longitude;
    private final double latitude;

    public Location(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public Location(Document document) {
        Document location = (Document) document.get("loc");
        ArrayList coordinates = (ArrayList) location.get("coordinates");
        this.longitude = (Double) coordinates.get(0);
        this.latitude = (Double) coordinates.get(1);
    }

    public double distanceToAsMeters(Location location) {
        return Location.haversine(this.latitude, this.longitude, location.latitude, location.longitude) * 1000;
    }

    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));

        return R * c; //outputs as kilometers

    }

    public Document toDocument() {
        Document location = new Document();
        location.append("loc", this.toGeoJSON());
        return location;
    }

    public Document toGeoJSON () {
        /*
        {
            "type":"Point",
            "coordinates":[longitude, latitude]
        }
        */

        Document geoJSON = new Document();
        geoJSON.append("type", "Point");
        geoJSON.append("coordinates", new double[]{longitude, latitude});
        return geoJSON;
    }


    public static void main(String[] args) {
        System.out.println(haversine(36.12, -86.67, 33.94, -118.40));
    }
}
