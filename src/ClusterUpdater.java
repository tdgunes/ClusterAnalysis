import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by tdgunes on 20/08/15.
 */
public class ClusterUpdater {

    public void final int operationIntervalSeconds = 5;
    public void final int thresholdDistanceMeters = 1000;
    public void final int maxNewestTweetLifespanSeconds = 48 * 60 * 60;
    public void final int maxTweetsPerCluster = 50; // n

    public void start() throws InterruptedException {
        while (true) {
            //eventCandidates = gather all of the EventCandidates(Clusters) from database
            ArrayList<Cluster> eventCandidates = this.getAllEventCandidates();
            //if candidate is older than n delete it directly
            this.deleteOldClusters(eventCandidates);

            this.mergeAll(eventCandidates);
            //merge event candidates if their center is above a threshold (like distance),
            // and if their result exceeds n tweets

            Thread.sleep(operationIntervalSeconds * 1000);
        }
    }

    private void deleteOldClusters(ArrayList<Cluster> eventCandidates) {
        for (Cluster cluster:eventCandidates) {
            if (getCurrentTime() - cluster.getNewestTweet().timestamp > maxNewestTweetLifespanSeconds) {
                this.remove(cluster);
            }
        }
    }

    private void remove(Cluster cluster) {
        // removes cluster from database
    }

    private long getCurrentTime() { // returns as seconds
        return Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() / 1000;
    }

    private boolean isValidMerge(Cluster cluster1, Cluster cluster2) {
        if ((cluster1.getCount() + cluster2.getCount()) > maxTweetsPerCluster)
            return false;
        return cluster1.getCenter().distanceToAsMeters(cluster2.getCenter()) <= thresholdDistanceMeters;
    }

    private void mergeAll(ArrayList<Cluster> eventCandidates) {
        for (Cluster candidate:eventCandidates) {
            for (Cluster candidate2:eventCandidates) {
                if (candidate.id != candidate2.id) { // if these are different clusters
                    boolean successful = this.merge(candidate, candidate2);
                    if (successful)
                        System.out.println("A successful merge!");
                }
            }
        }
    }

    private boolean merge(Cluster cluster1, Cluster cluster2) {
        if (isValidMerge(cluster1, cluster2) ) {
            //remove cluster1 and cluster2 from event candidates from database

            // merge them give a new identifier
        }

    }

    public ArrayList<Cluster> getAllEventCandidates() {
        return new ArrayList<Cluster>();
    }
}
