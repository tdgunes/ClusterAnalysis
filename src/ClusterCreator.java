/**
 * Created by tdgunes on 20/08/15.
 */
public class ClusterCreator {

    //These are from M.Walther and M.Kaiser
    public static final int minimumTweetCount = 3; // x
    public static final int lastYMinutes = 30; // y
    public static final int onRadiusMeters = 200; // z

    public void check(Tweet tweet) {
        if (this.valid(tweet)) {
            //write this to database as event candidates
        }
        else {
            //do nothing
        }
    }

    private boolean valid(Tweet tweet) {
        // "check if there are more than x other tweets issued in the last y minutes in a radius of z meters"
        // make a MongoDB Query about that

        return false;
    }


}
