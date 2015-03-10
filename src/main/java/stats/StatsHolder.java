package stats;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


/**
 * A container for thread local Stats instances. Upon dump, data is gathered and merged from all registered thread local instances. The stat name is used as a key for a dump.
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class StatsHolder {

    public static int[] DEFAULT_MS = new int[]{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 300, 400, 500, 1000};//ms
    public static int[] SPOUT_MS = new int[]{10, 25, 50, 75, 100, 150, 200, 350, 500, 1000};//ms
    public static int[] DEFAULT_NS = new int[]{5, 10, 15, 20, 25, 50, 100, 500, 1000, 2500, 5000};
//micro secs

//	static {
//		for (int i = 0; i < DEFAULT_NS.length; i++)
//			DEFAULT_NS[i] *= 1000;//turn to ns
//	}

    private static List<Stats> STATS = Collections.synchronizedList(new ArrayList<Stats>());

    private static ThreadLocal<Stats> statsEngine = new ThreadLocal<Stats>() {
        protected synchronized Stats initialValue() {
//			Stats s = new Stats("engine", "ns", DEFAULT_NS);
            Stats s = new Stats("engine", "ms", DEFAULT_MS);
            STATS.add(s);
            return s;
        }
    };

    public static Stats getEngine() {
        return statsEngine.get();
    }

    private static ThreadLocal<Stats> statsSpout = new ThreadLocal<Stats>() {
        protected synchronized Stats initialValue() {
            Stats s = new Stats("spout", "ms", SPOUT_MS);
            STATS.add(s);
            return s;
        }
    };

    public static Stats getSpout() {
        return statsSpout.get();
    }

    public static void remove(Stats stats) {
        STATS.remove(stats);
    }

    public static void dump(String name) {
        Stats sum = null;
        for (Stats s : STATS) {
            if (name.equals(s.name)) {
                if (sum == null) {
                    sum = Stats.createAndMergeFrom(s);
                } else {
                    sum.merge(s);
                }
            }
        }
        if (sum != null) {
            sum.dump();
        }
    }

    public static String histogram(String name) {
        String dump = null;
        Stats sum = null;
        for (Stats s : STATS) {
            if (name.equals(s.name)) {
                if (sum == null) {
                    sum = Stats.createAndMergeFrom(s);
                } else {
                    sum.merge(s);
                }
            }
        }
        if (sum != null) {
            dump = sum.dumpAsString();
        }
        return dump;
    }

    public static HashMap<String, Object> getHistogram(String name) {
        Stats sum = null;
        for (Stats s : STATS) {
            if (name.equals(s.name)) {
                if (sum == null) {
                    sum = Stats.createAndMergeFrom(s);
                } else {
                    sum.merge(s);
                }
            }
        }
        if (sum == null) {
            return null;
        }
        return sum.getHistogram();
    }

    public static void reset() {
        for (Stats s : STATS) {
            s.reset();
        }
    }
}