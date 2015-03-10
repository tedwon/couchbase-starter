package stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A Stats instance gathers percentile based on a given histogram This class is thread unsafe.
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 * @see stats.StatsHolder for thread safe access Use createAndMergeFrom(proto) for best effort merge of this instance into the proto instance (no read / write lock is performed so the actual counts are a best effort)
 */
public class Stats {


    /**
     * SLF4J Logging
     */
    private static Logger logger = LoggerFactory.getLogger(Stats.class);

    private static final String UNIQUE_ID = ManagementFactory.getRuntimeMXBean().getName();
    private static final String UNIQUE_PRINT_ID = "[LATENCY_STATS_" + ManagementFactory.getRuntimeMXBean().getName() + "] ";
    private static final File file = new File("/tmp/stats.latency." + UNIQUE_ID + ".log");

    private static PrintStream out;

    private AtomicBoolean mustReset = new AtomicBoolean(false);

    final public String name;
    final public String unit;
    private long count;
    private double avg;

    private int[] histogram;
    private long[] counts;

    public Stats(String name, String unit, int... hists) {//10, 20, (20+ implicit)

        try {
            if (out == null) {
                out = new PrintStream(file);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

//        System.out.println(out);

        this.name = name;
        this.unit = unit;
        histogram = new int[hists.length + 1];//we add one slot for the implicit 20+
        System.arraycopy(hists, 0, histogram, 0, hists.length);
        histogram[histogram.length - 1] = hists[hists.length - 1] + 1;
        counts = new long[histogram.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = 0;
        }
    }

    /**
     * Use this method to merge this stat instance into a prototype one (for thread safe read only snapshoting)
     */
    public static Stats createAndMergeFrom(Stats model) {
        Stats r = new Stats(model.name, model.unit, 0);
        r.histogram = new int[model.histogram.length];
        System.arraycopy(model.histogram, 0, r.histogram, 0, model.histogram.length);
        r.counts = new long[model.histogram.length];

        r.merge(model);
        return r;
    }

    public void update(long ns) {
        if (mustReset.compareAndSet(true, false)) {
            internal_reset();
        }

        count++;
        avg = (avg * (count - 1) + ns) / count;
        if (ns >= histogram[histogram.length - 1]) {
            counts[counts.length - 1]++;
        } else {
            int index = 0;
            for (int level : histogram) {
                if (ns < level) {
                    counts[index]++;
                    break;
                }
                index++;
            }
        }
    }

    public void dump() {
        out.printf("---Stats - " + name + " (unit: " + unit + ")");
        out.printf("  Avg: %.0f #%d\n", avg, count);
        int index = 0;
        long lastLevel = 0;
        long occurCumul = 0;
        for (long occur : counts) {
            occurCumul += occur;
            if (index != counts.length - 1) {
                out.printf("  %7d < %7d: %6.2f%% %6.2f%% #%d\n",
                           lastLevel, histogram[index], (float) occur / count * 100,
                           (float) occurCumul / count * 100, occur);

                lastLevel = histogram[index];
            } else {
                out.printf("  %7d <    more: %6.2f%% %6.2f%% #%d\n", lastLevel, (float) occur / count * 100,
                           100f, occur);
            }
            index++;
        }
    }

    public String dumpAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append(format("---Stats - " + name + " (unit: " + unit + ")  Avg: %.0f #%d\n", avg, count));
        int index = 0;
        long lastLevel = 0;
        long occurCumul = 0;
        for (long occur : counts) {
            occurCumul += occur;
            if (index != counts.length - 1) {
                sb.append(format("  %7d < %7d: %6.2f%% %6.2f%% #%d\n",
                                 lastLevel, histogram[index], (float) occur / count * 100,
                                 (float) occurCumul / count * 100, occur));

                lastLevel = histogram[index];
            } else {
                sb.append(format("  %7d <    more: %6.2f%% %6.2f%% #%d\n", lastLevel, (float) occur / count * 100,
                                 100f, occur));
            }
            index++;
        }
        return sb.toString();
    }

    /**
     * Histogram 정보를 맵타입으로 리턴한다.<br> Java Map Type return Information about Histogram <p> --- Histogram Map Structure ---------------------------------- key           : value :   desc -------------------------------------------------------------- stats         : String                        :
     * Statement 명 unit          : String                        : 단위 avg_latency   : float                         : 평균 Latency event_tot_cnt : long : 총 Event 갯수 latency_list  : ArrayList<Map<String,Object>> : Latency 목록 --------------------------------------------------------------- <p/> ---
     * latency_list Map Structure ---------------------------------- key          : value  : desc -------------------------------------------------------------- to_latency   : String : 시작 Latency from_latency : String : 종료 Latency to_percent   : String : 시작 Percent from_percent : String : 종료 Percent
     * event_cnt    : String : Event 갯수 --------------------------------------------------------------- <p/> * </p>
     *
     * @return
     */

    private static final String HISTOGRAM_STATS = "stats";
    private static final String HISTOGRAM_UNIT = "unit";
    private static final String HISTOGRAM_AVERAGE_LATENCY = "avg_latency";
    private static final String HISTOGRAM_EVENT_TOT_CNT = "event_tot_cnt";
    private static final String HISTOGRAM_LATENCY_LIST = "latency_list";

    private static final String HISTOGRAM_TO_LATENCY = "to_latency";
    private static final String HISTOGRAM_FROM_LATENCY = "from_latency";
    private static final String HISTOGRAM_TO_PERCENT = "to_percent";
    private static final String HISTOGRAM_FROM_PERCENT = "from_percent";
    private static final String HISTOGRAM_EVENT_CNT = "event_cnt";

    public HashMap<String, Object> getHistogram() {
        HashMap<String, Object> hResult = null;
        ArrayList<HashMap<String, Object>> aLatencyList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> hLatency = null;

        hResult = new HashMap<String, Object>();

        hResult.put(HISTOGRAM_STATS, name);
        hResult.put(HISTOGRAM_UNIT, unit);
        hResult.put(HISTOGRAM_AVERAGE_LATENCY, avg);
        hResult.put(HISTOGRAM_EVENT_TOT_CNT, count);

        int index = 0;
        long lastLevel = 0;
        long occurCumul = 0;
        for (long occur : counts) {
            occurCumul += occur;
            if (index != counts.length - 1) {
                hLatency = new HashMap<String, Object>();

                hLatency.put(HISTOGRAM_TO_LATENCY, lastLevel);
                hLatency.put(HISTOGRAM_FROM_LATENCY, histogram[index]);
                hLatency.put(HISTOGRAM_TO_PERCENT, (float) occur / count * 100);
                hLatency.put(HISTOGRAM_FROM_PERCENT, (float) occurCumul / count * 100);
                hLatency.put(HISTOGRAM_EVENT_CNT, occur);

                lastLevel = histogram[index];

                aLatencyList.add(hLatency);
            } else {
                hLatency.put(HISTOGRAM_TO_LATENCY, lastLevel);
                hLatency.put(HISTOGRAM_FROM_LATENCY, "more");
                hLatency.put(HISTOGRAM_TO_PERCENT, (float) occur / count * 100);
                hLatency.put(HISTOGRAM_FROM_PERCENT, 100f);
                hLatency.put(HISTOGRAM_EVENT_CNT, occur);

            }
            index++;
        }

        hResult.put(HISTOGRAM_LATENCY_LIST, aLatencyList);

        return hResult;
    }


    public void merge(Stats stats) {
        // we assume same histogram - no check done here
        count += stats.count;
        avg = ((avg * count) + (stats.avg * stats.count)) / (count + stats.count);
        for (int i = 0; i < counts.length; i++) {
            counts[i] += stats.counts[i];
        }
    }

    private void internal_reset() {
        count = 0;
        avg = 0;
        for (int i = 0; i < counts.length; i++) {
            counts[i] = 0;
        }
    }

    public void reset() {
        mustReset.set(true);
    }

    private static String format(String format, Object... args) {
        Formatter formatter = new Formatter();
        String str = formatter.format(Locale.getDefault(), format, args).toString();
        return UNIQUE_PRINT_ID + str;
    }

    public static void main(String[] args) {

//        long engineLatency = 0;
//        StatsHolder.getEngine().update(engineLatency);
//        StatsHolder.dump("engine");

        Stats stats = new Stats("a", "any", 10, 20, 30);
        stats.update(1);
        stats.update(2);
        stats.update(10);
        stats.update(15);
        stats.update(25);
        stats.update(30);
        stats.update(35);
        stats.update(40);
        //stats.dump();

        Stats stats2 = new Stats("b", "any", 10, 20, 30);
        stats2.update(1);
        stats.merge(stats2);
//		stats.dump();
        HashMap<String, Object> histogram1 = stats.getHistogram();
        System.out.println(histogram1);

//      try {
//          final String jsonAnalyticsResult = writer.writeValueAsString(histogram1);
//          System.out.println(jsonAnalyticsResult);
//      } catch (IOException e) {
//          e.printStackTrace();
//      }
//
//		long l = 100;
//		long l2 = 3;
//		System.out.printf("%15.4f", (float) l / l2);

//      stats.dump();
        String dump = stats.dumpAsString();
        System.out.println(dump);

    }

//    private static final ObjectMapper mapper = new ObjectMapper();
//    private static final ObjectWriter writer = mapper.writer();
}
