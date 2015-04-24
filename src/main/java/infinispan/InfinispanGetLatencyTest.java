package infinispan;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import couchbase.CouchbaseUtil;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import stats.StatsHolder;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Couchbase get operation시 latency가 얼마나 일정한지 테스트하는 class.
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class InfinispanGetLatencyTest {

	private static final AtomicLong throughputCount = new AtomicLong();

	public InfinispanGetLatencyTest() {
        Timer timer = new Timer("", true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                StatsHolder.dump("engine");
            }
        }, 0L, 10000L);

		Timer engineThroughputStatsTimer = new Timer("", true);
		try {
			engineThroughputStatsTimer.scheduleAtFixedRate(new TimerTask() {
				final AtomicLong beforeInputCount = new AtomicLong();
				@Override
				public void run() {
					// Thread 정상 종료
					if (Thread.currentThread().isInterrupted()) {
						return;
					}

					try {
						// Throughput
						final long currentInputCount = throughputCount.get();
						final long engineThroughput = currentInputCount - beforeInputCount.get();
						beforeInputCount.set(currentInputCount);

						System.out.println(engineThroughput);

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}, 666L, 1000L);
		} catch (Throwable e) {
		}
    }

    public static void run(String... nodes) {

		//API entry point, by default it connects to localhost:11222
		RemoteCacheManager remoteCacheManager = new RemoteCacheManager();

		//obtain a handle to the remote default cache
		//        RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
		RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();

        final int bigDataSize = 500000000;
        IntStream.range(0, bigDataSize)
                .parallel()
                .forEach(i -> {
                    try {
//                        long start = System.currentTimeMillis();
                        remoteCache.get("" + i);
//                        long stop = System.currentTimeMillis();
//                        long latency = stop - start;
						throughputCount.incrementAndGet();

//						StatsHolder.getEngine().update(latency);
//                        System.out.println(latency);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });


    }

    public static void main(String[] args) {
        InfinispanGetLatencyTest agent = new InfinispanGetLatencyTest();
        agent.run(args);
    }
}
