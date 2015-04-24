package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
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
public class GetLatencyTest {

	private static final AtomicLong throughputCount = new AtomicLong();

	public GetLatencyTest() {
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
		final String key = "1";
		final int value = 1;

		// Connect to the bucket and open it
		CouchbaseUtil cb = new CouchbaseUtil(nodes);
		Bucket bucket = cb.getBucket("default");

		final int bigDataSize = 500000000;
		IntStream.range(0, bigDataSize)
				.parallel()
				.forEach(i -> {
					try {
//						long start = System.currentTimeMillis();
						JsonDocument found = bucket.get("" + i);
						//                        Integer anInt = found.content().getInt("1");
//						long stop = System.currentTimeMillis();
//						long latency = stop - start;
//						StatsHolder.getEngine().update(latency);
						//                        System.out.println(latency);
						throughputCount.incrementAndGet();
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

	}

	public static void main(String[] args) {
		GetLatencyTest agent = new GetLatencyTest();
		agent.run(args);
	}
}
