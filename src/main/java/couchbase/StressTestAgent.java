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
 * Couchbase에 멀티 스레드로 데이터를 주입하는 Agent Class.
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class StressTestAgent {

	private static final AtomicLong throughputCount = new AtomicLong();

	public StressTestAgent() {
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

		// Create a JSON document and store it with the ID "helloworld"
		JsonObject content = JsonObject.create().put(key, value);

		// 5억건
		final int bigDataSize = 500000000;
		IntStream.range(0, bigDataSize)
				.parallel()
				.forEach(i -> {
					try {
						bucket
//								.async()
								.upsert(JsonDocument.create("" + i, content));
						throughputCount.incrementAndGet();
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}

	public static void main(String[] args) {
		StressTestAgent agent = new StressTestAgent();
		agent.run(args);
	}
}
