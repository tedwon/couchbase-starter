package infinispan;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import java.io.File;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Created by ted.won on 4/24/15.
 */
public class InfinispanStressTestAgent {

	private static final AtomicLong throughputCount = new AtomicLong();

	public InfinispanStressTestAgent() {
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

	public static void main(String[] args) {

		//API entry point, by default it connects to localhost:11222
		RemoteCacheManager remoteCacheManager = new RemoteCacheManager();

		//obtain a handle to the remote default cache
		//        RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
		RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();

		// 5억건
		final int bigDataSize = 500000000;
		IntStream.range(0, bigDataSize)
				.parallel()
				.forEach(i -> {
					try {
						remoteCache.put("" + i, "ABCDEFGHIJ");
						throughputCount.incrementAndGet();
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}
}
