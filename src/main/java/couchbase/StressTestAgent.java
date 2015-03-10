package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.stream.IntStream;

/**
 * Couchbase에 멀티 스레드로 데이터를 주입하는 Agent Class.
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class StressTestAgent {

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
        IntStream.range(0, bigDataSize).parallel()
                .forEach(i -> {
                    try {
                        bucket
                                .async()
                                .upsert(JsonDocument.create("" + i, content));
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
