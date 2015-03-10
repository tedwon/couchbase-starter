package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Couchbase cluster connection management utility class.
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class CouchbaseUtil {

    // Create a cluster reference
    private CouchbaseCluster cluster;

    /**
     * localhost cb 접속.
     */
    public CouchbaseUtil() {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment
                .builder()
                .bootstrapCarrierDirectPort(11215)
                .build();

        cluster = CouchbaseCluster.create(env);
    }

    /**
     * remote cb 접속.
     */
    public CouchbaseUtil(String... nodes) {
        cluster = CouchbaseCluster.create(nodes);
    }

    public CouchbaseCluster create() {
        return cluster;
    }

    public Bucket getBucket(String bucket) {
        return create().openBucket();
//        return create().openBucket(bucket);
    }

    public void disconnect() {
        cluster.disconnect();
    }
}
