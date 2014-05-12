package cassandra.benchmark.service;

import cassandra.benchmark.transfer.BenchmarkResult;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by cosh on 12.05.14.
 */
public class CassandraBenchmarkServiceImpl implements CassandraBenchmarkService {

    private static Logger logger = LogManager.getLogger("CassandraBenchmarkServiceImpl");

    private static final String keyspaceName = "cassandraBenchmark";
    private static final String tableName = "wideRowTable";


    @Override
    public BenchmarkResult executeBenchmark(final String seedNode, final String clusterName, final long numberOfRows, final int wideRowCount, final int batchSize) {
        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 11.12);
    }

    @Override
    public BenchmarkResult createSchema(final String seedNode, final String clusterName, final int replicationFactor) {

        final Cluster cluster = connect(seedNode, clusterName);
        final Session session = cluster.connect();

        long startTime = System.nanoTime();

        createSchema_private(replicationFactor, session);

        long estimatedTime = System.nanoTime() - startTime;

        session.close();
        cluster.close();

        return new BenchmarkResult(2, 0.0, 0.0, 0.0, 0.0, 0.0, calculateSeconds(estimatedTime));
    }

    private final static double calculateSeconds(final long estimatedTime) {
        return (double)estimatedTime / 1000000000.0;
    }

    private final static void createSchema_private(int replicationFactor, Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");

        logger.debug("Created the keyspace {0} with replication factor {1}.", keyspaceName, replicationFactor);

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + keyspaceName + " ." + tableName + " (" +
                        "identity text," +
                        "timeBucket int," +
                        "time timeuuid," +
                        "aparty text," +
                        "bparty text," +
                        "duration float," +
                        "primary key((identity, timeBucket), time)" +
                        ");");

        logger.debug("Created the table {0} in keyspace {1}.", tableName, keyspaceName);
    }

    public Cluster connect(final String node, final String clusterName) {
        final Cluster cluster = Cluster.builder()
                .addContactPoint(node)
                .withClusterName(clusterName)
                .build();
        final Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: %s\n",
                metadata.getClusterName());
        return cluster;
    }
}
