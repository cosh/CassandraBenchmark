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

    private final String keyspaceName = "cassandraBenchmark";
    private final String tableName = "wideRowTable";


    @Override
    public BenchmarkResult executeBenchmark(final String seedNode, final String clusterName, final long numberOfRequests, final int batchSize) {
        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 11.12);
    }

    @Override
    public BenchmarkResult createSchema(final String seedNode, final String clusterName, final int replicationFactor) {

        final Cluster cluster = connect(seedNode, clusterName);
        final Session session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");

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

        session.close();
        cluster.close();

        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 11.12);
    }

    public Cluster connect(final String node, final String clusterName) {
        final Cluster cluster = Cluster.builder()
                .addContactPoint(node)
                .withClusterName(clusterName)
                .build();
        final Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: %s\n",
                metadata.getClusterName());
        return cluster;
    }
}
