package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.CassandraBenchmarkService;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by cosh on 12.05.14.
 */
public class CassandraBenchmarkServiceImpl implements CassandraBenchmarkService {

    private static Logger logger = LogManager.getLogger("CassandraBenchmarkServiceImpl");

    private static final String keyspaceName = "cassandraBenchmark";
    private static final String tableName = "wideRowTable";


    @Override
    public BenchmarkResult executeBenchmark(final String seedNode, final String clusterName, final long numberOfRows, final int wideRowCount, final int batchSize) {
        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 9.55, 11.12);
    }

    @Override
    public BenchmarkResult createSchema(final String seedNode, final String clusterName, final int replicationFactor) {

        final Cluster cluster = connect(seedNode, clusterName);
        final Session session = cluster.connect();

        long[] measures = new long[2];

        long startTime = System.nanoTime();

        long measure1 = executeStatement(session,
                "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");

        logger.debug("Created the keyspace {0} with replication factor {1}.", keyspaceName, replicationFactor);

        long measure2 = executeStatement(session,
                "CREATE TABLE IF NOT EXISTS " + keyspaceName + " ." + tableName + " (" +
                        "identity text," +
                        "timeBucket int," +
                        "time timeuuid," +
                        "aparty text," +
                        "bparty text," +
                        "duration float," +
                        "primary key((identity, timeBucket), time)" +
                        ");"
        );

        long endTime = System.nanoTime();

        measures[0] = measure1;
        measures[1] = measure2;

        logger.debug("Created the table {0} in keyspace {1}.", tableName, keyspaceName);

        SampleOfLongs measurements = new SampleOfLongs(measures, 1);

        TimingInterval ti = new TimingInterval(startTime, endTime, getMax(measures), 0, 0, 2, getTotal(measures), 2, measurements);

        session.close();
        cluster.close();

        return new BenchmarkResult(ti.operationCount, ti.realOpRate(), ti.keyRate(),ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime() );
    }

    private long getTotal(long[] samples) {
        if(samples.length == 0)
        {
            return 0;
        }

        Arrays.sort(samples);

        return samples[samples.length - 1];
    }

    private long getMax(long[] samples) {
        if(samples.length == 0)
        {
            return 0;
        }
        
        long max = 0;

        for (int i = 0; i < samples.length; i++) {
            if(samples[i] > max)
            {
                max = samples[i];
            }
        }
        
        return max;
    }

    private long executeStatement(Session session, String statement) {
        long startTime = System.nanoTime();
        session.execute(statement);
        return System.nanoTime() - startTime;
    }

    private final static double calculateSeconds(final long estimatedTime) {
        return (double)estimatedTime / 1000000000.0;
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
