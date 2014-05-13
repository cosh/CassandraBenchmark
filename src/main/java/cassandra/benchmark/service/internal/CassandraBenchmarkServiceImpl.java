package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.CassandraBenchmarkService;
import cassandra.benchmark.service.CassandraClientType;
import cassandra.benchmark.service.internal.Astyanax.CassandraClientAstyanaxImpl;
import cassandra.benchmark.service.internal.Datastax.CassandraClientDatastaxImpl;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * Created by cosh on 12.05.14.
 */
public class CassandraBenchmarkServiceImpl implements CassandraBenchmarkService {

    private static Logger logger = LogManager.getLogger("CassandraBenchmarkServiceImpl");

    private final static CassandraClientAstyanaxImpl astyanax = new CassandraClientAstyanaxImpl();
    private final static CassandraClientDatastaxImpl datastax = new CassandraClientDatastaxImpl();

    @Override
    public BenchmarkResult executeBenchmark(final CassandraClientType client, final String seedNode, final String clusterName, final long numberOfRows, final int wideRowCount, final int batchSize) {
        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 9.55, 11.12);
    }

    @Override
    public BenchmarkResult createSchema(final CassandraClientType clientEnum, final String seedNode, final String clusterName, final int replicationFactor) {

        CassandraClient client = getClient(clientEnum);

        client.initialize(seedNode, clusterName);

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {
            long[] measures = new long[2];

            long measure1 = client.createKeyspace(replicationFactor);
            logger.debug("Created the keyspace {0} with replication factor {1}.", Constants.keyspaceName, replicationFactor);

            long measure2 = client.createTable();
            logger.debug("Created the table {0} in keyspace {1}.", Constants.tableName, Constants.keyspaceName);

            long endTime = System.nanoTime();

            measures[0] = measure1;
            measures[1] = measure2;

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, getMax(measures), 0, 0, 2, getTotal(measures), 2, measurements);
        }
        finally {
            client.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.realOpRate(), ti.keyRate(),ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime() );
    }

    private CassandraClient getClient(CassandraClientType clientEnum) {
        CassandraClient client = null;

        switch (clientEnum) {
            case ASTYANAX:
                client = astyanax;
                break;

            case DATASTAX:
                client = datastax;
                break;

            default:
                client = datastax;
        }

        return client;
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
}
