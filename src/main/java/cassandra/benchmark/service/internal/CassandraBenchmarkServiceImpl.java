package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.CassandraBenchmarkService;
import cassandra.benchmark.service.CassandraClientType;
import cassandra.benchmark.service.internal.Astyanax.CassandraClientAstyanaxImpl;
import cassandra.benchmark.service.internal.Datastax.CassandraClientDatastaxImpl;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by cosh on 12.05.14.
 */
public class CassandraBenchmarkServiceImpl implements CassandraBenchmarkService {

    private static Logger logger = LogManager.getLogger("CassandraBenchmarkServiceImpl");

    private final static CassandraClientAstyanaxImpl astyanax = new CassandraClientAstyanaxImpl();
    private final static CassandraClientDatastaxImpl datastax = new CassandraClientDatastaxImpl();

    @Override
    public BenchmarkResult executeBenchmark(final CassandraClientType clientType,
                                            final String seedNode,
                                            final int port,
                                            final String clusterName,
                                            final long numberOfRows,
                                            final int wideRowCount,
                                            final int batchSize) {
        CassandraClient client = getClient(clientType);

        client.initialize(seedNode, port, clusterName);

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            final int numberOfBatches = getNumberOfBatches(numberOfRows, wideRowCount, batchSize);
            final long[] measures = new long[numberOfBatches];
            final Random prng = new Random();

            int currentColumnCount = 0;
            String identityBase = "identity";
            int currentIdentityNonce = prng.nextInt(23232323);
            Long currentBucket = new Long(prng.nextInt(54));

            IdentityBucketRK identity = new IdentityBucketRK(String.format("%s-%d", identityBase, currentIdentityNonce), currentBucket);

            for (int i = 0; i < numberOfBatches; i++) {

                List<Mutation> mutations = new ArrayList<Mutation>(batchSize);
                while (mutations.size() < batchSize) {

                    if (currentColumnCount < wideRowCount) {
                        //same identity
                    } else {
                        currentIdentityNonce = prng.nextInt(23232323);
                        currentBucket = new Long(prng.nextInt(54));
                        identity = new IdentityBucketRK(String.format("%s-%d", identityBase, currentIdentityNonce), currentBucket);
                        currentColumnCount = 0;
                    }

                    mutations.add(new Mutation(identity, prng.nextLong(), new CommunicationCV("aParty", "bParty", prng.nextDouble())));
                    currentColumnCount++;
                }

                measures[i] = client.executeBatch(mutations);
            }

            long endTime = System.nanoTime();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, getMax(measures), 0, 0, numberOfRows * wideRowCount, getTotal(measures), numberOfBatches, measurements);
        } finally {
            client.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime());
    }

    private List<Mutation> generateObjects(final Random prng, int batchSize, int wideRowCount) {
        List<Mutation> mutations = new ArrayList<Mutation>();

        for (int i = 0; i < batchSize; i++) {

            IdentityBucketRK identity = new IdentityBucketRK(createIdentiy(i, prng), new Long(i));

            for (int j = 0; j < wideRowCount; j++) {
                mutations.add(generateMutation(prng, identity, j));
            }
        }

        return mutations;
    }

    private Mutation generateMutation(final Random prng, final IdentityBucketRK row, int column) {

        Long timestamp = new Long(column);
        CommunicationCV communication = new CommunicationCV("aPartyName", "bPartyName", prng.nextDouble());

        return new Mutation(row, timestamp, communication);
    }

    private String createIdentiy(int row, Random prng) {
        return String.format("identity%s-%d", row, prng.nextInt(100));
    }

    private int getNumberOfBatches(long numberOfRows, final int wideRowCount, final int batchSize) {
        return (int) ((numberOfRows * wideRowCount) / batchSize);
    }

    @Override
    public BenchmarkResult createSchema(final CassandraClientType clientEnum, final String seedNode,
                                        final int port, final String clusterName, final int replicationFactor) {

        CassandraClient client = getClient(clientEnum);

        client.initialize(seedNode, port, clusterName);

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {
            long[] measures = new long[2];

            long measure1 = client.createKeyspace(replicationFactor);
            logger.debug("Created the keyspace {0} with replication factor {1}.", Constants.keyspaceName, replicationFactor);

            long measure2 = client.createTable();
            logger.debug("Created the table {0} in keyspace {1}.", Constants.tableNameCQL, Constants.keyspaceName);

            long endTime = System.nanoTime();

            measures[0] = measure1;
            measures[1] = measure2;

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, getMax(measures), 0, 0, 2, getTotal(measures), 2, measurements);
        } finally {
            client.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime());
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
        if (samples.length == 0) {
            return 0;
        }

        Arrays.sort(samples);

        return samples[samples.length - 1];
    }

    private long getMax(long[] samples) {
        if (samples.length == 0) {
            return 0;
        }

        long max = 0;

        for (int i = 0; i < samples.length; i++) {
            if (samples[i] > max) {
                max = samples[i];
            }
        }

        return max;
    }
}
