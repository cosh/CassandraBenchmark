package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.CassandraBenchmarkService;
import cassandra.benchmark.service.CassandraClientType;
import cassandra.benchmark.service.internal.Astyanax.CassandraClientAstyanaxImpl;
import cassandra.benchmark.service.internal.Datastax.CassandraClientDatastaxImpl;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static cassandra.benchmark.service.internal.helper.DataGenerator.createRandomIdentity;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getARandomBucket;

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
            final Long[] measures = new Long[numberOfBatches];
            final Random prng = new Random();

            int currentColumnCount = 0;
            String identityBase = "identity";
            Integer currentBucket = getARandomBucket(prng);

            IdentityBucketRK identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);

            for (int i = 0; i < numberOfBatches; i++) {

                List<Mutation> mutations = new ArrayList<Mutation>(batchSize);
                while (mutations.size() < batchSize) {

                    if (currentColumnCount < wideRowCount) {
                        //same identity
                    } else {
                        currentBucket = getARandomBucket(prng);
                        identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);
                        currentColumnCount = 0;
                    }

                    mutations.add(new Mutation(identity, prng.nextLong(), new CommunicationCV("aParty", "bParty", prng.nextDouble())));
                    currentColumnCount++;
                }

                measures[i] = client.executeBatch(mutations);
            }

            long endTime = System.nanoTime();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getTotal(measures), numberOfBatches, measurements);
        } finally {
            client.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    private Mutation generateMutation(final Random prng, final IdentityBucketRK row, int column) {

        Long timestamp = new Long(column);
        CommunicationCV communication = new CommunicationCV("aPartyName", "bPartyName", prng.nextDouble());

        return new Mutation(row, timestamp, communication);
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
            Long[] measures = new Long[2];

            long measure1 = client.createKeyspace(replicationFactor);
            logger.debug("Created the keyspace {0} with replication factor {1}.", Constants.keyspaceName, replicationFactor);

            long measure2 = client.createTable();
            logger.debug("Created the table {0} in keyspace {1}.", Constants.tableNameCQL, Constants.keyspaceName);

            long endTime = System.nanoTime();

            measures[0] = measure1;
            measures[1] = measure2;

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, 2, SimpleMath.getTotal(measures), 2, measurements);
        } finally {
            client.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
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
}
