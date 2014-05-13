package cassandra.benchmark.service.internal.Astyanax;

import cassandra.benchmark.service.internal.CassandraClient;
import cassandra.benchmark.service.internal.Constants;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by cosh on 13.05.14.
 */
public class CassandraClientAstyanaxImpl implements CassandraClient{
    private static Logger logger = LogManager.getLogger("CassandraClientAstyanaxImpl");

    /**
     * cluster object
     */
    protected Cluster cluster;

    /**
     * keyspace within cluster
     */
    protected Keyspace keyspace;

    /**
     * This object tracks the context of an astyanax instance of either a Cluster or Keyspace
     */
    protected AstyanaxContext<Cluster> astyanaxContext;

    private int initConnectionsPerHost = 10;
    private int maxConnectionsPerHost = 3;
    /**
     * Major cassandra version compatibility
     */
    private String cassandraVersion = "1.2";
    private int port = 9160;
    private int connectTimeout = ConnectionPoolConfigurationImpl.DEFAULT_CONNECT_TIMEOUT;

    @Override
    public long createKeyspace(int replicationFactor) {
        long startTime = System.nanoTime();
        this.keyspace = getOrCreateKeyspace(this, Constants.keyspaceName, "SimpleStrategy", replicationFactor);
        return System.nanoTime() - startTime;
    }

    @Override
    public long createTable() {
        long startTime = System.nanoTime();

        com.netflix.astyanax.model.ColumnFamily<String, String> model =
                new com.netflix.astyanax.model.ColumnFamily<String, String>(
                        Constants.tableName,
                        new StringSerializer(),
                        new StringSerializer());

        try {
            keyspace.createColumnFamily(model, ImmutableMap.<String, Object>builder()
                    .put("default_validation_class", "UTF8Type")
                    .put("key_validation_class", "UTF8Type")
                    .put("comparator_type", "CompositeType(UTF8Type, LongType)")
                    .build());
        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        return System.nanoTime() - startTime;
    }

    @Override
    public void initialize(String seedNode, String clusterName) {
        this.astyanaxContext = initializeContext(seedNode, clusterName);
        this.astyanaxContext.start();
        this.cluster = astyanaxContext.getClient();

    }

    @Override
    public void teardown() {
        astyanaxContext.shutdown();
        cluster = null;
        keyspace = null;
    }

    private Keyspace getOrCreateKeyspace(CassandraClientAstyanaxImpl cassandraClientAstyanax, String keyspaceName, String simpleStrategy, int replicationFactor) {
        if (!keySpaceExists(keyspaceName))
        {
            try {
                createKeyspace_private(keyspaceName, simpleStrategy, replicationFactor);
            } catch (Exception e) {
                logger.error("Could not create keyspace {}. Will try again.", keyspaceName, e);

                return null;
            }
        }

        try {
            return cluster.getKeyspace(keyspaceName);
        } catch (ConnectionException e) {
            logger.error("Could not get keyspace {}. Will try again.", keyspaceName, e);
            return null;
        }
    }

    private void createKeyspace_private(String keyspaceName, String simpleStrategy, int replicationFactor) throws Exception {
        final KeyspaceDefinition keyspaceDefinition = cluster.makeKeyspaceDefinition()
                .setName(keyspaceName)
                .setStrategyClass(simpleStrategy)
                .addStrategyOption("replication_factor", String.valueOf(replicationFactor));

        int retries = 10;

        try {
            cluster.addKeyspace(keyspaceDefinition);
        } catch (ConnectionException e) {
            logger.error("Could not add keyspace.", e);
            logger.warn(String.format("Starting max %d retries.", retries));
            //retry
            Boolean retry = false;

            for (int i = 0; i < retries; i++) {
                logger.info(String.format("Starting retry %d of %d.", i + 1, retries));
                try {
                    cluster.addKeyspace(keyspaceDefinition);
                } catch (final ConnectionException retryException) {
                    logger.error("Could not add keyspace.", retryException);
                    retry = true;
                }

                if (!retry) {
                    logger.info(String.format("Cassandra cluster was available in try %d of %d.", i + 1, retries));
                    return;
                }
            }

            throw new Exception(String.format("Could not create keyspace %s after %d retries.", keyspaceName, retries));
        }

        logger.info("Created keyspace " + keyspaceName);
    }

    private boolean keySpaceExists(String keyspaceName) {
        boolean keyspaceExists = false;

        try {
            for (KeyspaceDefinition keyspaceDefinition : cluster.describeKeyspaces()) {
                if (keyspaceDefinition.getName().equals(keyspaceName)) {
                    keyspaceExists = true;
                    break;
                }
            }
        } catch (final ConnectionException e) {
            //return true;
            throw new IllegalStateException("Keyspace could not be created", e);
        }

        return keyspaceExists;
    }

    private AstyanaxContext<Cluster> initializeContext(String seeds, String clusterName) {

        /* Will resort hosts per token partition every 10 seconds */
        final int updateInterval = 10000;

        /* Will clear the latency every 10 seconds */
        final int resetInterval = 10000;

        /* Uses last 100 latency samples */
        final int windowSize = 100;

        /*
         * Will sort hosts if a host is more than 100% slower than the best and always assign connections to the fastest
         * host, otherwise will use round robin
         */
        final double badnessThreshold = 0.50;

        final SmaLatencyScoreStrategyImpl scoreStrategy = new SmaLatencyScoreStrategyImpl(
                updateInterval,
                resetInterval,
                windowSize,
                badnessThreshold
        );

        final ConnectionPoolConfigurationImpl poolConfig =
                new ConnectionPoolConfigurationImpl("CassandraConnectionPool")
                        .setPort(port)
                        .setSocketTimeout(ConnectionPoolConfigurationImpl.DEFAULT_SOCKET_TIMEOUT)
                        .setInitConnsPerHost(initConnectionsPerHost) //--> might be the reason for the too many open files
                        .setConnectTimeout(connectTimeout)
                        .setMaxConnsPerHost(maxConnectionsPerHost)
                        .setSeeds(seeds);

        poolConfig.setLatencyScoreStrategy(scoreStrategy); // Enabled SMA. Omit this to use round robin with a token
        // range

        return new AstyanaxContext.Builder()
                .forCluster(clusterName)
                .forKeyspace(Constants.keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                                .setTargetCassandraVersion(cassandraVersion)
                )
                .withConnectionPoolConfiguration(poolConfig)
                .buildCluster(ThriftFamilyFactory.getInstance());
    }
}
