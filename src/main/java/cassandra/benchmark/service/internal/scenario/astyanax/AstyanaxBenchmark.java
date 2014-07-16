// Copyright (c) 2014 Henning Rauch
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra.benchmark.service.internal.scenario.astyanax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public abstract class AstyanaxBenchmark {

    static Log log = LogFactory.getLog(AstyanaxBenchmark.class.getName());

    protected Integer wideRowCount = Constants.defaultColumnCount;
    protected Long numberOfRows = Constants.defaultRowCount;
    protected Integer batchSize = Constants.defaultBatchSize;

    /**
     * This object tracks the context of an astyanax instance of either a Cluster or Keyspace
     */
    protected AstyanaxContext<Keyspace> astyanaxContext;
    private int initConnectionsPerHost = 5;
    private int maxConnectionsPerHost = 10;
    /**
     * Major cassandra version compatibility
     */
    private String cassandraVersion = "1.2";
    private int connectTimeout = ConnectionPoolConfigurationImpl.DEFAULT_CONNECT_TIMEOUT;

    protected void initializeCluster(final ExecutionContext context, final String keyspaceName) {
        this.astyanaxContext = initializeContext(context, keyspaceName);
        if (this.astyanaxContext != null) {
            this.astyanaxContext.start();
        }
    }

    public void teardown() {
        if (astyanaxContext != null) astyanaxContext.shutdown();
    }

    protected void initializeForBenchMarkDefault(final ExecutionContext context) {
        initializeCluster(context, Constants.keyspaceName);
    }

    protected static void checkContext(final ExecutionContext context) {
        if (context.getPort() == 0) {
            context.setPort(Constants.defaultThriftPort);
            log.info(String.format("set thrift port to default (%d).", Constants.defaultThriftPort));
        }
    }

    private void createKeyspace(String keyspaceName, String simpleStrategy, int replicationFactor) {

        Keyspace ks = astyanaxContext.getClient();

        Properties props = new Properties();
        props.setProperty("name", keyspaceName);
        props.setProperty("strategy_class", simpleStrategy);
        props.setProperty("strategy_options.replication_factor", String.valueOf(replicationFactor));

        try {
            ks.createKeyspaceIfNotExists(props);
        } catch (ConnectionException e) {
            log.error(e);
        }

        log.info("Created keyspace " + keyspaceName);
    }

    private AstyanaxContext<Keyspace> initializeContext(final ExecutionContext context, final String keyspaceName) {

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
                        .setPort(context.getPort())
                        .setSocketTimeout(ConnectionPoolConfigurationImpl.DEFAULT_SOCKET_TIMEOUT)
                        .setInitConnsPerHost(initConnectionsPerHost) //--> might be the reason for the too many open files
                        .setConnectTimeout(connectTimeout)
                        .setMaxConnsPerHost(maxConnectionsPerHost)
                        .setSeeds(context.getSeedNode());

        //poolConfig.setLatencyScoreStrategy(scoreStrategy); // Enabled SMA. Omit this to use round robin with a token
        // range

        return new AstyanaxContext.Builder()
                .forCluster(context.getClusterName())
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setTargetCassandraVersion(cassandraVersion)
                )
                .withConnectionPoolConfiguration(poolConfig)
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    protected BenchmarkResult createDefaultDatamodel(CreationContext context) {
        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        List<String> errors = new ArrayList<String>();

        try {
            Long[] measures = new Long[2];

            initializeCluster(context, Constants.keyspaceName);
            createKeyspace(Constants.keyspaceName, "SimpleStrategy", context.getReplicatioFactor());
            Long measure1 = System.nanoTime() - startTime;
            log.info(String.format("Created the keyspace %s with replication factor %s.", Constants.keyspaceName, context.getReplicatioFactor()));

            try {


                astyanaxContext.getClient().createColumnFamily(DefaultModel.model, ImmutableMap.<String, Object>builder()
                        .put("default_validation_class", "CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.DoubleType)")
                        .put("key_validation_class", "CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.IntegerType)")
                        .put("comparator_type", "LongType")
                        .build());
            } catch (ConnectionException e) {
                log.error(e);
                errors.add(e.getMessage());
            }

            long measure2 = System.nanoTime() - startTime - measure1;
            log.info(String.format("Created the table %s in keyspace %s.", Constants.tableNameCQL, Constants.keyspaceName));

            long endTime = System.nanoTime();

            measures[0] = measure1;
            measures[1] = measure2;

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, 2, SimpleMath.getSum(measures), 2, measurements);
        } finally {
            teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, errors);
    }

    protected void exctractParameter(final ExecutionContext context) {
        if (context.getParameter() == null) return;

        final Integer extractedWideRowCount = extractColumnCountPerRow(context.getParameter());
        if (extractedWideRowCount != null) {
            this.wideRowCount = extractedWideRowCount;

        }

        final Long extractedNumberOfRows = extractnumberOfRowsCount(context.getParameter());
        if (extractedNumberOfRows != null) {
            this.numberOfRows = extractedNumberOfRows;
        }

        Integer extractedBatchSize = extractBatchSize(context.getParameter());
        if (extractedBatchSize != null) {
            this.batchSize = extractedBatchSize;
        }
    }

    public Keyspace getKeyspace() {
        return astyanaxContext.getClient();
    }
}