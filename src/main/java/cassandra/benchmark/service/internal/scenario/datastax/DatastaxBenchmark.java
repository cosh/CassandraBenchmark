package cassandra.benchmark.service.internal.scenario.datastax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cosh on 02.06.14.
 */
public abstract class DatastaxBenchmark {
    static Log logger = LogFactory.getLog(DatastaxBenchmark.class.getName());

    protected Cluster cluster;
    protected Session session;

    protected static Cluster connect(final String node, final int port, final String clusterName) {
        final Cluster cluster = Cluster.builder()
                .addContactPoints(node.split(","))
                .withPort(port)
                .withClusterName(clusterName)
                //.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy()) //uses the DC of the seed node it connects to!! So one needs to give it the right seed
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                 .build();
        final Metadata metadata = cluster.getMetadata();
        logger.info(String.format("Connected to cluster: %s\n",
                metadata.getClusterName()));
        return cluster;
    }

    private static long executeStatement(final Session session, final String statement) {
        long startTime = System.nanoTime();
        session.execute(statement);
        return System.nanoTime() - startTime;
    }

    protected void initializeForBenchMarkDefault(final ExecutionContext context) {
        String seedNode = context.getSeedNode();
        Integer port = context.getPort();
        String clusterName = context.getClusterName();

        if(seedNode == null || seedNode.isEmpty())
        {
            seedNode = Constants.defaultSeedNode;
        }

        if(port.equals(0))
        {
            port = Constants.defaultCQLPort;
        }

        logger.info(String.format("connecting to seed-node: %s, port: %s and clusterName: %s", seedNode, port, clusterName));

        cluster = connect(seedNode, port, clusterName);
        session = cluster.connect();
    }

    protected static void checkContext(final ExecutionContext context) {
        if(context.getPort() == 0)
        {
            context.setPort(Constants.defaultCQLPort);
            logger.info(String.format("set cql port to default (%d).", Constants.defaultCQLPort));
        }

    }

    protected void teardown() {
        if(session != null) session.close();
        if(cluster != null) cluster.close();
    }

    protected BenchmarkResult createDataModel(final CreationContext context) {

        initializeForBenchMarkDefault(context);

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {
            Long[] measures = new Long[2];

            executeStatement(session,
                    "CREATE KEYSPACE IF NOT EXISTS " + Constants.keyspaceName + " WITH replication " +
                            "= {'class':'SimpleStrategy', 'replication_factor':" + context.getReplicatioFactor() + "};"
            );

            long measure1 = System.nanoTime() - startTime;
            logger.info(String.format("Created the keyspace %s with replication factor %d.", Constants.keyspaceName, context.getReplicatioFactor()));

            executeStatement(session,
                    "CREATE TABLE IF NOT EXISTS " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
                            "identity text," +
                            "timeBucket int," +
                            "time bigint," +
                            "aPartyImsi text," +
                            "aPartyImei text," +
                            "bparty text," +
                            "duration double," +
                            "primary key((identity, timeBucket), time)" +
                            ");"
            );

            long measure2 = System.nanoTime() - startTime - measure1;
            logger.info(String.format("Created the table %s in keyspace %s.", Constants.tableNameCQL, Constants.keyspaceName));

            long endTime = System.nanoTime();

            measures[0] = measure1;
            measures[1] = measure2;

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, 2, SimpleMath.getSum(measures), 2, measurements);
        } finally {
            teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, null);
    }
}