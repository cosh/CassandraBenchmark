package cassandra.benchmark.service.internal.Datastax;

import cassandra.benchmark.service.internal.CassandraClient;
import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.model.Mutation;
import com.datastax.driver.core.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created by cosh on 13.05.14.
 */
public class CassandraClientDatastaxImpl implements CassandraClient {

    private static Logger logger = LogManager.getLogger("CassandraClientDatastaxImpl");

    private Cluster cluster;
    private Session session;

    @Override
    public long createKeyspace(int replicationFactor) {

        return executeStatement(session,
                "CREATE KEYSPACE IF NOT EXISTS " + Constants.keyspaceName + " WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");
    }

    @Override
    public long createTable() {
        return executeStatement(session,
                "CREATE TABLE IF NOT EXISTS " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
                        "identity text," +
                        "timeBucket int," +
                        "time bigint," +
                        "aparty text," +
                        "bparty text," +
                        "duration float," +
                        "primary key((identity, timeBucket), time)" +
                        ");"
        );
    }

    @Override
    public void initialize(final String seedNode, final int port, final String clusterName) {
        cluster = connect(seedNode, port, clusterName);
        session = cluster.connect();
    }

    @Override
    public void teardown() {
        session.close();
        cluster.close();
    }

    @Override
    public long executeBatch(final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement();

        for (Mutation aMutation : mutations)
        {
            SimpleStatement statement = createInsertStatement(aMutation);
            bs.add(statement);
        }

        session.execute(bs);

        return System.nanoTime() - startTime;
    }

    private SimpleStatement createInsertStatement(Mutation mutation) {

        String insertString = "INSERT INTO " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
                "identity," +
                "timeBucket," +
                "time," +
                "aparty," +
                "bparty," +
                "duration ) " +
                "VALUES (" +
                "'" + mutation.getIdentity().getIdentity()+ "'" + ","+
                mutation.getIdentity().getBucket() + ","+
                mutation.getTimeStamp() + ","+
                "'" + mutation.getCommunication().getAparty()+ "'" + ","+
                "'" + mutation.getCommunication().getBparty()+ "'" + ","+
                mutation.getCommunication().getDuration()+
                ");";

        return new SimpleStatement(insertString);
    }

    public Cluster connect(final String node, final int port,  final String clusterName) {
        final Cluster cluster = Cluster.builder()
                .addContactPoint(node)
                .withClusterName(clusterName)
                .build();
        final Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: %s\n",
                metadata.getClusterName());
        return cluster;
    }

    private long executeStatement(Session session, String statement) {
        long startTime = System.nanoTime();
        session.execute(statement);
        return System.nanoTime() - startTime;
    }
}
