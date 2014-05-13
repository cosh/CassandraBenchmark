package cassandra.benchmark.service.internal.Astyanax;

import cassandra.benchmark.service.internal.CassandraClient;

/**
 * Created by cosh on 13.05.14.
 */
public class CassandraClientAstyanaxImpl implements CassandraClient{
    @Override
    public long createKeyspace(int replicationFactor) {
        return 0;
    }

    @Override
    public long createTable() {
        return 0;
    }

    @Override
    public void initialize(String seedNode, String clusterName) {

    }

    @Override
    public void teardown() {

    }
}
