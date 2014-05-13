package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.transfer.BenchmarkResult;

import java.util.List;

/**
 * Created by cosh on 13.05.14.
 */
public interface CassandraClient {

    long createKeyspace(final int replicationFactor);

    long createTable();

    void initialize(final String seedNode, final String clusterName);

    void teardown();

    long executeBatch(final List<Mutation> mutation);
}
