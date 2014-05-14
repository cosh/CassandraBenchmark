package cassandra.benchmark.service.internal;

import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.transfer.BenchmarkResult;

import java.util.List;

/**
 * Created by cosh on 13.05.14.
 */
public interface CassandraClient {

    /**
     * Creats the keyspace
     * @param replicationFactor The replication factor
     * @return time to execute the request in ms
     */
    long createKeyspace(final int replicationFactor);

    /**
     * Creates the table/column-family
     * @return time to execute the request in ms
     */
    long createTable();

    /**
     * Initializes the client
     * @param seedNode The seed node
     * @param port The (thrift/cql-native) port
     * @param clusterName The cluster name
     */
    void initialize(final String seedNode, final int port, final String clusterName);

    /**
     * shutting down the client
     */
    void teardown();

    /**
     * Executes a mutation/statement
     * @param mutations The list of mutations
     * @return time to execute the request (batch of mutations) in ms
     */
    long executeBatch(final List<Mutation> mutations);
}
