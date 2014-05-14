package cassandra.benchmark.service;

import cassandra.benchmark.transfer.BenchmarkResult;

/**
 * Created by cosh on 12.05.14.
 */
public interface CassandraBenchmarkService {

    /**
     * Benchmark the cluster
     *
     * This method does NOT create a schema,
     *
     * @param clientEnum The client that should be used
     * @param seedNode The seed node
     * @param port The (thrift) port
     * @param clusterName The name of the cluster
     * @param numberOfRows The number of rows that should be created
     * @param wideRowCount The size of each wide row
     * @param batchSize The batch size
     * @return Timings
     */
    BenchmarkResult executeBenchmark(final CassandraClientType clientEnum, final String seedNode, final int port, final String clusterName, final long numberOfRows, final int wideRowCount, final int batchSize);

    /**
     * Creates the schema for the benchmark
     *
     * there are two different schema variations for Astyanax(thrift) or Datastax (CQL).
     *
     * @param clientEnum The client that should be used
     * @param seedNode The seed node
     * @param port The (thrift) port
     * @param clusterName The name of the cluster
     * @param replicationFactor The replication factor
     * @return Timings
     */
    BenchmarkResult createSchema(final CassandraClientType clientEnum, final String seedNode, final int port, final String clusterName, final int replicationFactor);
}
