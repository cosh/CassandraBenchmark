package cassandra.benchmark.service;

import cassandra.benchmark.transfer.BenchmarkResult;

/**
 * Created by cosh on 12.05.14.
 */
public interface CassandraBenchmarkService {
    BenchmarkResult executeBenchmark(final CassandraClientType clientEnum, final String seedNode, final String clusterName, final long numberOfRows, final int wideRowCount, final int batchSize);

    BenchmarkResult createSchema(final CassandraClientType clientEnum, final String seedNode, final String clusterName, final int replicationFactor);
}
