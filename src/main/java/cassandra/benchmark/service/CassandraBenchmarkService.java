package cassandra.benchmark.service;

import cassandra.benchmark.transfer.BenchmarkResult;

/**
 * Created by cosh on 12.05.14.
 */
public interface CassandraBenchmarkService {
    BenchmarkResult executeBenchmark(String seedNode, long numberOfRequests, int batchSize);
}
