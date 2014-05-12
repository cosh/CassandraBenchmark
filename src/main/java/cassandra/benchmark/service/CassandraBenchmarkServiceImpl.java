package cassandra.benchmark.service;

import cassandra.benchmark.transfer.BenchmarkResult;

/**
 * Created by cosh on 12.05.14.
 */
public class CassandraBenchmarkServiceImpl implements CassandraBenchmarkService {

    @Override
    public BenchmarkResult executeBenchmark(String seedNode, long numberOfRequests, int batchSize) {
        return new BenchmarkResult(1, 2.3, 4.5, 6.7, 8.9, 9.10, 11.12);
    }
}
