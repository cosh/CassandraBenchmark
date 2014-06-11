package cassandra.benchmark.service.internal.scenario;

import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.transfer.BenchmarkResult;

import java.util.List;

/**
 * Created by cosh on 02.06.14.
 */
public interface Scenario {
    /**
     * Creates the datamodel
     * @param context the context of this scenario
     * @param replicationFactor The replication factor
     * @return time to execute the request in ms
     */
    BenchmarkResult createDatamodel(final ScenarioContext context, final int replicationFactor);

    /**
     * Executes a mutation/statement
     * @param context the context of this scenario
     * @return time to execute the request (batch of mutations) in ms
     */
    BenchmarkResult executeBenchmark(final ScenarioContext context);
}
