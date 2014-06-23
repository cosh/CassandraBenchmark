package cassandra.benchmark.service.internal.scenario;

import cassandra.benchmark.transfer.BenchmarkResult;

/**
 * Created by cosh on 02.06.14.
 */
public interface Scenario {
    /**
     * Returns the name of the scenario
     *
     * @return The name of the scenario
     */
    String getName();

    /**
     * Creates the datamodel
     *
     * @param context the context of this scenario
     * @return time to execute the request in ms
     */
    BenchmarkResult createDatamodel(final CreationContext context);

    /**
     * Executes a mutation/statement
     *
     * @param context the context of this scenario
     * @return time to execute the request (batch of mutations) in ms
     */
    BenchmarkResult executeBenchmark(final ExecutionContext context);
}
