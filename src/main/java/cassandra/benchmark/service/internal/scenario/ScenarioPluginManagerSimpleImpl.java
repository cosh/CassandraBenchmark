package cassandra.benchmark.service.internal.scenario;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by cosh on 20.06.14.
 */
public class ScenarioPluginManagerSimpleImpl implements ScenarioPluginManager {
    @Override
    public Map<String, Scenario> getAllAvailableScenarios() {
        Map<String, Scenario> result = new HashMap<String, Scenario>();

        cassandra.benchmark.service.internal.scenario.astyanax.BatchInsertAsyncBenchmark batchInsertAsyncBenchmarkAstyanax = new cassandra.benchmark.service.internal.scenario.astyanax.BatchInsertAsyncBenchmark();
        cassandra.benchmark.service.internal.scenario.datastax.BatchInsertAsyncBenchmark batchInsertAsyncBenchmarkDatastax = new cassandra.benchmark.service.internal.scenario.datastax.BatchInsertAsyncBenchmark();

        cassandra.benchmark.service.internal.scenario.astyanax.BatchInsertBenchmark batchInsertBenchmarkAstyanax = new cassandra.benchmark.service.internal.scenario.astyanax.BatchInsertBenchmark();
        cassandra.benchmark.service.internal.scenario.datastax.BatchInsertBenchmark batchInsertBenchmarkDatastax = new cassandra.benchmark.service.internal.scenario.datastax.BatchInsertBenchmark();

        result.put(batchInsertAsyncBenchmarkAstyanax.getName().toLowerCase(), batchInsertAsyncBenchmarkAstyanax);
        result.put(batchInsertAsyncBenchmarkDatastax.getName().toLowerCase(), batchInsertAsyncBenchmarkDatastax);
        result.put(batchInsertBenchmarkAstyanax.getName().toLowerCase(), batchInsertBenchmarkAstyanax);
        result.put(batchInsertBenchmarkDatastax.getName().toLowerCase(), batchInsertBenchmarkDatastax);

        return result;
    }

    @Override
    public Scenario tryGetScenario(final String name) {

        if (name == null) {
            return null;
        }

        final Map<String, Scenario> allAvailableScenarios = getAllAvailableScenarios();

        if (allAvailableScenarios.containsKey(name.toLowerCase())) {
            return allAvailableScenarios.get(name.toLowerCase());
        }

        return null;
    }
}
