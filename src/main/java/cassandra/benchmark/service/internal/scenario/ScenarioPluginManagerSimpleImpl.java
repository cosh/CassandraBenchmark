// Copyright (c) 2014 Henning Rauch
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
