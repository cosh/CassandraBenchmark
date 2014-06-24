/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cassandra.benchmark.controller;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.service.internal.scenario.ScenarioPluginManager;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * A simple REST controller that enables the user to execute certain benchmark scenarios
 */
@RestController
public class BenchmarkScenarioController {

    static Log log = LogFactory.getLog(BenchmarkScenarioController.class.getName());

    /**
     * used for finding the desired/available benchmark scenarios
     */
    private final ScenarioPluginManager scenarioPluginManager;

    @Autowired
    public BenchmarkScenarioController(final ScenarioPluginManager scenarioPluginManager) {
        this.scenarioPluginManager = scenarioPluginManager;
    }

    /**
     * Executes a benchmark scenario
     * @param context The context of the scenario
     * @return The benchmark result
     */
    @RequestMapping(value = "/scenario/execute", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<BenchmarkResult> executeScenario(
            final @RequestBody ExecutionContext context) {

        checkContext(context);

        log.info(String.format("Executing benchmark with name %s and parameters %s", context.getBenchmarkName(), context.toString()));

        Scenario scenario = scenarioPluginManager.tryGetScenario(context.getBenchmarkName());

        if (scenario != null) {
            return new ResponseEntity<BenchmarkResult>(scenario.executeBenchmark(context), HttpStatus.OK);
        }

        log.error(String.format("Benchmark with name %s not available.", context.getBenchmarkName()));
        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }

    /**
     * Creates the datamodel for a given benchmark scenario
     * @param context The context of the scenario
     * @return The benchmark result
     */
    @RequestMapping(value = "/scenario/createDatamodel", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<BenchmarkResult> createDatamodelForScenario(
            final @RequestBody CreationContext context) {

        checkContext(context);

        if(context.getReplicatioFactor() == 0 )
        {
            context.setReplicatioFactor(Constants.defaultReplicationFactor);
            log.info(String.format("Set replication factor to default(%d)", Constants.defaultReplicationFactor));
        }

        log.info(String.format("Executing benchmark with name %s and parameters %s", context.getBenchmarkName(), context.toString()));

        Scenario scenario = scenarioPluginManager.tryGetScenario(context.getBenchmarkName());

        if (scenario != null) {
            return new ResponseEntity<BenchmarkResult>(scenario.createDatamodel(context), HttpStatus.OK);
        }

        log.error(String.format("Benchmark with name %s not available.", context.getBenchmarkName()));
        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }

    /**
     * Checks the context and if certain properties are not set, the default values are used
     * @param context The execution context
     */
    private static void checkContext(final ExecutionContext context) {
        if(context.getSeedNode() == null || context.getSeedNode().isEmpty())
        {
            context.setSeedNode(Constants.defaultSeedNode);
            log.info(String.format("Set default seed node to default(%s)", Constants.defaultSeedNode));
        }

        if(context.getClusterName() == null || context.getClusterName().isEmpty())
        {
            context.setClusterName(Constants.defaultClusterName);
            log.info(String.format("Set default cluster name to default(%s)", Constants.defaultClusterName));
        }
    }
}
