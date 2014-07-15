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

import java.net.InetAddress;
import java.net.UnknownHostException;

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
            final BenchmarkResult result = scenario.executeBenchmark(context);

            result.setHostName(getHostName());

            return new ResponseEntity<BenchmarkResult>(result, HttpStatus.OK);
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
            final BenchmarkResult result = scenario.createDatamodel(context);
            result.setHostName(getHostName());

            return new ResponseEntity<BenchmarkResult>(result, HttpStatus.OK);
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

    private static String getHostName()
    {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error(e);
        }

        return "Unknown";
    }
}
