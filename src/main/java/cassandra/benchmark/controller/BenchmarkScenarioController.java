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

import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.service.internal.scenario.ScenarioPluginManager;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tomcat.util.http.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.Consumes;

@RestController
public class BenchmarkScenarioController {

    private final ScenarioPluginManager scenarioPluginManager;
    private static Logger logger = LogManager.getLogger(BenchmarkScenarioController.class);


    @Autowired
    public BenchmarkScenarioController(final ScenarioPluginManager scenarioPluginManager) {
        this.scenarioPluginManager = scenarioPluginManager;
    }

    @RequestMapping(value = "/scenario/execute", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<BenchmarkResult> executeScenario(
            final @RequestBody ExecutionContext context)
    {
        Scenario scenario = null;

        logger.debug(String.format("Executing benchmark with name %s and parameters %s", context.getBenchmarkName(), context.toString()));

        if(scenarioPluginManager.tryGetScenario(scenario, context.getBenchmarkName()))
        {
            return new ResponseEntity<BenchmarkResult>(scenario.executeBenchmark(context), HttpStatus.OK);
        }

        logger.error(String.format("Benchmark with name %s not available.", context.getBenchmarkName()));
        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/scenario/createDatamodel", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<BenchmarkResult> createDatamodelForScenario(
            final @RequestBody CreationContext context)
    {
        Scenario scenario = null;

        logger.debug(String.format("Executing benchmark with name %s and parameters %s", context.getBenchmarkName(), context.toString()));

        if(scenarioPluginManager.tryGetScenario(scenario, context.getBenchmarkName()))
        {
            return new ResponseEntity<BenchmarkResult>(scenario.createDatamodel(context), HttpStatus.OK);
        }

        logger.error(String.format("Benchmark with name %s not available.", context.getBenchmarkName()));
        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }
}
