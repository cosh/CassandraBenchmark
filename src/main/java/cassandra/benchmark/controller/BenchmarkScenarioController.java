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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class BenchmarkScenarioController {

    private final ScenarioPluginManager scenarioPluginManager;

    @Autowired
    public BenchmarkScenarioController(final ScenarioPluginManager scenarioPluginManager) {
        this.scenarioPluginManager = scenarioPluginManager;
    }

	@RequestMapping
    public String letsSeeIfItsAlive()
    {
        return "This service is working fine";
    }

    @RequestMapping(value = "/scenario/{scenarioName}/execute", method = RequestMethod.POST)
    public ResponseEntity<BenchmarkResult> executeScenario(
            final @PathVariable String scenarioName,
            final @RequestBody ExecutionContext context)
    {
        Scenario scenario = null;

        if(scenarioPluginManager.tryGetScenario(scenario, scenarioName))
        {
            return new ResponseEntity<BenchmarkResult>(scenario.executeBenchmark(context), HttpStatus.OK);
        }

        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/scenario/{scenarioName}/createDatamodel", method = RequestMethod.POST)
    public ResponseEntity<BenchmarkResult> createDatamodelForScenario(
            final @PathVariable String scenarioName,
            final @RequestBody CreationContext context)
    {
        Scenario scenario = null;

        if(scenarioPluginManager.tryGetScenario(scenario, scenarioName))
        {
            return new ResponseEntity<BenchmarkResult>(scenario.createDatamodel(context), HttpStatus.OK);
        }

        return new ResponseEntity<BenchmarkResult>(HttpStatus.NOT_FOUND);
    }
}
