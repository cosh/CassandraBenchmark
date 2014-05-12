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

import cassandra.benchmark.service.CassandraBenchmarkService;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class BenchmarkController {

    private final CassandraBenchmarkService service;

    @Autowired
    public BenchmarkController(final CassandraBenchmarkService service) {
        this.service = service;
    }

	@RequestMapping
    public String testOnAllNodes()
    {
        return "this is a test";
    }


    @RequestMapping(value = "/all", method = RequestMethod.GET)
    public BenchmarkResult testAllNodes(
            final @RequestParam(required = true, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = true, defaultValue = "100000") long numberOfRequests,
            final @RequestParam(required = false, defaultValue = "1000") int batchSize)
    {
        return service.executeBenchmark(seedNode, numberOfRequests, batchSize);
    }
}
