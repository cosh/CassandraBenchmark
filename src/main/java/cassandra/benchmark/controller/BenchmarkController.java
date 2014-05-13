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
import cassandra.benchmark.service.CassandraClientType;
import cassandra.benchmark.transfer.BenchmarkResult;
import org.springframework.beans.factory.annotation.Autowired;
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
        return "This service is working fine";
    }


    @RequestMapping(value = "/all", method = RequestMethod.GET)
    public BenchmarkResult testAllNodes(
            final @RequestParam(required = false, defaultValue = "datastax") String client,
            final @RequestParam(required = false, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = false, defaultValue = "Test Cluster") String clusterName,
            final @RequestParam(required = false, defaultValue = "100000") long numberOfRows,
            final @RequestParam(required = false, defaultValue = "20") int wideRowCount,
            final @RequestParam(required = false, defaultValue = "1000") int batchSize)
    {
        CassandraClientType clientType = getClientType(client);

        return service.executeBenchmark(clientType, seedNode, clusterName, numberOfRows, wideRowCount, batchSize);
    }

    @RequestMapping(value = "/schema/create", method = RequestMethod.PUT)
    public BenchmarkResult createSchema(
            final @RequestParam(required = false, defaultValue = "datastax") String client,
            final @RequestParam(required = false, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = false, defaultValue = "Test Cluster") String clusterName,
            final @RequestParam(required = false, defaultValue = "3") int rf)
    {
        CassandraClientType clientType = getClientType(client);

        return service.createSchema(clientType, seedNode, clusterName, rf);
    }

    private CassandraClientType getClientType(String client) {
        CassandraClientType clientType;

        String clientTypeString = client.toLowerCase();
        if(clientTypeString.equals("datastax"))
        {
            clientType = CassandraClientType.DATASTAX;
        }
        else
        {
            if(clientTypeString.equals("astyanax"))
            {
                clientType = CassandraClientType.ASTYANAX;
            }
            else
            {
                clientType = CassandraClientType.DATASTAX;
            }
        }

        return clientType;
    }
}
