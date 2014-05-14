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

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class BenchmarkController {

    private final CassandraBenchmarkService service;

    @Autowired
    public BenchmarkController(final CassandraBenchmarkService service) {
        this.service = service;
    }

	@RequestMapping
    public String letsSeeIfItsAlive()
    {
        return "This service is working fine";
    }


    /**
     * Benchmark the cluster
     *
     * This method does NOT create a schema,
     *
     * @param client The client that should be used
     * @param seedNode The seed node
     * @param port The (thrift) port
     * @param clusterName The name of the cluster
     * @param numberOfRows The number of rows that should be created
     * @param wideRowCount The size of each wide row
     * @param batchSize The batch size
     * @return Timings
     */
    @RequestMapping(value = "/client/benchmark", method = RequestMethod.GET)
    public BenchmarkResult testAllNodes(
            final @RequestParam(required = false, defaultValue = "astyanax") String client,
            final @RequestParam(required = false, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = false, defaultValue = "9160") int port,
            final @RequestParam(required = false, defaultValue = "Test Cluster") String clusterName,
            final @RequestParam(required = false, defaultValue = "100000") long numberOfRows,
            final @RequestParam(required = false, defaultValue = "20") int wideRowCount,
            final @RequestParam(required = false, defaultValue = "1000") int batchSize)
    {
        CassandraClientType clientType = getClientType(client);

        return service.executeBenchmark(clientType, seedNode, port, clusterName, numberOfRows, wideRowCount, batchSize);
    }

    /**
     * Benchmark the cluster
     *
     * This method does NOT create a schema,
     *
     * @param seedNode The seed node
     * @param port The (thrift) port
     * @param clusterName The name of the cluster
     * @param numberOfRows The number of rows that should be created
     * @param wideRowCount The size of each wide row
     * @param batchSize The batch size
     * @return Timings
     */
    @RequestMapping(value = "/benchmark", method = RequestMethod.GET)
    public Map<String, BenchmarkResult> testAllNodes(
            final @RequestParam(required = false, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = false, defaultValue = "9160") int port,
            final @RequestParam(required = false, defaultValue = "Test Cluster") String clusterName,
            final @RequestParam(required = false, defaultValue = "100000") long numberOfRows,
            final @RequestParam(required = false, defaultValue = "20") int wideRowCount,
            final @RequestParam(required = false, defaultValue = "1000") int batchSize) throws InterruptedException {
        Map<String, BenchmarkResult> result = new HashMap<String, BenchmarkResult>(4);

        result.put("createSchemaAstyanax", service.createSchema(CassandraClientType.ASTYANAX, seedNode, port, clusterName, 3));
        Thread.sleep(2000);
        result.put("createSchemaDatastax", service.createSchema(CassandraClientType.DATASTAX, seedNode, port, clusterName, 3));
        Thread.sleep(2000);

        result.put("benchMarkAstyanax", service.executeBenchmark(CassandraClientType.ASTYANAX, seedNode, port, clusterName, numberOfRows, wideRowCount, batchSize));
        result.put("benchMarkDatastax", service.executeBenchmark(CassandraClientType.DATASTAX, seedNode, port, clusterName, numberOfRows, wideRowCount, batchSize));

        return result;
    }

    /**
     * Creates the schema for the benchmark
     *
     * there are two different schema variations for Astyanax(thrift) or Datastax (CQL).
     *
     * @param client The client that should be used
     * @param seedNode The seed node
     * @param port The (thrift) port
     * @param clusterName The name of the cluster
     * @param rf The replication factor
     * @return Timings
     */
    @RequestMapping(value = "/schema/create", method = RequestMethod.PUT)
    public BenchmarkResult createSchema(
            final @RequestParam(required = false, defaultValue = "astyanax") String client,
            final @RequestParam(required = false, defaultValue = "127.0.0.1") String seedNode,
            final @RequestParam(required = false, defaultValue = "9160") int port,
            final @RequestParam(required = false, defaultValue = "Test Cluster") String clusterName,
            final @RequestParam(required = false, defaultValue = "3") int rf)
    {
        CassandraClientType clientType = getClientType(client);

        return service.createSchema(clientType, seedNode, port, clusterName, rf);
    }

    /**
     * Define the client type corresponding to the client type string
     * @param client
     * @return
     */
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
