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

package cassandra.benchmark.service.internal.scenario.datastax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.*;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertBenchmark extends DatastaxBenchmark implements Scenario {

    static Log logger = LogFactory.getLog(BatchInsertBenchmark.class.getName());

    private static String name = "datastaxBatchInsert";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(CreationContext context) {

        super.checkContext(context);

        logger.info(String.format("Creating standard CQL data-model with parameters: %s", context));

        return super.createDataModel(context);
    }

    @Override
    public BenchmarkResult executeBenchmark(ExecutionContext context) {
        if (context == null) return null;

        super.checkContext(context);

        exctractParameter(context);

        logger.info(String.format("Executing datastax batch insert benchmark with the following parameters rowCount:%d, wideRowCount:%d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        List<String> errors = new ArrayList<String>(Constants.errorThreshold);

        int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
        if(numberOfBatches == 0)
        {
            errors.add("Number of batches is 0");
            return new BenchmarkResult(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, startTime, errors);
        }

        TimingInterval ti;
        BenchmarkResult result = null;

        try {

            super.initializeForBenchMarkDefault(context);

            int counter = 0;

            int cores = Runtime.getRuntime().availableProcessors();
            int fixedThreadSize = cores * 2;
            int parallelFutureCount = (fixedThreadSize * 3) / 2;
            logger.info(String.format("Cores %d, threadPoolSize %d, parallelFutureCount %d", cores, fixedThreadSize, parallelFutureCount));

            final ExecutorService executor = Executors.newFixedThreadPool(fixedThreadSize);
            final com.datastax.driver.core.PreparedStatement preparedStatement = createPreparedStatement();

            final List<Long> measures = new ArrayList<Long>(numberOfBatches);

            boolean done = false;

            while (!done)
            {
                List<FutureTask<PartialResult>> futureTasks = new ArrayList<FutureTask<PartialResult>>(parallelFutureCount);

                for (int i = 0; i < parallelFutureCount; i++) {

                    int localBatchCount = numberOfBatches < Constants.batchesPerThread  ? numberOfBatches : Constants.batchesPerThread;
                    numberOfBatches -= localBatchCount;

                    if(localBatchCount != 0)
                    {
                        final BatchRunnable runnable = new BatchRunnable(localBatchCount, batchSize, wideRowCount, super.session, preparedStatement);

                        FutureTask<PartialResult> task = new FutureTask<PartialResult>(runnable);
                        executor.execute(task);

                        futureTasks.add(task);
                    }
                    else
                    {
                        done = true;
                    }

                }

                for (FutureTask<PartialResult> aFuture : futureTasks) {

                    counter++;

                    final PartialResult partialResult = aFuture.get();

                    measures.addAll(partialResult.getMeasures());
                    errors.addAll(partialResult.getErrors());

                    if (counter % 100 == 0 && counter != 0) {
                        logger.info(String.format("Execution %d", counter));
                    }

                    if (errors.size() > Constants.errorThreshold) {
                        {
                            logger.error("Skipping benchmark because of too many errors.");
                            break;
                        }
                    }
                }
            }

            logger.info(String.format("batch execs %d, future execs %d", measures.size(), counter));

            long endTime = System.nanoTime();

            final Long[] samples = Transformer.transform(measures);
            SampleOfLongs measurements = new SampleOfLongs(samples, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(samples), 0, 0, measures.size() * batchSize, SimpleMath.getSum(samples), measures.size(), measurements);

            result = new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, errors);


        } catch (InterruptedException e) {
            logger.error(e);
        } catch (ExecutionException e) {
            logger.error(e);
        } finally {
            super.teardown();
        }

        return result;
    }

    private com.datastax.driver.core.PreparedStatement createPreparedStatement() {
        return session.prepare("INSERT INTO " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
                "identity," +
                "timeBucket," +
                "time," +
                "aPartyImsi," +
                "aPartyImei," +
                "bparty," +
                "duration ) " +
                "VALUES (" +
                "?,?,?,?,?,?,?);");
    }
}