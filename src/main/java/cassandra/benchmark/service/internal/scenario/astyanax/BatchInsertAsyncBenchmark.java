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

package cassandra.benchmark.service.internal.scenario.astyanax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertAsyncBenchmark extends AstyanaxBenchmark implements Scenario {

    static Log logger = LogFactory.getLog(BatchInsertAsyncBenchmark.class.getName());

    private static String name = "astyanaxBatchInsertAsync";
    private final List<Long> listOfAsyncBatchRequestDuration = new ArrayList<Long>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private Integer wideRowCount = Constants.defaultColumnCount;
    private Long numberOfRows = Constants.defaultRowCount;
    private Integer batchSize = Constants.defaultBatchSize;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(CreationContext context) {

        super.checkContext(context);

        logger.info(String.format("Creating standard data-model with parameters: %s", context));

        return super.createDefaultDatamodel(context);
    }

    @Override
    public BenchmarkResult executeBenchmark(ExecutionContext context) {
        if (context == null) return null;

        super.checkContext(context);

        exctractParameter(context);

        logger.info(String.format("Executing astyanax async batch insert benchmark with the following parameters rowCount:%d, wideRowCount:%d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);
            final Keyspace keyspace = super.getKeyspace();

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final Random prng = new Random();

            int currentColumnCount = 0;

            Integer currentBucket = getARandomBucket(prng);

            IdentityBucketRK identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);

            List<ListenableFuture<OperationResult<Void>>> queries = new ArrayList<ListenableFuture<OperationResult<Void>>>();

            for (int i = 0; i < numberOfBatches; i++) {


                List<Mutation> mutations = new ArrayList<Mutation>(batchSize);
                while (mutations.size() < batchSize) {

                    if (currentColumnCount < wideRowCount) {
                        //same identity
                    } else {
                        currentBucket = getARandomBucket(prng);
                        identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);
                        currentColumnCount = 0;
                    }

                    mutations.add(new Mutation(identity, prng.nextLong(), new CommunicationCV(identity.getIdentity(), "some static bParty", 23.5)));
                    currentColumnCount++;
                }

                final ListenableFuture<OperationResult<Void>> operationResultListenableFuture = executeBatch(mutations, keyspace);
                if (operationResultListenableFuture != null) {
                    queries.add(operationResultListenableFuture);
                }

                if(i%100 == 0 && i!=0) {
                    logger.info(String.format("Started async batch %d/%d.", i + 100, numberOfBatches));
                }
            }

            logger.info(String.format("Finished with starting %d async batches. Waiting for a reply...", numberOfBatches));

            boolean done = false;

            while(!done)
            {
                Thread.sleep(1000);
                boolean internalDone = true;

                for (ListenableFuture<OperationResult<Void>> aFuture : queries)
                {
                    if(!aFuture.isDone())
                    {
                        logger.info("A future is not yet finished. Relooping...");
                        internalDone = false;
                        break;
                    }
                }

                if (internalDone)
                {
                    logger.info("All futures are finished.");
                    done = true;
                }
            }

            long endTime = System.nanoTime();

            Long[] measures = new Long[this.listOfAsyncBatchRequestDuration.size()];
            this.listOfAsyncBatchRequestDuration.toArray(measures); // fill the array

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getSum(measures), numberOfBatches, measurements);
        } catch (InterruptedException e) {
            log.error(e);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, null);
    }

    synchronized
    private void addBatchResult(final long duration) {
        this.listOfAsyncBatchRequestDuration.add(duration);
    }

    private ListenableFuture<OperationResult<Void>> executeBatch(final List<Mutation> mutations, final Keyspace keyspace) {
        final long startTime = System.nanoTime();

        final MutationBatch batch = keyspace
                .prepareMutationBatch()
                .withAtomicBatch(false)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE);

        for (Mutation aMutation : mutations) {
            batch.withRow(DefaultModel.model, aMutation.getIdentity())
                    .putColumn(aMutation.getTimeStamp(), aMutation.getCommunication(), DefaultModel.valueSerializer, 0);
        }

        try {
            final ListenableFuture<OperationResult<Void>> operationResultListenableFuture = batch.executeAsync();

            operationResultListenableFuture.addListener(new OneShotTask(startTime), executorService);

        } catch (ConnectionException e) {
            logger.error("error inserting batch", e);
        }

        return null;
    }

    class OneShotTask implements Runnable {
        private final long startTime;
        OneShotTask(final long s) { startTime = s; }
        public void run() {

            addBatchResult(System.nanoTime() - startTime);
        }
    }
}