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
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertAsyncBenchmark extends DatastaxBenchmark implements Scenario {

    static Log logger = LogFactory.getLog(BatchInsertAsyncBenchmark.class.getName());

    private static String name = "datastaxBatchInsertAsync";
    private final List<Long> listOfAsyncBatchRequestDuration = new ArrayList<Long>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(CreationContext context) {

        logger.info(String.format("Creating standard CQL data-model with parameters: %s", context));

        return super.createDataModel(context);
    }

    @Override
    public BenchmarkResult executeBenchmark(ExecutionContext context) {
        if (context == null) return null;

        exctractParameter(context);

        logger.info(String.format("Executing datastax async batch insert benchmark with the following parameters rowCount:%d, wideRowCount:%d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final Random prng = new Random();

            int currentColumnCount = 0;

            Integer currentBucket = getARandomBucket(prng);

            final com.datastax.driver.core.PreparedStatement preparedStatement = createPreparedStatement();

            IdentityBucketRK identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);

            List<ListenableFuture<ResultSet>> queries = new ArrayList<ListenableFuture<ResultSet>>();

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

                queries.add(executeBatch(preparedStatement, mutations));

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

                for (ListenableFuture<ResultSet> aFuture : queries)
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

            executorService.shutdownNow();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getSum(measures), numberOfBatches, measurements);
        } catch (InterruptedException e) {
            logger.error(e);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, null);
    }

    private ListenableFuture<ResultSet> executeBatch(final PreparedStatement preparedStatement, final List<Mutation> mutations) {
        final long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED);
        bs.setConsistencyLevel(ConsistencyLevel.ONE);

        for (Mutation aMutation : mutations) {
            BoundStatement statement = createInsertStatement(aMutation, preparedStatement);
            bs.add(statement);
        }

        ResultSetFuture resultSetFuture = super.session.executeAsync(bs);
        Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(final ResultSet result) {
                addBatchResult(System.nanoTime() - startTime);
            }

            @Override
            public void onFailure(final Throwable t) {
                // Could do better I suppose
                logger.error("Error during request: " + t);
            }
        }, executorService);

        return resultSetFuture;
    }

    synchronized
    private void addBatchResult(final long duration) {
        this.listOfAsyncBatchRequestDuration.add(duration);
        logger.info(String.format("Inserted one async batch in %d ms.", TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS)));
    }

    private BoundStatement createInsertStatement(final Mutation mutation, final com.datastax.driver.core.PreparedStatement preparedStatement) {

        return preparedStatement.bind(
                mutation.getIdentity().getIdentity(),
                mutation.getIdentity().getBucket(),
                mutation.getTimeStamp(),
                mutation.getCommunication().getaPartyImsi(),
                mutation.getCommunication().getaPartyImei(),
                mutation.getCommunication().getbParty(),
                mutation.getCommunication().getDuration());
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