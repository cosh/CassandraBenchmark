package cassandra.benchmark.service.internal.scenario.astyanax;

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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static cassandra.benchmark.service.internal.helper.DataGenerator.createRandomIdentity;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getARandomBucket;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getNumberOfBatches;
import static cassandra.benchmark.service.internal.helper.ParameterParser.extractBatchSize;
import static cassandra.benchmark.service.internal.helper.ParameterParser.extractWideRowCount;
import static cassandra.benchmark.service.internal.helper.ParameterParser.extractnumberOfRowsCount;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertAsyncBenchmark extends AstyanaxBenchmark implements Scenario {

    private static Logger logger = LogManager.getLogger(BatchInsertAsyncBenchmark.class);

    private static Long numberOfRows = 10000L;
    private static Integer wideRowCount = 100;
    private static Integer batchSize = 100;

    private final List<Long> listOfAsyncBatchRequestDuration = new ArrayList<Long>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private static String name = "astyanaxBatchInsertAsync";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(CreationContext context) {

        return super.createDefaultDatamodel(context);
    }

    @Override
    public BenchmarkResult executeBenchmark(ExecutionContext context) {
        if(context == null) return null;

        exctractParameter(context);

        logger.debug(String.format("Executing astyanax async batch insert benchmark with the following parameters rowCount:%d, wideRowCount:&d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);

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

                final ListenableFuture<OperationResult<Void>> operationResultListenableFuture = executeBatch(mutations);
                if(operationResultListenableFuture != null)
                {
                    queries.add(operationResultListenableFuture);
                }
            }

            ListenableFuture<List<OperationResult<Void>>> successfulQueries = Futures.successfulAsList(queries);
            final List<OperationResult<Void>> operationResults = successfulQueries.get();
            //so the work is done and we can get the measures;

            long endTime = System.nanoTime();

            Long[] measures = new Long[this.listOfAsyncBatchRequestDuration.size()];
            this.listOfAsyncBatchRequestDuration.toArray(measures); // fill the array

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getTotal(measures), numberOfBatches, measurements);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    synchronized
    private void addBatchResult(final long duration) {
        this.listOfAsyncBatchRequestDuration.add(duration);
    }

    private ListenableFuture<OperationResult<Void>> executeBatch(final List<Mutation> mutations) {
        final long startTime = System.nanoTime();

        final MutationBatch batch = super.keyspace.prepareMutationBatch();

        for (Mutation aMutation : mutations)
        {
            batch.withRow(model, aMutation.getIdentity())
                    .putColumn(aMutation.getTimeStamp(), aMutation.getCommunication(), valueSerializer, 0);
        }

        try {
            final ListenableFuture<OperationResult<Void>> operationResultListenableFuture = batch.executeAsync();

            operationResultListenableFuture.addListener(new Runnable() {
                public void run() {

                    long endTime = System.nanoTime();

                    addBatchResult(endTime - startTime);
                }
            }, executorService);

        } catch (ConnectionException e) {
            logger.error("error inserting batch", e);
        }

        return null;
    }

    private void exctractParameter(final ExecutionContext context) {
        if(context.getParameter() == null) return;

        this.wideRowCount = extractWideRowCount(context.getParameter());
        this.numberOfRows = extractnumberOfRowsCount(context.getParameter());
        this.batchSize = extractBatchSize(context.getParameter());
    }

}