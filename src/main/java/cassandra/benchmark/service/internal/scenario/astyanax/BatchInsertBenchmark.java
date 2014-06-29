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
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertBenchmark extends AstyanaxBenchmark implements Scenario {

    static Log logger = LogFactory.getLog(BatchInsertBenchmark.class.getName());

    private static String name = "astyanaxBatchInsert";

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

        logger.info(String.format("Executing astyanax batch insert benchmark with the following parameters rowCount:%d, wideRowCount:%d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final Long[] measures = new Long[numberOfBatches];
            final Random prng = new Random();

            int currentColumnCount = 0;

            Integer currentBucket = getARandomBucket(prng);

            IdentityBucketRK identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);

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

                measures[i] = executeBatch(mutations);

                if(i%100 == 0 && i!=0) {
                    logger.info(String.format("Executed batch %d/%d.", i + 100, numberOfBatches));
                }
            }

            long endTime = System.nanoTime();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getSum(measures), numberOfBatches, measurements);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    private long executeBatch(final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        MutationBatch batch = super.keyspace
                .prepareMutationBatch()
                .withAtomicBatch(false)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE);

        for (Mutation aMutation : mutations) {
            batch.withRow(model, aMutation.getIdentity())
                    .putColumn(aMutation.getTimeStamp(), aMutation.getCommunication(), valueSerializer, 0);
        }

        try {
            batch.execute();
        } catch (ConnectionException e) {
            logger.error("error inserting batch", e);
        }

        long timeSpan = (System.nanoTime() - startTime);

        return timeSpan;
    }

    private void exctractParameter(final ExecutionContext context) {
        if (context.getParameter() == null) return;

        final Integer extractedWideRowCount = extractColumnCountPerRow(context.getParameter());
        if(extractedWideRowCount != null)
        {
            this.wideRowCount = extractedWideRowCount;

        }

        final Long extractedNumberOfRows = extractnumberOfRowsCount(context.getParameter());
        if(extractedNumberOfRows != null)
        {
            this.numberOfRows = extractedNumberOfRows;
        }

        Integer extractedBatchSize = extractBatchSize(context.getParameter());
        if(extractedBatchSize != null)
        {
            this.batchSize = extractedBatchSize;
        }
    }
}