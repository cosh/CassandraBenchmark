package cassandra.benchmark.service.internal.scenario.astyanax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.helper.Transformer;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.service.internal.scenario.CreationContext;
import cassandra.benchmark.service.internal.scenario.ExecutionContext;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
        List<String> errors = new ArrayList<String>(Constants.errorThreshold);

        BenchmarkResult result = null;

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final List<Long> measures = new ArrayList<Long>(numberOfBatches);
            final Random prng = new Random();

            int currentColumnCount = 0;
            Long statementCounter = 0L;
            Integer batchCount = 0;

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

                try {
                    batchCount++;
                    measures.add(executeBatch(mutations));
                    statementCounter+=mutations.size();
                } catch (ConnectionException e) {
                    logger.error(e);

                    if(errors.size() < Constants.errorThreshold) {
                        errors.add(e.getMessage());
                    }
                    else
                    {
                        errors.add(e.getMessage());
                        logger.error("Skipping benchmark because of too many errors.");
                        break;
                    }
                }

                if(i%100 == 0 && i!=0) {
                    logger.info(String.format("Executed batch %d/%d.", i + 100, numberOfBatches));
                }
            }

            long endTime = System.nanoTime();

            final Long[] samples = Transformer.transform(measures);
            SampleOfLongs measurements = new SampleOfLongs(samples, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(samples), 0, 0, statementCounter, SimpleMath.getSum(samples), batchCount, measurements);

            result = new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime, errors);

        } finally {
            super.teardown();
        }

        return result;
    }

    private long executeBatch(final List<Mutation> mutations) throws ConnectionException {
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
            throw e;
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