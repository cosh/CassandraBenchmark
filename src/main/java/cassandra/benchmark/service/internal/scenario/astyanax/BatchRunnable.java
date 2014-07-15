package cassandra.benchmark.service.internal.scenario.astyanax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.PartialResult;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static cassandra.benchmark.service.internal.helper.DataGenerator.createRandomIdentity;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getARandomBucket;

/**
 * Created by cosh on 15.07.14.
 */
public class BatchRunnable implements Runnable {

    static Log logger = LogFactory.getLog(BatchRunnable.class.getName());

    private final int numberOfBatches;
    private final int batchSize;
    private final int wideRowCount;
    private final Keyspace keyspace;

    private final PartialResult result;

    public BatchRunnable(int numberOfBatches, int batchSize, int wideRowCount, Keyspace keyspace) {
        this.numberOfBatches = numberOfBatches;
        this.batchSize = batchSize;
        this.wideRowCount = wideRowCount;
        this.keyspace = keyspace;
        result = new PartialResult();
    }

    @Override
    public void run() {

        final Random prng = new Random();

        Integer currentBucket = getARandomBucket(prng);
        IdentityBucketRK identity = new IdentityBucketRK(createRandomIdentity(prng), currentBucket);

        int currentColumnCount = 0;

        for (int i = 0; i < numberOfBatches; i++) {

            List<Mutation> mutations = new ArrayList<Mutation>(batchSize);
            while (mutations.size() < batchSize) {

                if (currentColumnCount < wideRowCount) {
                    //same identity
                } else {
                    identity = new IdentityBucketRK(createRandomIdentity(prng), getARandomBucket(prng));
                    currentColumnCount = 0;
                }

                mutations.add(new Mutation(identity, prng.nextLong(), new CommunicationCV(identity.getIdentity(), "some static bParty", 23.5)));
                currentColumnCount++;
            }

            try {
                result.addMeasurement(executeBatch(mutations));
            } catch (ConnectionException e) {
                logger.error(e);

                if(result.getErrors().size() < Constants.errorThreshold) {
                    result.addErrors(e.getMessage());
                }
                else
                {
                    result.addErrors(e.getMessage());
                    logger.error("Skipping benchmark because of too many errors.");
                    break;
                }
            }

            if(i%100 == 0 && i!=0) {
                logger.info(String.format("Executed batch %d/%d.", i + 100, numberOfBatches));
            }
        }
    }

    private long executeBatch(final List<Mutation> mutations) throws ConnectionException {
        long startTime = System.nanoTime();

        MutationBatch batch = keyspace
                .prepareMutationBatch()
                .withAtomicBatch(false)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE);

        for (Mutation aMutation : mutations) {
            batch.withRow(DefaultModel.model, aMutation.getIdentity())
                    .putColumn(aMutation.getTimeStamp(), aMutation.getCommunication(), DefaultModel.valueSerializer, 0);
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
}
