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
import java.util.concurrent.Callable;

import static cassandra.benchmark.service.internal.helper.DataGenerator.createRandomIdentity;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getARandomBucket;

/**
 * Created by cosh on 15.07.14.
 */
public class BatchRunnable implements Callable<PartialResult> {

    static Log logger = LogFactory.getLog(BatchRunnable.class.getName());

    private final int numberOfBatches;
    private final int batchSize;
    private final int wideRowCount;
    private final Keyspace keyspace;

    public BatchRunnable(int numberOfBatches, int batchSize, int wideRowCount, Keyspace keyspace) {
        this.numberOfBatches = numberOfBatches;
        this.batchSize = batchSize;
        this.wideRowCount = wideRowCount;
        this.keyspace = keyspace;
    }

    @Override
    public PartialResult call() {

        final PartialResult result = new PartialResult();

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
        }

        return result;
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
