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
import cassandra.benchmark.service.internal.helper.PartialResult;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import com.datastax.driver.core.*;
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
    private final Session session;
    private final PreparedStatement preparedStatement;

    public BatchRunnable(int numberOfBatches, int batchSize, int wideRowCount, final Session session, final com.datastax.driver.core.PreparedStatement preparedStatement) {
        this.numberOfBatches = numberOfBatches;
        this.batchSize = batchSize;
        this.wideRowCount = wideRowCount;
        this.session = session;
        this.preparedStatement = preparedStatement;
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
            } catch (Exception e) {
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

    private long executeBatch(final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED);

        bs.setConsistencyLevel(ConsistencyLevel.ONE);

        for (Mutation aMutation : mutations) {
            BoundStatement statement = createInsertStatement(aMutation, preparedStatement);
            bs.add(statement);
        }

        this.session.execute(bs);

        long timeSpan = (System.nanoTime() - startTime);

        return timeSpan;
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
}
