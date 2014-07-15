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
import cassandra.benchmark.service.internal.helper.Transformer;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertBenchmark extends DatastaxBenchmark implements Scenario {

    static Log logger = LogFactory.getLog(BatchInsertBenchmark.class.getName());

    private static String name = "datastaxBatchInsert";
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
        TimingInterval ti = new TimingInterval(startTime);
        List<String> errors = new ArrayList<String>(Constants.errorThreshold);

        BenchmarkResult result = null;

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final List<Long> measures = new ArrayList<Long>(numberOfBatches);
            final Random prng = new Random();

            Long statementCounter = 0L;
            Integer batchCount = 0;

            int currentColumnCount = 0;

            Integer currentBucket = getARandomBucket(prng);

            final com.datastax.driver.core.PreparedStatement preparedStatement = createPreparedStatement();

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
                    measures.add(executeBatch(preparedStatement, mutations));
                    statementCounter+=mutations.size();
                }
                catch (Exception e)
                {
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

    private long executeBatch(final com.datastax.driver.core.PreparedStatement preparedStatement, final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED);

        bs.setConsistencyLevel(ConsistencyLevel.ONE);

        for (Mutation aMutation : mutations) {
            BoundStatement statement = createInsertStatement(aMutation, preparedStatement);
            bs.add(statement);
        }

        super.session.execute(bs);

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