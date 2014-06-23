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
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertBenchmark extends DatastaxBenchmark implements Scenario {

    private static Logger logger = LogManager.getLogger(BatchInsertBenchmark.class);

    private static Long numberOfRows = 10000L;
    private static Integer wideRowCount = 100;
    private static Integer batchSize = 100;

    private static String name = "datastaxBatchInsert";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(CreationContext context) {

        return super.createDataModel(context);
    }

    @Override
    public BenchmarkResult executeBenchmark(ExecutionContext context) {
        if (context == null) return null;

        exctractParameter(context);

        logger.debug(String.format("Executing datastax batch insert benchmark with the following parameters rowCount:%d, wideRowCount:&d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final Long[] measures = new Long[numberOfBatches];
            final Random prng = new Random();

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

                measures[i] = executeBatch(preparedStatement, mutations);
            }

            long endTime = System.nanoTime();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getSum(measures), numberOfBatches, measurements);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    private long executeBatch(final com.datastax.driver.core.PreparedStatement preparedStatement, final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement();

        for (Mutation aMutation : mutations) {
            BoundStatement statement = createInsertStatement(aMutation, preparedStatement);
            bs.add(statement);
        }

        super.session.execute(bs);

        return System.nanoTime() - startTime;
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

        this.wideRowCount = extractColumnCountPerRow(context.getParameter());
        this.numberOfRows = extractnumberOfRowsCount(context.getParameter());
        this.batchSize = extractBatchSize(context.getParameter());
    }
}