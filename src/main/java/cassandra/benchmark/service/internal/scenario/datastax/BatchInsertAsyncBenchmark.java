package cassandra.benchmark.service.internal.scenario.datastax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.helper.SampleOfLongs;
import cassandra.benchmark.service.internal.helper.SimpleMath;
import cassandra.benchmark.service.internal.helper.TimingInterval;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import cassandra.benchmark.service.internal.model.Mutation;
import cassandra.benchmark.service.internal.scenario.Scenario;
import cassandra.benchmark.service.internal.scenario.ScenarioContext;
import cassandra.benchmark.transfer.BenchmarkResult;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

import static cassandra.benchmark.service.internal.helper.DataGenerator.*;
import static cassandra.benchmark.service.internal.helper.ParameterParser.*;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertAsyncBenchmark extends DatastaxBenchmark implements Scenario {

    private static Logger logger = LogManager.getLogger(BatchInsertAsyncBenchmark.class);

    private static Long numberOfRows = 10000L;
    private static Integer wideRowCount = 100;
    private static Integer batchSize = 100;

    private final List<Long> listOfAsyncBatchRequestDuration = new ArrayList<Long>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private static String name = "datastaxBatchInsertAsync";

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BenchmarkResult createDatamodel(ScenarioContext context, int replicationFactor) {

        return  super.createDataModel(context, replicationFactor);
    }

    @Override
    public BenchmarkResult executeBenchmark(ScenarioContext context) {
        if(context == null) return null;

        exctractParameter(context);

        logger.debug(String.format("Executing datastax async batch insert benchmark with the following parameters rowCount:%d, wideRowCount:&d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

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
            ListenableFuture<List<ResultSet>> successfulQueries = Futures.successfulAsList(queries);


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
            }

            final List<ResultSet> resultSets = successfulQueries.get();
            //so the work is done and we can get the measures;

            long endTime = System.nanoTime();

            Long[] measures = new Long[this.listOfAsyncBatchRequestDuration.size()];
            this.listOfAsyncBatchRequestDuration.toArray(measures); // fill the array


            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getTotal(measures), numberOfBatches, measurements);
        } catch (InterruptedException e) {
            logger.error(e);
        } catch (ExecutionException e) {
            logger.error(e);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    private ListenableFuture<ResultSet> executeBatch(final PreparedStatement preparedStatement, final List<Mutation> mutations) {
        final long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement();

        for (Mutation aMutation : mutations)
        {
            BoundStatement statement = createInsertStatement(aMutation, preparedStatement);
            bs.add(statement);
        }

        final ResultSetFuture resultSetFuture = super.session.executeAsync(bs);

        resultSetFuture.addListener(new Runnable() {
            public void run() {

                long endTime = System.nanoTime();

                addBatchResult(endTime - startTime);
            }
        }, executorService);

        return resultSetFuture;
    }

    synchronized
    private void addBatchResult(final long duration) {
        this.listOfAsyncBatchRequestDuration.add(duration);
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

    private com.datastax.driver.core.PreparedStatement createPreparedStatement()
    {
        return  session.prepare("INSERT INTO " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
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

    private void exctractParameter(final ScenarioContext context) {
        if(context.getParameter() == null) return;

        this.wideRowCount = extractWideRowCount(context.getParameter());
        this.numberOfRows = extractnumberOfRowsCount(context.getParameter());
        this.batchSize = extractBatchSize(context.getParameter());
    }
}