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
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static cassandra.benchmark.service.internal.helper.DataGenerator.createRandomIdentity;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getARandomBucket;
import static cassandra.benchmark.service.internal.helper.DataGenerator.getNumberOfBatches;

/**
 * Created by cosh on 02.06.14.
 */
public class BatchInsertBenchmark extends DatastaxBenchmark implements Scenario {

    private static Logger logger = LogManager.getLogger(BatchInsertBenchmark.class);
    private static Long defaultInsertCount = 10000L;

    private static Long numberOfRows = 10000L;
    private static Integer wideRowCount = 100;
    private static Integer batchSize = 100;

    @Override
    public BenchmarkResult createDatamodel(ScenarioContext context, int replicationFactor) {

        return  super.createDataModel(context, replicationFactor);
    }

    @Override
    public BenchmarkResult executeBenchmark(ScenarioContext context) {
        if(context == null) return null;

        exctractParameter(context);

        logger.debug(String.format("Executing datastax batch insert benchmark with the following parameters rowCount:%d, wideRowCount:&d, batchSize:%d", numberOfRows, wideRowCount, batchSize));

        long startTime = System.nanoTime();
        TimingInterval ti = new TimingInterval(startTime);

        try {

            super.initializeForBenchMarkDefault(context);

            final int numberOfBatches = getNumberOfBatches(this.numberOfRows, this.wideRowCount, this.batchSize);
            final long[] measures = new long[numberOfBatches];
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
            }

            long endTime = System.nanoTime();

            SampleOfLongs measurements = new SampleOfLongs(measures, 1);

            ti = new TimingInterval(startTime, endTime, SimpleMath.getMax(measures), 0, 0, numberOfRows * wideRowCount, SimpleMath.getTotal(measures), numberOfBatches, measurements);
        } finally {
            super.teardown();
        }

        return new BenchmarkResult(ti.operationCount, ti.keyCount, ti.realOpRate(), ti.keyRate(), ti.meanLatency(), ti.medianLatency(), ti.rankLatency(0.95f), ti.rankLatency(0.99f), ti.runTime(), startTime);
    }

    private long executeBatch(final List<Mutation> mutations) {
        long startTime = System.nanoTime();

        BatchStatement bs = new BatchStatement();

        for (Mutation aMutation : mutations)
        {
            SimpleStatement statement = createInsertStatement(aMutation);
            bs.add(statement);
        }

        super.session.execute(bs);

        return System.nanoTime() - startTime;
    }

    private SimpleStatement createInsertStatement(Mutation mutation) {

        String insertString = "INSERT INTO " + Constants.keyspaceName + " ." + Constants.tableNameCQL + " (" +
                "identity," +
                "timeBucket," +
                "time," +
                "aPartyImsi," +
                "aPartyImei," +
                "bparty," +
                "duration ) " +
                "VALUES (" +
                "'" + mutation.getIdentity().getIdentity()+ "'" + ","+
                mutation.getIdentity().getBucket() + ","+
                mutation.getTimeStamp() + ","+
                "'" + mutation.getCommunication().getaPartyImsi()+ "'" + ","+
                "'" + mutation.getCommunication().getaPartyImei()+ "'" + ","+
                "'" + mutation.getCommunication().getbParty()+ "'" + ","+
                mutation.getCommunication().getDuration()+
                ");";

        return new SimpleStatement(insertString);
    }

    private static void exctractParameter(final ScenarioContext context) {
        if(context.getParameter() == null) return;

        extractWideRowCount(context.getParameter());
        extractnumberOfRowsCount(context.getParameter());
        extractBatchSize(context.getParameter());
    }

    private static void extractBatchSize(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, "batchSize");
        if(extractedParameterString != null);
        {
            batchSize = Integer.parseInt((extractedParameterString));
        }
    }

    private static String extractParameterString(Map<String, String> parameters, String interestingElement) {
        if(interestingElement != null)
        {
            if(parameters.containsKey(interestingElement))
            {
                return parameters.get(interestingElement);
            }
        }

        return null;
    }

    private static void extractnumberOfRowsCount(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, "rowCount");
        if(extractedParameterString != null);
        {
            numberOfRows = Long.parseLong((extractedParameterString));
        }
    }

    private static void extractWideRowCount(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, "wideRowCount");
        if(extractedParameterString != null);
        {
            wideRowCount = Integer.parseInt((extractedParameterString));
        }
    }
}