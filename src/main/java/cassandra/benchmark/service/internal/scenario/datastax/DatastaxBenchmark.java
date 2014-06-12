package cassandra.benchmark.service.internal.scenario.datastax;

import cassandra.benchmark.service.internal.scenario.ScenarioContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by cosh on 02.06.14.
 */
public abstract class DatastaxBenchmark {
    private static Logger logger = LogManager.getLogger(DatastaxBenchmark.class);


    public void initializeForBenchMarkDefault(final ScenarioContext context)
    {

    }

    public void teardown() {

    }
}