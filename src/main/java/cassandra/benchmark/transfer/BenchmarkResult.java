package cassandra.benchmark.transfer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cosh on 12.05.14.
 */
public class BenchmarkResult {

    private final long startTime;
    private final long totalCassandraClientCalls;
    private final long totalStatements;
    private final double clientCallsPerSecond;
    private final double statementsPerSecond;
    private final double meanlatency_clientCall;
    private final double medianlatency_clientCall;
    private final double ninetyFiveTh_clientCall;
    private final double ninetyNineTh_clientCall;
    private final double elapsed_ms;
    private String benchmarkHostName;
    private final List<String> errors;
    private Map<String, String> additionalInformation;


    public BenchmarkResult(long totalCassandraClientCalls, long totalStatements, double clientCallsPerSecond, double statementsPerSecond, double meanlatency_clientCall, double medianlatency_clientCall, double ninetyFiveTh_clientCall, double ninetyNineTh_clientCall, double elapsed_ms, long startTime, List<String> errors) {
        this.totalCassandraClientCalls = totalCassandraClientCalls;
        this.totalStatements = totalStatements;
        this.clientCallsPerSecond = clientCallsPerSecond;
        this.statementsPerSecond = statementsPerSecond;
        this.meanlatency_clientCall = meanlatency_clientCall;
        this.medianlatency_clientCall = medianlatency_clientCall;
        this.ninetyFiveTh_clientCall = ninetyFiveTh_clientCall;
        this.ninetyNineTh_clientCall = ninetyNineTh_clientCall;
        this.elapsed_ms = elapsed_ms;
        this.startTime = startTime;
        this.errors = errors;
        additionalInformation = null;
        benchmarkHostName = null;
    }

    /**
     * The total number of requests to the database (batch-requests most of the time)
     *
     * @return The total number of operations
     */
    public long getTotalCassandraClientCalls() {
        return totalCassandraClientCalls;
    }

    /**
     * The number of operations (batch-requests) per second)
     *
     * @return Operations per second
     */
    public double getClientCallsPerSecond() {
        return clientCallsPerSecond;
    }

    /**
     * The number of statements per second
     *
     * @return Statements per second
     */
    public double getStatementsPerSecond() {
        return statementsPerSecond;
    }

    /**
     * The mean latency  of the requests
     *
     * @return Mean latency
     */
    public double getMeanlatency_clientCall() {
        return meanlatency_clientCall;
    }

    /**
     * The median latency of the requests
     *
     * @return The media latency
     */
    public double getMedianlatency_clientCall() {
        return medianlatency_clientCall;
    }

    /**
     * The 95th percentile of the request latency
     *
     * @return The 95th percentile
     */
    public double getNinetyFiveTh_clientCall() {
        return ninetyFiveTh_clientCall;
    }

    /**
     * The 99th percentile of the request latency
     *
     * @return The 99th percentile
     */
    public double getNinetyNineTh_clientCall() {
        return ninetyNineTh_clientCall;
    }

    /**
     * Elapsed time in ms
     *
     * @return Elapsed time in ms
     */
    public double getElapsed_ms() {
        return elapsed_ms;
    }

    /**
     * The total number of statements
     *
     * @return The total number of statements
     */
    public long getTotalStatements() {
        return totalStatements;
    }

    /**
     * The start time as timestamp
     * @return The start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * List of errors
     * @return Errors
     */
    public List<String> getErrors() {
        return errors;
    }

    /**
     * Additional information about the result of the benchmark
     * @return A map of K/V pairs
     */
    public Map<String, String> getAdditionalInformation() {
        return additionalInformation;
    }

    public void addAdditionalInformation(final String key, final String value)
    {
        if(this.additionalInformation == null) additionalInformation = new HashMap<String, String>();

        this.additionalInformation.put(key, value);
    }

    public void setHostName(final String hostName) {
        this.benchmarkHostName = hostName;
    }

    public String getBenchmarkHostName() {
        return benchmarkHostName;
    }
}
