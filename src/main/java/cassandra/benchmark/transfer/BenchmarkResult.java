package cassandra.benchmark.transfer;

/**
 * Created by cosh on 12.05.14.
 */
public class BenchmarkResult {

    private final long startTime;
    private final long totalOps;
    private final long totalStatements;
    private final double ops_rate;
    private final double statements_rate;
    private final double meanlatency;
    private final double medianlatency;
    private final double ninetyFiveTh;
    private final double ninetyNineTh;
    private final double elapsed;

    public BenchmarkResult(long totalOps, long totalStatements, double ops_rate, double statements_rate, double meanlatency, double medianlatency, double ninetyFiveTh, double ninetyNineTh, double elapsed, long startTime) {
        this.totalOps = totalOps;
        this.totalStatements = totalStatements;
        this.ops_rate = ops_rate;
        this.statements_rate = statements_rate;
        this.meanlatency = meanlatency;
        this.medianlatency = medianlatency;
        this.ninetyFiveTh = ninetyFiveTh;
        this.ninetyNineTh = ninetyNineTh;
        this.elapsed = elapsed;
        this.startTime = startTime;
    }

    /**
     * The total number of requests to the database (batch-requests most of the time)
     *
     * @return The total number of operations
     */
    public long getTotalOps() {
        return totalOps;
    }

    /**
     * The number of operations (batch-requests) per second)
     *
     * @return Operations per second
     */
    public double getOps_rate() {
        return ops_rate;
    }

    /**
     * The number of statements per second
     *
     * @return Statements per second
     */
    public double getStatements_rate() {
        return statements_rate;
    }

    /**
     * The mean latency  of the requests
     *
     * @return Mean latency
     */
    public double getMeanlatency() {
        return meanlatency;
    }

    /**
     * The median latency of the requests
     *
     * @return The media latency
     */
    public double getMedianlatency() {
        return medianlatency;
    }

    /**
     * The 95th percentile of the request latency
     *
     * @return The 95th percentile
     */
    public double getNinetyFiveTh() {
        return ninetyFiveTh;
    }

    /**
     * The 99th percentile of the request latency
     *
     * @return The 99th percentile
     */
    public double getNinetyNineTh() {
        return ninetyNineTh;
    }

    /**
     * Elapsed time in ms
     *
     * @return Elapsed time in ms
     */
    public double getElapsed() {
        return elapsed;
    }

    /**
     * The total number of statements
     *
     * @return The total number of statements
     */
    public long getTotalStatements() {
        return totalStatements;
    }
}
