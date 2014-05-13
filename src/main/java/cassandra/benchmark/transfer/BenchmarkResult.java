package cassandra.benchmark.transfer;

/**
 * Created by cosh on 12.05.14.
 */
public class BenchmarkResult {

    private final long total;
    private final double interval_op_rate;
    private final double interval_key_rate;
    private final double meanlatency;
    private final double medianlatency;
    private final double ninetyFiveTh;
    private final double ninetyNineTh;
    private final double elapsed;

    public BenchmarkResult(long total, double interval_op_rate, double interval_key_rate, double meanlatency, double medianlatency, double ninetyFiveTh, double ninetyNineTh, double elapsed) {
        this.total = total;
        this.interval_op_rate = interval_op_rate;
        this.interval_key_rate = interval_key_rate;
        this.meanlatency = meanlatency;
        this.medianlatency = medianlatency;
        this.ninetyFiveTh = ninetyFiveTh;
        this.ninetyNineTh = ninetyNineTh;
        this.elapsed = elapsed;
    }

    public long getTotal() {
        return total;
    }

    public double getInterval_op_rate() {
        return interval_op_rate;
    }

    public double getInterval_key_rate() {
        return interval_key_rate;
    }

    public double getMeanlatency() {
        return meanlatency;
    }
    public double getMedianlatency() {
        return medianlatency;
    }

    public double getNinetyFiveTh() {
        return ninetyFiveTh;
    }

    public double getNinetyNineTh() {
        return ninetyNineTh;
    }

    public double getElapsed() {
        return elapsed;
    }
}
