package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 02.06.14.
 */
public class CreationContext extends ExecutionContext {

    private int replicatioFactor;

    public int getReplicatioFactor() {
        return replicatioFactor;
    }

    public void setReplicatioFactor(int replicatioFactor) {
        this.replicatioFactor = replicatioFactor;
    }

    public CreationContext() {
    }

    @Override
    public String toString() {
        return "CreationContext{" +
                "replicatioFactor=" + replicatioFactor +
                "} " + super.toString();
    }
}
