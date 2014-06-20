package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 02.06.14.
 */
public class CreationContext extends ExecutionContext {

    private final int replicatioFactor;

    public CreationContext(final String seedNode, final int port, Map<String, String> parameter, final String clusterName, final int replicatioFactor) {
        super(seedNode, port, parameter, clusterName);
        this.replicatioFactor = replicatioFactor;
    }

    public int getReplicatioFactor() {
        return replicatioFactor;
    }
}
