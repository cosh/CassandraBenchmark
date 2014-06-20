package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 02.06.14.
 */
public class ExecutionContext {

    private final String seedNode;
    private final int port;
    private final String clusterName;
    private final Map<String, String> parameter;

    public ExecutionContext(final String seedNode, final int port, Map<String, String> parameter, final String clusterName) {
        this.seedNode = seedNode;
        this.port = port;
        this.parameter = parameter;
        this.clusterName = clusterName;
    }

    public String getSeedNode() {
        return seedNode;
    }

    public int getPort() {
        return port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, String> getParameter() {
        return parameter;
    }
}
