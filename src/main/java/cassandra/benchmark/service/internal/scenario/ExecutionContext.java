package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 02.06.14.
 */
public class ExecutionContext {

    private String seedNode;
    private int port;
    private String clusterName;
    private Map<String, String> parameter;
    private String benchmarkName;

    public ExecutionContext() {
    }

    public String getSeedNode() {
        return seedNode;
    }

    public void setSeedNode(String seedNode) {
        this.seedNode = seedNode;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Map<String, String> getParameter() {
        return parameter;
    }

    public void setParameter(Map<String, String> parameter) {
        this.parameter = parameter;
    }

    public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    @Override
    public String toString() {
        return "ExecutionContext{" +
                "seedNode='" + seedNode + '\'' +
                ", port=" + port +
                ", clusterName='" + clusterName + '\'' +
                ", parameter=" + parameter +
                ", benchmarkName='" + benchmarkName + '\'' +
                '}';
    }
}
