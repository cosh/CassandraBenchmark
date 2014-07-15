package cassandra.benchmark.service.internal;

/**
 * Created by cosh on 13.05.14.
 */
public class Constants {

    public static final String keyspaceName = "cassandrabenchmark";
    public static final String tableNameCQL = "cqltable";
    public static final String tableNameThrift = "thrifttable";
    public static String rowCountString = "rowCount";
    public static String columnsPerRowCount = "wideRowCount";
    public static String batchSizeString = "batchSize";
    public static String defaultSeedNode = "127.0.0.1";
    public static Integer defaultCQLPort = 9042;
    public static Integer defaultThriftPort = 9160;
    public static String defaultClusterName = "Test Cluster";
    public static int defaultReplicationFactor = 3;
    public static Integer defaultBatchSize = 100;
    public static Long defaultRowCount = 10000L;
    public static Integer defaultColumnCount = 100;
    public static Integer errorThreshold = 10;
    public static Integer batchesPerThread = 100;
}
