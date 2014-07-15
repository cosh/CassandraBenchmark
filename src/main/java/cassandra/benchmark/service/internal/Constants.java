// Copyright (c) 2014 Henning Rauch
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
