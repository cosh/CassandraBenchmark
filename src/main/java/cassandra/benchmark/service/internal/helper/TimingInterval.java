package cassandra.benchmark.service.internal.helper;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

// represents measurements taken over an interval of time
// used for both single timer results and merged timer results
public final class TimingInterval {
    public final long maxLatency;
    public final long pauseLength;
    public final long pauseStart;
    public final long totalLatency;
    // discrete
    public final long keyCount;
    public final long operationCount;
    final SampleOfLongs sample;
    // nanos
    private final long start;
    private final long end;

    public TimingInterval(long time) {
        start = end = time;
        maxLatency = totalLatency = 0;
        keyCount = operationCount = 0;
        pauseStart = pauseLength = 0;
        sample = new SampleOfLongs(new Long[0], 1d);
    }

    public TimingInterval(long start, long end, long maxLatency, long pauseStart, long pauseLength, long keyCount, long totalLatency, long operationCount, SampleOfLongs sample) {
        this.start = start;
        this.end = Math.max(end, start);
        this.maxLatency = maxLatency;
        this.keyCount = keyCount;
        this.totalLatency = totalLatency;
        this.operationCount = operationCount;
        this.pauseStart = pauseStart;
        this.pauseLength = pauseLength;
        this.sample = sample;
    }

    // merge multiple timer intervals together
    static TimingInterval merge(Random rnd, List<TimingInterval> intervals, int maxSamples, long start) {
        int operationCount = 0, keyCount = 0;
        long maxLatency = 0, totalLatency = 0;
        List<SampleOfLongs> latencies = new ArrayList<SampleOfLongs>();
        long end = 0;
        long pauseStart = 0, pauseEnd = Long.MAX_VALUE;
        for (TimingInterval interval : intervals) {
            end = Math.max(end, interval.end);
            operationCount += interval.operationCount;
            maxLatency = Math.max(interval.maxLatency, maxLatency);
            totalLatency += interval.totalLatency;
            keyCount += interval.keyCount;
            latencies.addAll(Arrays.asList(interval.sample));
            if (interval.pauseLength > 0) {
                pauseStart = Math.max(pauseStart, interval.pauseStart);
                pauseEnd = Math.min(pauseEnd, interval.pauseStart + interval.pauseLength);
            }
        }
        if (pauseEnd < pauseStart)
            pauseEnd = pauseStart = 0;
        return new TimingInterval(start, end, maxLatency, pauseStart, pauseEnd - pauseStart, keyCount, totalLatency, operationCount,
                SampleOfLongs.merge(rnd, latencies, maxSamples));

    }

    public double realOpRate() {
        return operationCount / ((end - start) * 0.000000001d);
    }

    public double adjustedOpRate() {
        return operationCount / ((end - (start + pauseLength)) * 0.000000001d);
    }

    public double keyRate() {
        return keyCount / ((end - start) * 0.000000001d);
    }

    public double meanLatency() {
        return (totalLatency / (double) operationCount) * 0.000001d;
    }

    public double maxLatency() {
        return maxLatency * 0.000001d;
    }

    public long runTime() {
        return (end - start) / 1000000;
    }

    public double medianLatency() {
        return sample.medianLatency();
    }

    // 0 < rank < 1
    public double rankLatency(float rank) {
        return sample.rankLatency(rank);
    }

    public final long endNanos() {
        return end;
    }

    public final long endMillis() {
        return end / 1000000;
    }

    public long startNanos() {
        return start;
    }
}

