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


import java.util.Arrays;
import java.util.List;
import java.util.Random;

// represents a sample of long (latencies) together with the probability of selection of each sample (i.e. the ratio of
// samples to total number of events). This is used to ensure that, when merging, the result has samples from each
// with equal probability
public final class SampleOfLongs {

    // nanos
    final Long[] sample;

    // probability with which each sample was selected
    final double p;

    public SampleOfLongs(Long[] sample, int p) {
        this.sample = sample;
        this.p = 1 / (float) p;
    }

    SampleOfLongs(Long[] sample, double p) {
        this.sample = sample;
        this.p = p;
    }

    static SampleOfLongs merge(Random rnd, List<SampleOfLongs> merge, int maxSamples) {
        int maxLength = 0;
        double targetp = 1;
        for (SampleOfLongs sampleOfLongs : merge) {
            maxLength += sampleOfLongs.sample.length;
            targetp = Math.min(targetp, sampleOfLongs.p);
        }
        Long[] sample = new Long[maxLength];
        int count = 0;
        for (SampleOfLongs latencies : merge) {
            Long[] in = latencies.sample;
            double p = targetp / latencies.p;
            for (int i = 0; i < in.length; i++)
                if (rnd.nextDouble() < p)
                    sample[count++] = in[i];
        }
        if (count > maxSamples) {
            targetp = subsample(rnd, maxSamples, sample, count, targetp);
            count = maxSamples;
        }
        sample = Arrays.copyOf(sample, count);
        Arrays.sort(sample);
        return new SampleOfLongs(sample, targetp);
    }

    private static double subsample(Random rnd, int maxSamples, Long[] sample, int count, double p) {
        // want exactly maxSamples, so select random indexes up to maxSamples
        for (int i = 0; i < maxSamples; i++) {
            int take = i + rnd.nextInt(count - i);
            long tmp = sample[i];
            sample[i] = sample[take];
            sample[take] = tmp;
        }

        // calculate new p; have selected with probability maxSamples / count
        // so multiply p by this probability
        p *= maxSamples / (double) sample.length;
        return p;
    }

    public SampleOfLongs subsample(Random rnd, int maxSamples) {
        if (maxSamples > sample.length)
            return this;

        Long[] sample = this.sample.clone();
        double p = subsample(rnd, maxSamples, sample, sample.length, this.p);
        sample = Arrays.copyOf(sample, maxSamples);
        return new SampleOfLongs(sample, p);
    }

    public double medianLatency() {
        if (sample.length == 0)
            return 0;
        return sample[sample.length >> 1] * 0.000001d;
    }

    // 0 < rank < 1
    public double rankLatency(float rank) {
        if (sample.length == 0)
            return 0;
        int index = (int) (rank * sample.length);
        if (index >= sample.length)
            index = sample.length - 1;
        return sample[index] * 0.000001d;
    }

}

