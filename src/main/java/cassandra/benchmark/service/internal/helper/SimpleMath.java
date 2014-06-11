package cassandra.benchmark.service.internal.helper;

import java.util.Arrays;

/**
 * Created by cosh on 02.06.14.
 */
public class SimpleMath {
    public static long getTotal(long[] samples) {
        if (samples.length == 0) {
            return 0;
        }

        Arrays.sort(samples);

        return samples[samples.length - 1];
    }

    public static long getMax(long[] samples) {
        if (samples.length == 0) {
            return 0;
        }

        long max = 0;

        for (int i = 0; i < samples.length; i++) {
            if (samples[i] > max) {
                max = samples[i];
            }
        }

        return max;
    }
}
