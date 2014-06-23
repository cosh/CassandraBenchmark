package cassandra.benchmark.service.internal.helper;

import java.util.Arrays;

/**
 * A helper class for simple math
 *
 * Created by cosh on 02.06.14.
 */
public class SimpleMath {

    /**
     * Get the total sum of an array of Longs
     * @param array The array of longs
     * @return The sum
     */
    public static long getSum(Long[] array) {
        if(array == null) {
            return 0;
        }

        if (array.length == 0) {
            return 0;
        }

        long totalLatency = 0L;

        for (int i = 0; i < array.length; i++) {
            totalLatency += array[i];
        }

        return totalLatency;
    }

    /**
     * Calculates the maximum of an array of longs
     * @param array The array of longs
     * @return The maximum or 0 if it's null or empty
     */
    public static long getMax(final Long[] array) {
        if(array == null) {
            return 0;
        }

        if (array.length == 0) {
            return 0;
        }

        long max = 0;

        for (int i = 0; i < array.length; i++) {
            if (array[i] > max) {
                max = array[i];
            }
        }

        return max;
    }
}
