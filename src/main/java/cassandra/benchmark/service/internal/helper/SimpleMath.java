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
