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

import java.util.Random;

/**
 * Helper class for data generation
 *
 * Created by cosh on 02.06.14.
 */
public class DataGenerator {

    /**
     * Creates a random identity (some random digits)
     * @param prng A (pseudo) random number generator
     * @return An identity
     */
    public static String createRandomIdentity(final Random prng) {
        final int currentIdentityPartA = prng.nextInt(42424242);
        final int currentIdentityPartB = prng.nextInt(23232323);

        return String.format("%d-%d", currentIdentityPartA, currentIdentityPartB);
    }

    /**
     * Returns a random bucket (based on days per year)
     * @param prng A (pseudo) random number generator
     * @return a number from 0-364
     */
    public static Integer getARandomBucket(Random prng) {
        return new Integer(prng.nextInt(365));
    }

    /**
     * Calculates the number of batches
     * @param numberOfRows The number of rows
     * @param wideRowCount The number of columns per row
     * @param batchSize The desired batch size
     * @return The number of batches which is necessary to insert the rows/columns
     */
    public static int getNumberOfBatches(final long numberOfRows, final int wideRowCount, final int batchSize) {
        return (int) ((numberOfRows * wideRowCount) / batchSize);
    }
}
