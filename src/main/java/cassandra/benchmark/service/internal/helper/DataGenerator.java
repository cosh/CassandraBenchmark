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
