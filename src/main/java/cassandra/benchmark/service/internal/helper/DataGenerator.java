package cassandra.benchmark.service.internal.helper;

import java.util.Random;

/**
 * Created by cosh on 02.06.14.
 */
public class DataGenerator {
    public static String createRandomIdentity(final Random prng) {
        final int currentIdentityPartA = prng.nextInt(42424242);
        final int currentIdentityPartB = prng.nextInt(23232323);

        return String.format("%d-%d", currentIdentityPartA, currentIdentityPartB);
    }

    public static Integer getARandomBucket(Random prng) {
        return new Integer(prng.nextInt(365));
    }

    public static int getNumberOfBatches(final long numberOfRows, final int wideRowCount, final int batchSize) {
        return (int) ((numberOfRows * wideRowCount) / batchSize);
    }
}
