package cassandra.benchmark.service.internal.Astyanax.model;

import com.netflix.astyanax.annotations.Component;

/**
 * Created by cosh on 13.05.14.
 */
public class IdentityBucketRK {

    @Component(ordinal = 0)
    String identity;

    @Component(ordinal = 1)
    private Long bucket;

    public IdentityBucketRK(final String identity, final Long bucket)
    {
        this.identity = identity;
        this.bucket = bucket;
    }


    public IdentityBucketRK()
    {

    }
}