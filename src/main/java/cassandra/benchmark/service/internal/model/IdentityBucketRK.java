package cassandra.benchmark.service.internal.model;

import com.netflix.astyanax.annotations.Component;

/**
 * Created by cosh on 13.05.14.
 */
public class IdentityBucketRK {

    @Component(ordinal = 0)
    String identity;

    @Component(ordinal = 1)
    private Integer bucket;

    public IdentityBucketRK(final String identity, final Integer bucket) {
        this.identity = identity;
        this.bucket = bucket;
    }


    public IdentityBucketRK() {

    }

    public String getIdentity() {
        return identity;
    }

    public Integer getBucket() {
        return bucket;
    }
}