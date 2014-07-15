package cassandra.benchmark.service.internal.scenario.astyanax;

import cassandra.benchmark.service.internal.Constants;
import cassandra.benchmark.service.internal.model.CommunicationCV;
import cassandra.benchmark.service.internal.model.IdentityBucketRK;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;

/**
 * Created by cosh on 15.07.14.
 */
public class DefaultModel {

    public final static AnnotatedCompositeSerializer<IdentityBucketRK> identityBucketSerializer = new AnnotatedCompositeSerializer<IdentityBucketRK>(
            IdentityBucketRK.class);
    public static final com.netflix.astyanax.model.ColumnFamily<IdentityBucketRK, Long> model =
            new com.netflix.astyanax.model.ColumnFamily<IdentityBucketRK, Long>(
                    Constants.tableNameThrift,
                    identityBucketSerializer,
                    new LongSerializer());
    public final static AnnotatedCompositeSerializer<CommunicationCV> valueSerializer = new AnnotatedCompositeSerializer<CommunicationCV>(
            CommunicationCV.class);
}
