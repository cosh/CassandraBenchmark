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
