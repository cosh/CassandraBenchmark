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

package cassandra.benchmark.service.internal.model;

/**
 * Created by cosh on 13.05.14.
 */
public class Mutation {
    private IdentityBucketRK identity;
    private Long timeStamp;
    private CommunicationCV communication;

    public Mutation(IdentityBucketRK identity, long timeStamp, CommunicationCV communication) {
        this.identity = identity;
        this.timeStamp = timeStamp;
        this.communication = communication;
    }

    public IdentityBucketRK getIdentity() {
        return identity;
    }

    public void setIdentity(IdentityBucketRK identity) {
        this.identity = identity;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public CommunicationCV getCommunication() {
        return communication;
    }

    public void setCommunication(CommunicationCV communication) {
        this.communication = communication;
    }
}
