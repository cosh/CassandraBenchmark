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
