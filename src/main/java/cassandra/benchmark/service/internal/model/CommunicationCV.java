package cassandra.benchmark.service.internal.model;

import com.netflix.astyanax.annotations.Component;

/**
 * Created by cosh on 13.05.14.
 */
public class CommunicationCV {

    @Component(ordinal = 0)
    String aparty;

    @Component(ordinal = 1)
    String bparty;

    @Component(ordinal = 2)
    Double duration;

    public CommunicationCV(final String aParty, final String bParty, final Double duration)
    {
        this.aparty = aParty;
        this.bparty = bParty;
        this.duration = duration;
    }


    public CommunicationCV()
    {

    }

    public String getAparty() {
        return aparty;
    }

    public String getBparty() {
        return bparty;
    }

    public Double getDuration() {
        return duration;
    }
}