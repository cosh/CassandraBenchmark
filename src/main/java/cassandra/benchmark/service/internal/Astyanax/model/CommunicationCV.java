package cassandra.benchmark.service.internal.Astyanax.model;

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
    private Double duration;

    public CommunicationCV(final String aParty, final String bParty, final Double duration)
    {
        this.aparty = aparty;
        this.bparty = bparty;
        this.duration = duration;
    }


    public CommunicationCV()
    {

    }
}