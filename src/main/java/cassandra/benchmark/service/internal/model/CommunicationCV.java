package cassandra.benchmark.service.internal.model;

import com.netflix.astyanax.annotations.Component;

/**
 * Created by cosh on 13.05.14.
 */
public class CommunicationCV {

    @Component(ordinal = 0)
    String aPartyImsi;

    @Component(ordinal = 1)
    String aPartyImei;

    @Component(ordinal = 2)
    String bParty;

    @Component(ordinal = 3)
    Double duration;

    public CommunicationCV(final String aParty, final String bParty, final Double duration)
    {
        this.aPartyImei = "imei of " + aParty;
        this.aPartyImsi = "imsi of " + aParty;
        this.bParty = bParty;
        this.duration = duration;
    }


    public CommunicationCV()
    {

    }

    public String getaPartyImsi() {
        return aPartyImsi;
    }

    public String getaPartyImei() {
        return aPartyImei;
    }

    public String getbParty() {
        return bParty;
    }

    public Double getDuration() {
        return duration;
    }
}