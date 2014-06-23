package cassandra.benchmark.service.internal.model;

import com.netflix.astyanax.annotations.Component;

/**
 * A model for a column value
 *
 * Created by cosh on 13.05.14.
 */
public class CommunicationCV {

    /**
     * The IMSI of the a-party
     */
    @Component(ordinal = 0)
    String aPartyImsi;

    /**
     * The IMEI of the a-party
     */
    @Component(ordinal = 1)
    String aPartyImei;

    /**
     * The MSISDN of the b-party
     */
    @Component(ordinal = 2)
    String bParty;

    /**
     * The duration of the call
     */
    @Component(ordinal = 3)
    Double duration;

    /**
     * Creates a new communication column value
     * @param aParty The MSISDN of the a-Party
     * @param bParty THe MSISDN of the b-Party
     * @param duration The duration
     */
    public CommunicationCV(final String aParty, final String bParty, final Double duration) {
        this.aPartyImei = "imei of " + aParty;
        this.aPartyImsi = "imsi of " + aParty;
        this.bParty = bParty;
        this.duration = duration;
    }

    /**
     * Empty contructor which is needed for astyanax
     */
    public CommunicationCV() {

    }

    /**
     * Gets the IMSI of the a-party
     * @return IMSI
     */
    public String getaPartyImsi() {
        return aPartyImsi;
    }

    /**
     * Gets the IMEI of the a-party
     * @return IMEI
     */
    public String getaPartyImei() {
        return aPartyImei;
    }

    /**
     * Gets the MSISDN of the b-party
     * @return MSISDN
     */
    public String getbParty() {
        return bParty;
    }

    /**
     * Gets the duration
     * @return The duration
     */
    public Double getDuration() {
        return duration;
    }
}