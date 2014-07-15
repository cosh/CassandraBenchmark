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