package com.flare.mq.broker.cluster;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BrokerIdentityTest {

    @Test
    public void testDeriveFromAddress() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10911", null, false, null, false);
        assertEquals("127.0.0.1-10911", id.brokerName);
        assertEquals(0L, id.brokerId);
    }

    @Test
    public void testDeriveIdFromPort() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10913", null, false, null, false);
        assertEquals(2L, id.brokerId);
    }

    @Test
    public void testExplicitOverrides() {
        BrokerIdentity id = BrokerIdentity.resolve("127.0.0.1:10912", "broker-x", true, 7L, true);
        assertEquals("broker-x", id.brokerName);
        assertEquals(7L, id.brokerId);
    }
}
