package com.flare.mq.nameserver.cluster;

import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MasterElectionManagerTest {

    private BrokerData broker(String name, long totalMessages, long brokerId) {
        BrokerData d = new BrokerData("DefaultCluster", name);
        d.setTotalMessages(totalMessages);
        d.getBrokerAddrs().put(brokerId, "127.0.0.1:" + (10911 + brokerId));
        return d;
    }

    @Test
    public void testLargestOffsetWins() {
        BrokerData b = broker("b", 100, 1);
        BrokerData c = broker("c", 50, 2);
        BrokerData winner = MasterElectionManager.electNewMaster(Arrays.asList(c, b));
        assertEquals("b", winner.getBrokerName());
    }

    @Test
    public void testTieBreakBySmallestBrokerId() {
        BrokerData b = broker("b", 100, 2);
        BrokerData c = broker("c", 100, 1);
        BrokerData winner = MasterElectionManager.electNewMaster(Arrays.asList(b, c));
        assertEquals("c", winner.getBrokerName());
    }

    @Test
    public void testPromoteSetsIdZeroSlotAndBumpsEpoch() {
        MasterElectionManager mgr = new MasterElectionManager(new ServiceRegistry(), 20000);
        BrokerData b = broker("b", 100, 1);
        long epoch = mgr.promoteToMaster(b);
        assertEquals(1L, epoch);
        assertEquals(1L, mgr.getEpoch());
        assertTrue(b.getBrokerAddrs().containsKey(0L));   // id0 槽位指向新 master
        assertTrue(b.hasMaster());
    }
}
