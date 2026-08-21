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

    @Test
    public void testCheckAndFailoverElectsBestSlave() throws Exception {
        ServiceRegistry registry = new ServiceRegistry();
        MasterElectionManager mgr = new MasterElectionManager(registry, 5000) {
            @Override
            protected long now() { return 100_000L; }   // 固定时钟
        };

        // master 已过期：注册为 id0 后把 lastUpdate 置旧（距 now=100s 超过 lease 5s）
        registry.registerBroker("DefaultCluster", "addr-m", "m", 0L, null, null, null, false);
        registry.getBrokerData("m").setLastUpdateTimestamp(10_000L);

        // 两个存活 slave，offset 不同（在注册对象上设置，模拟 broker 上报）
        registry.registerBroker("DefaultCluster", "addr-b", "b", 1L, null, null, null, false);
        registry.registerBroker("DefaultCluster", "addr-c", "c", 2L, null, null, null, false);
        registry.getBrokerData("b").setTotalMessages(100);
        registry.getBrokerData("c").setTotalMessages(50);
        registry.getBrokerData("b").setLastUpdateTimestamp(95_000L);
        registry.getBrokerData("c").setLastUpdateTimestamp(95_000L);

        mgr.checkAndFailover();

        // RPC 发给不存在的 broker 会失败，但被吞掉；注册表与 epoch 应已更新
        assertEquals(1L, mgr.getEpoch());
        assertTrue(registry.getBrokerData("b").getBrokerAddrs().containsKey(0L));   // offset 大者当选
        assertFalse(registry.getBrokerData("c").getBrokerAddrs().containsKey(0L));
    }
}
