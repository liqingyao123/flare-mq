package com.flare.mq.nameserver;

import com.flare.mq.nameserver.cluster.MasterElectionManager;
import com.flare.mq.nameserver.health.HealthChecker;
import com.flare.mq.nameserver.registry.ServiceDiscovery;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.nameserver.route.RouteInfoManager;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class NameServerEpochFenceTest {

    @Test
    public void testStaleEpochMasterRegistrationRejected() {
        ServiceRegistry registry = new ServiceRegistry();
        MasterElectionManager election = new MasterElectionManager(registry, 20000);
        // 先把 epoch 抬到 1（模拟发生过一次选举）
        election.promoteToMaster(makeBroker("b", 1));
        assertEquals(1L, election.getEpoch());

        NameServerRequestHandler handler = new NameServerRequestHandler(
                new ServiceDiscovery(registry), registry, new RouteInfoManager(),
                new HealthChecker(registry), election);

        // 旧的 id0 master 用 epoch=0 重新注册 → 应被拒
        String body = "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"m\",\"brokerAddr\":\"addr-m\","
                + "\"brokerId\":0,\"epoch\":0}";
        ProtocolMessage resp = handler.handleRequest(null,
                new ProtocolMessage(MessageType.REGISTER_BROKER_REQUEST,
                        body.getBytes(StandardCharsets.UTF_8)));

        assertFalse(resp.isSuccess());
        assertEquals(ResponseCode.STALE_EPOCH, resp.getStatus());
    }

    private com.flare.mq.nameserver.registry.BrokerData makeBroker(String name, long id) {
        com.flare.mq.nameserver.registry.BrokerData d =
                new com.flare.mq.nameserver.registry.BrokerData("DefaultCluster", name);
        d.getBrokerAddrs().put(id, "127.0.0.1:" + (10911 + id));
        return d;
    }
}
