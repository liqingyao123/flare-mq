package com.flare.mq.nameserver;

import com.flare.mq.nameserver.cluster.MasterElectionManager;
import com.flare.mq.nameserver.health.HealthChecker;
import com.flare.mq.nameserver.registry.ServiceDiscovery;
import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.nameserver.route.RouteInfoManager;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class NameServerHeartbeatTest {

    @Test
    public void testHeartbeatRefreshesBrokerLiveness() {
        ServiceRegistry registry = new ServiceRegistry();
        HealthChecker checker = new HealthChecker(registry);
        NameServerRequestHandler handler = new NameServerRequestHandler(
                new ServiceDiscovery(registry), registry, new RouteInfoManager(),
                checker, new MasterElectionManager(registry, 20000));

        // 先注册 broker，使其存在于注册表
        registry.registerBroker("DefaultCluster", "127.0.0.1:20911", "127.0.0.1-20911", 0L,
                "127.0.0.1:20912", null, null, false);
        registry.getBrokerData("127.0.0.1-20911").setLastUpdateTimestamp(0L); // 故意置旧

        // 携带身份的 broker 心跳
        String body = "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"127.0.0.1-20911\","
                + "\"brokerAddr\":\"127.0.0.1:20911\",\"brokerId\":0}";
        ProtocolMessage hb = new ProtocolMessage(MessageType.HEARTBEAT_REQUEST,
                body.getBytes(StandardCharsets.UTF_8));

        handler.handleRequest(null, hb);

        assertTrue(registry.getBrokerData("127.0.0.1-20911").getLastUpdateTimestamp() > 0L);
    }
}
