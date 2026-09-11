package com.flare.mq.test.cluster;

import com.flare.mq.broker.cluster.ClusterConfig;
import com.flare.mq.broker.cluster.ClusterManager;
import com.flare.mq.nameserver.NameServerConfig;
import com.flare.mq.nameserver.NameServerController;
import com.flare.mq.nameserver.registry.BrokerData;
import com.flare.mq.nameserver.registry.TopicRouteData;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.client.NettyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterFailoverIntegrationTest {

    private NameServerController nameServer;
    private ClusterManager broker1; // master
    private ClusterManager broker2; // slave, brokerId 更小(id1) 但 offset 更小
    private ClusterManager broker3; // slave, brokerId 更大(id2) 但 offset 更大

    private static final String NS_ADDR = "127.0.0.1:19876";

    @BeforeEach
    public void setUp() throws Exception {
        NameServerConfig nsConfig = new NameServerConfig();
        nsConfig.setListenPort(19876);
        nsConfig.setMasterLeaseDurationMs(12000);  // 租约(12s) ≥ broker 心跳(5s)，所有 broker 连续存活，选主确定
        nsConfig.setFailoverScanIntervalMs(500);
        nameServer = new NameServerController(nsConfig);
        nameServer.start();

        broker1 = startBroker("127.0.0.1:20911", 0, "broker-1");
        broker2 = startBroker("127.0.0.1:20912", 1, "broker-2");
        broker3 = startBroker("127.0.0.1:20913", 2, "broker-3");
    }

    private ClusterManager startBroker(String addr, long brokerId, String dirSuffix) throws Exception {
        ClusterConfig config = new ClusterConfig(addr, brokerId);
        config.setNameServerAddr(NS_ADDR);
        Path tmp = Files.createTempDirectory("flare-" + dirSuffix);
        config.setDataDir(tmp.toString());
        ClusterManager cm = new ClusterManager("DefaultCluster", "127.0.0.1-" + addr.split(":")[1], config);
        cm.start();
        return cm;
    }

    @AfterEach
    public void tearDown() {
        if (broker3 != null) broker3.shutdown();
        if (broker2 != null) broker2.shutdown();
        if (broker1 != null) broker1.shutdown();
        if (nameServer != null) nameServer.shutdown();
    }

    @Test
    public void testFailoverPicksMostCaughtUpSlave() throws Exception {
        // 等待三个 broker 注册完成
        await(() -> nameServer.getServiceRegistry().getBrokerCount() == 3, 15_000);
        // 初始 master = brokerId 0 的 broker1（过滤存活，避开残留）
        BrokerData master = findAliveMaster();
        assertNotNull(master, "should have a master after startup");
        assertEquals("127.0.0.1:20911", master.getMasterAddr());

        // 制造 offset 差异：broker2 有 100 条、broker3 有 200 条（offset 更大但 brokerId 更大）
        // broker2: offset=100, minBrokerId=1；broker3: offset=200, minBrokerId=2
        // 选举规则为 offset 降序 → brokerId 升序，故赢家是 broker3——证明 offset 规则压过 brokerId 规则
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20912").setTotalMessages(100);
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20913").setTotalMessages(200);

        long epochBefore = nameServer.getMasterElectionManager().getEpoch();

        // C1：建在旧主（broker1）上的 topic 路由，故障转移后应迁移到新主
        nameServer.getServiceRegistry().registerTopicRoute("127.0.0.1-20911", "test-topic", 4, 4, 6);

        // kill master
        broker1.shutdown();
        broker1 = null;

        // 等待 failover：新 master 地址变为 broker3（offset 200 最大，理应接管；brokerId 更大也不影响），
        // epoch 递增，且 broker3 本地角色翻转。租约 12s 与心跳同量级，给足 20s 安全余量。
        await(() -> {
            BrokerData m = findAliveMaster();
            return m != null && "127.0.0.1:20913".equals(m.getMasterAddr())
                    && broker3.isMaster();
        }, 20_000);

        assertEquals(epochBefore + 1, nameServer.getMasterElectionManager().getEpoch());

        // C1：topic 写路由已从旧主迁到新主——死主不再持有任何写路由，且新主已接管
        // （注：broker 共享 user.home/flare-mq-store，test-topic 可能残留多 broker 条目，故按"无死主 + 有新主"断言，不依赖顺序）
        TopicRouteData migrated =
                nameServer.getServiceRegistry().getTopicRouteData("test-topic");
        assertNotNull(migrated);
        assertTrue(migrated.getQueueDatas().stream()
                        .anyMatch(qd -> "127.0.0.1-20913".equals(qd.getBrokerName())),
                "new master should appear in topic write routes");
        assertFalse(migrated.getQueueDatas().stream()
                        .anyMatch(qd -> "127.0.0.1-20911".equals(qd.getBrokerName())),
                "dead old master should no longer hold any write route");

        // C1：真实写入新主成功（客户端写路径可用）
        NettyClient client = new NettyClient("127.0.0.1", 20913);
        client.connect();
        try {
            String body = "{\"topic\":\"test-topic\",\"body\":\"hello-after-failover\",\"messageId\":\"post-failover-1\"}";
            ProtocolMessage send = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST,
                    body.getBytes(StandardCharsets.UTF_8));
            ProtocolMessage resp = client.sendSync(send, 5000);
            assertNotNull(resp);
            assertTrue(resp.isSuccess(), "new master should accept writes after failover");
        } finally {
            client.shutdown();
        }
    }

    /** 拥有 id0 槽位且租约内（存活）的节点才是当前 master。 */
    private BrokerData findAliveMaster() {
        long now = System.currentTimeMillis();
        for (BrokerData d : nameServer.getServiceRegistry().getAllBrokerData().values()) {
            if (d.getBrokerAddrs().containsKey(0L)
                    && (now - d.getLastUpdateTimestamp()) <= 5000) {
                return d;
            }
        }
        return null;
    }

    private void await(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("condition not met within " + timeoutMs + "ms");
    }
}
