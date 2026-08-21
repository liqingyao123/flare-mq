package com.flare.mq.test.cluster;

import com.flare.mq.broker.cluster.ClusterConfig;
import com.flare.mq.broker.cluster.ClusterManager;
import com.flare.mq.nameserver.NameServerConfig;
import com.flare.mq.nameserver.NameServerController;
import com.flare.mq.nameserver.registry.BrokerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterFailoverIntegrationTest {

    private NameServerController nameServer;
    private ClusterManager broker1; // master
    private ClusterManager broker2; // slave, offset 大
    private ClusterManager broker3; // slave, offset 小

    private static final String NS_ADDR = "127.0.0.1:19876";

    @BeforeEach
    public void setUp() throws Exception {
        NameServerConfig nsConfig = new NameServerConfig();
        nsConfig.setListenPort(19876);
        nsConfig.setMasterLeaseDurationMs(1500);   // 缩短租约，加速 kill-master 场景
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

        // 制造 offset 差异：broker2 有 100 条，broker3 有 50 条
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20912").setTotalMessages(100);
        nameServer.getServiceRegistry().getBrokerData("127.0.0.1-20913").setTotalMessages(50);

        long epochBefore = nameServer.getMasterElectionManager().getEpoch();

        // kill master
        broker1.shutdown();
        broker1 = null;

        // 等待 failover：新 master 地址变为 broker2（存活），epoch 递增，且 broker2 本地角色翻转
        await(() -> {
            BrokerData m = findAliveMaster();
            return m != null && "127.0.0.1:20912".equals(m.getMasterAddr())
                    && broker2.isMaster();
        }, 15_000);

        assertEquals(epochBefore + 1, nameServer.getMasterElectionManager().getEpoch());
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
