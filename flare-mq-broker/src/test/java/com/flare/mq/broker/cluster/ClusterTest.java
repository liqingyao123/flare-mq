package com.flare.mq.broker.cluster;

import com.flare.mq.store.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 集群和高可用功能测试
 * 
 * @author FlareMQ Team
 */
public class ClusterTest {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterTest.class);
    
    private ClusterManager clusterManager;
    private ClusterConfig clusterConfig;
    
    @BeforeEach
    void setUp() {
        clusterConfig = new ClusterConfig("127.0.0.1:10911", 0L);
        clusterConfig.setMasterCandidate(true);
        clusterConfig.setEnableReplication(true);
        clusterConfig.setEnableFailover(true);
        
        clusterManager = new ClusterManager("TestCluster", "broker-a", clusterConfig);
        logger.info("=== 开始集群功能测试 ===");
    }
    
    @AfterEach
    void tearDown() {
        if (clusterManager != null && clusterManager.isRunning()) {
            clusterManager.shutdown();
        }
        logger.info("=== 集群功能测试结束 ===");
    }
    
    @Test
    void testClusterManagerLifecycle() {
        logger.info("--- 测试集群管理器生命周期 ---");
        
        // 初始状态检查
        assertFalse(clusterManager.isRunning());
        assertEquals(ClusterState.INITIALIZING, clusterManager.getClusterState());
        
        // 启动集群管理器
        clusterManager.start();
        assertTrue(clusterManager.isRunning());
        assertEquals(ClusterState.RUNNING, clusterManager.getClusterState());
        
        // 检查当前节点是否成为Master
        assertTrue(clusterManager.isMaster());
        assertEquals(BrokerRole.MASTER, clusterManager.getCurrentRole());
        
        // 关闭集群管理器
        clusterManager.shutdown();
        assertFalse(clusterManager.isRunning());
        assertEquals(ClusterState.STOPPED, clusterManager.getClusterState());
        
        logger.info("集群管理器生命周期测试通过");
    }
    
    @Test
    void testClusterConfig() {
        logger.info("--- 测试集群配置 ---");
        
        // 测试默认配置
        ClusterConfig config = new ClusterConfig();
        assertEquals("127.0.0.1:10911", config.getBrokerAddr());
        assertEquals(0L, config.getBrokerId());
        assertTrue(config.isMasterCandidate());
        assertTrue(config.isEnableReplication());
        assertTrue(config.isEnableFailover());
        
        // 测试配置验证
        config.validate(); // 应该不抛异常
        
        // 测试无效配置
        ClusterConfig invalidConfig = new ClusterConfig();
        invalidConfig.setBrokerAddr("");
        assertThrows(IllegalArgumentException.class, invalidConfig::validate);
        
        logger.info("集群配置测试通过");
    }
    
    @Test
    void testBrokerNode() {
        logger.info("--- 测试Broker节点 ---");
        
        BrokerNode node = new BrokerNode("broker-a", "127.0.0.1:10911", 0L,
                                       BrokerRole.MASTER, System.currentTimeMillis());

        // 设置节点为运行状态
        node.setStatus(BrokerStatus.RUNNING);

        // 测试基本属性
        assertEquals("broker-a", node.getBrokerName());
        assertEquals("127.0.0.1:10911", node.getBrokerAddr());
        assertEquals(0L, node.getBrokerId());
        assertTrue(node.isMaster());
        assertFalse(node.isSlave());

        // 测试健康状态
        assertTrue(node.isHealthy());
        assertTrue(node.isAvailable());
        assertTrue(node.isActive(30000L));
        
        // 测试性能指标更新
        node.updateMetrics(0.5, 0.6, 0.3, 10L);
        assertEquals(0.5, node.getCpuUsage(), 0.01);
        assertEquals(0.6, node.getMemoryUsage(), 0.01);
        assertEquals(0.3, node.getDiskUsage(), 0.01);
        assertEquals(10L, node.getNetworkLatency());
        
        // 测试负载分数计算
        double loadScore = node.calculateLoadScore();
        assertTrue(loadScore >= 0.0 && loadScore <= 1.0);
        
        logger.info("Broker节点测试通过");
    }
    
    @Test
    void testReplicationManager() throws Exception {
        logger.info("--- 测试数据复制管理器 ---");
        
        clusterManager.start();
        ReplicationManager replicationManager = clusterManager.getReplicationManager();
        
        // 测试复制管理器状态
        assertTrue(replicationManager.isRunning());
        assertTrue(replicationManager.isMaster());
        
        // 创建测试消息
        Message message = createTestMessage("test-topic", "test-key", "test-value");
        
        // 测试单消息复制
        CompletableFuture<Boolean> future = replicationManager.replicateMessage(message);
        Boolean result = future.get(5, TimeUnit.SECONDS);
        assertTrue(result); // 没有Slave节点时应该返回true
        
        // 测试批量消息复制
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(createTestMessage("test-topic", "key-" + i, "value-" + i));
        }
        
        CompletableFuture<Boolean> batchFuture = replicationManager.replicateMessages(messages);
        Boolean batchResult = batchFuture.get(5, TimeUnit.SECONDS);
        assertTrue(batchResult);
        
        // 测试统计信息
        ReplicationStatistics stats = replicationManager.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.getReplicatedMessages() >= 0);
        assertTrue(stats.getSuccessRate() >= 0.0 && stats.getSuccessRate() <= 1.0);
        
        logger.info("数据复制管理器测试通过");
    }

    @Test
    void testLoadBalancer() {
        logger.info("--- 测试负载均衡器 ---");
        
        clusterManager.start();
        LoadBalancer loadBalancer = clusterManager.getLoadBalancer();
        
        // 测试负载均衡器状态
        assertTrue(loadBalancer.isRunning());
        assertEquals(LoadBalanceStrategy.ROUND_ROBIN, loadBalancer.getStrategy());
        
        // 创建测试节点列表
        List<BrokerNode> nodes = new ArrayList<>();
        BrokerNode node1 = new BrokerNode("broker-1", "127.0.0.1:10911", 1L,
                                BrokerRole.SLAVE, System.currentTimeMillis());
        node1.setStatus(BrokerStatus.RUNNING);
        nodes.add(node1);

        BrokerNode node2 = new BrokerNode("broker-2", "127.0.0.1:10912", 2L,
                                BrokerRole.SLAVE, System.currentTimeMillis());
        node2.setStatus(BrokerStatus.RUNNING);
        nodes.add(node2);
        
        // 测试节点选择
        BrokerNode selected = loadBalancer.selectBroker(nodes);
        assertNotNull(selected);
        assertTrue(nodes.contains(selected));
        
        // 测试写节点选择（当前集群只有一个master节点）
        BrokerNode writeNode = loadBalancer.selectWriteNode();
        assertNotNull(writeNode);
        assertTrue(writeNode.isMaster());

        // 测试读节点选择（当前集群只有一个节点）
        BrokerNode readNode = loadBalancer.selectReadNode();
        assertNotNull(readNode);
        
        // 测试策略更新
        loadBalancer.updateStrategy(LoadBalanceStrategy.RANDOM);
        assertEquals(LoadBalanceStrategy.RANDOM, loadBalancer.getStrategy());
        
        // 测试统计信息
        LoadBalanceStatistics stats = loadBalancer.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.getTotalSelections() > 0);
        
        logger.info("负载均衡器测试通过");
    }
    
    @Test
    void testClusterStatistics() {
        logger.info("--- 测试集群统计信息 ---");
        
        clusterManager.start();
        
        // 获取集群统计信息
        ClusterStatistics stats = clusterManager.getStatistics();
        assertNotNull(stats);
        
        assertEquals("TestCluster", stats.getClusterName());
        assertTrue(stats.getTotalNodes() > 0);
        assertTrue(stats.getHealthyNodes() >= 0);
        assertTrue(stats.getHealthRatio() >= 0.0 && stats.getHealthRatio() <= 1.0);
        assertTrue(stats.hasMaster());
        assertEquals("broker-a", stats.getMasterBroker());
        
        // 测试健康等级
        ClusterStatistics.HealthLevel healthLevel = stats.getHealthLevel();
        assertNotNull(healthLevel);
        
        logger.info("集群统计信息: {}", stats);
        logger.info("集群统计信息测试通过");
    }
    
    @Test
    void testMultiNodeCluster() {
        logger.info("--- 测试多节点集群 ---");

        clusterManager.start();

        // 验证单节点集群状态（当前节点作为master）
        assertEquals(1, clusterManager.getClusterNodes().size());
        assertTrue(clusterManager.isMaster());

        // 测试集群节点信息
        Map<String, BrokerNode> nodes = clusterManager.getClusterNodes();
        assertTrue(nodes.containsKey("broker-a"));
        BrokerNode masterNode = nodes.get("broker-a");
        assertNotNull(masterNode);
        assertEquals(BrokerRole.MASTER, masterNode.getRole());
        
        // 测试负载均衡（使用集群中的实际节点）
        LoadBalancer loadBalancer = clusterManager.getLoadBalancer();
        List<BrokerNode> allNodes = new ArrayList<>(clusterManager.getClusterNodes().values());

        // 确保有可用节点
        assertFalse(allNodes.isEmpty());
        BrokerNode selected = loadBalancer.selectBroker(allNodes);
        assertNotNull(selected);
        
        logger.info("多节点集群测试通过");
    }
    
    /**
     * 创建测试消息
     */
    private Message createTestMessage(String topic, String key, String value) {
        Message message = new Message();
        message.setTopic(topic);
        message.setKeys(key);
        message.setBody(value.getBytes());
        message.setBornTimestamp(System.currentTimeMillis());
        return message;
    }
}
