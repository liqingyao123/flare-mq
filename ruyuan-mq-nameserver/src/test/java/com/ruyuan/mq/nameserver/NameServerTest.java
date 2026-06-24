package com.ruyuan.mq.nameserver;

import com.ruyuan.mq.nameserver.registry.*;
import com.ruyuan.mq.nameserver.health.HealthChecker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NameServer测试类
 * 
 * @author RuYuan MQ Team
 */
public class NameServerTest {
    
    private static final Logger logger = LoggerFactory.getLogger(NameServerTest.class);
    
    private ServiceRegistry serviceRegistry;
    private ServiceDiscovery serviceDiscovery;
    private HealthChecker healthChecker;
    
    @BeforeEach
    void setUp() {
        serviceRegistry = new ServiceRegistry();
        serviceDiscovery = new ServiceDiscovery(serviceRegistry);
        healthChecker = new HealthChecker(serviceRegistry);
        logger.info("=== 开始NameServer测试 ===");
    }
    
    @AfterEach
    void tearDown() {
        if (healthChecker != null) {
            healthChecker.shutdown();
        }
        if (serviceRegistry != null) {
            serviceRegistry.shutdown();
        }
        logger.info("=== NameServer测试结束 ===");
    }
    
    @Test
    void testServiceRegistry() {
        logger.info("--- 测试服务注册 ---");
        
        // 创建Topic配置
        TopicConfigSerializeWrapper topicWrapper = new TopicConfigSerializeWrapper();
        TopicConfig topicConfig = new TopicConfig("test-topic", 4, 4, 6);
        topicWrapper.addTopicConfig("test-topic", topicConfig);
        
        // 注册Broker
        RegisterBrokerResult result = serviceRegistry.registerBroker(
                "DefaultCluster",
                "192.168.1.100:10911",
                "broker-a",
                0L,
                "192.168.1.100:10912",
                topicWrapper,
                null,
                false
        );
        
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertEquals("192.168.1.100:10911", result.getMasterAddr());
        
        // 验证Broker信息
        BrokerData brokerData = serviceRegistry.getBrokerData("broker-a");
        assertNotNull(brokerData);
        assertEquals("DefaultCluster", brokerData.getCluster());
        assertEquals("broker-a", brokerData.getBrokerName());
        assertTrue(brokerData.hasMaster());
        
        // 验证集群信息
        ClusterInfo clusterInfo = serviceRegistry.getClusterInfo("DefaultCluster");
        assertNotNull(clusterInfo);
        assertTrue(clusterInfo.containsBroker("broker-a"));
        
        logger.info("服务注册测试通过");
    }
    
    @Test
    void testServiceDiscovery() {
        logger.info("--- 测试服务发现 ---");
        
        // 先注册一个Broker
        registerTestBroker("broker-a", "192.168.1.100:10911");
        
        // 测试获取Topic路由信息
        TopicRouteData routeData = serviceDiscovery.getTopicRouteData("test-topic");
        assertNotNull(routeData);
        assertFalse(routeData.getQueueDatas().isEmpty());
        assertFalse(routeData.getBrokerDatas().isEmpty());
        
        // 测试获取所有可用Broker
        List<BrokerData> availableBrokers = serviceDiscovery.getAllAvailableBrokers();
        assertFalse(availableBrokers.isEmpty());
        assertEquals("broker-a", availableBrokers.get(0).getBrokerName());
        
        // 测试查找Master Broker
        BrokerData masterBroker = serviceDiscovery.findMasterBroker("broker-a");
        assertNotNull(masterBroker);
        assertTrue(masterBroker.hasMaster());
        
        // 测试获取写队列
        List<QueueData> writeQueues = serviceDiscovery.getWriteQueuesByTopic("test-topic");
        assertFalse(writeQueues.isEmpty());
        assertEquals(4, writeQueues.get(0).getWriteQueueNums());
        
        logger.info("服务发现测试通过");
    }
    
    @Test
    void testHealthChecker() {
        logger.info("--- 测试健康检查 ---");
        
        // 注册Broker
        registerTestBroker("broker-a", "192.168.1.100:10911");
        
        // 发送心跳
        healthChecker.processBrokerHeartbeat(
                "DefaultCluster",
                "192.168.1.100:10911",
                "broker-a",
                0L,
                30000L
        );
        
        // 检查健康状态
        HealthChecker.BrokerHealthStatus healthStatus = 
                healthChecker.getBrokerHealthStatus("192.168.1.100:10911");
        assertNotNull(healthStatus);
        assertTrue(healthStatus.isHealthy());
        assertEquals("Healthy", healthStatus.getStatus());
        
        // 获取统计信息
        HealthChecker.HealthCheckStats stats = healthChecker.getStatistics();
        assertNotNull(stats);
        assertEquals(1, stats.getTotalBrokers());
        assertEquals(1, stats.getHealthyBrokers());
        assertTrue(stats.getTotalHeartbeats() > 0);
        
        logger.info("健康检查测试通过");
    }
    
    @Test
    void testBrokerUnregister() {
        logger.info("--- 测试Broker注销 ---");
        
        // 注册Broker
        registerTestBroker("broker-a", "192.168.1.100:10911");
        
        // 验证Broker存在
        assertEquals(1, serviceRegistry.getBrokerCount());
        assertNotNull(serviceRegistry.getBrokerData("broker-a"));
        
        // 注销Broker
        serviceRegistry.unregisterBroker(
                "DefaultCluster",
                "192.168.1.100:10911",
                "broker-a",
                0L
        );
        
        // 验证Broker已被移除
        assertEquals(0, serviceRegistry.getBrokerCount());
        assertNull(serviceRegistry.getBrokerData("broker-a"));
        
        logger.info("Broker注销测试通过");
    }
    
    @Test
    void testMultipleBrokers() {
        logger.info("--- 测试多Broker场景 ---");
        
        // 注册多个Broker
        registerTestBroker("broker-a", "192.168.1.100:10911");
        registerTestBroker("broker-b", "192.168.1.101:10911");
        registerTestBroker("broker-c", "192.168.1.102:10911");
        
        // 验证Broker数量
        assertEquals(3, serviceRegistry.getBrokerCount());
        
        // 验证服务发现
        List<BrokerData> availableBrokers = serviceDiscovery.getAllAvailableBrokers();
        assertEquals(3, availableBrokers.size());
        
        // 验证集群信息
        ClusterInfo clusterInfo = serviceRegistry.getClusterInfo("DefaultCluster");
        assertNotNull(clusterInfo);
        assertEquals(3, clusterInfo.getBrokerCount());
        
        // 验证Topic路由信息
        TopicRouteData routeData = serviceDiscovery.getTopicRouteData("test-topic");
        assertNotNull(routeData);
        assertEquals(3, routeData.getQueueDatas().size());
        assertEquals(3, routeData.getBrokerDatas().size());
        
        logger.info("多Broker场景测试通过");
    }
    
    @Test
    void testServiceDiscoveryCache() {
        logger.info("--- 测试服务发现缓存 ---");
        
        // 注册Broker
        registerTestBroker("broker-a", "192.168.1.100:10911");
        
        // 第一次查询
        long startTime = System.currentTimeMillis();
        TopicRouteData routeData1 = serviceDiscovery.getTopicRouteData("test-topic");
        long firstQueryTime = System.currentTimeMillis() - startTime;
        
        // 第二次查询（应该使用缓存）
        startTime = System.currentTimeMillis();
        TopicRouteData routeData2 = serviceDiscovery.getTopicRouteData("test-topic");
        long secondQueryTime = System.currentTimeMillis() - startTime;
        
        assertNotNull(routeData1);
        assertNotNull(routeData2);
        
        // 缓存查询应该更快
        assertTrue(secondQueryTime <= firstQueryTime);
        
        // 获取统计信息
        ServiceDiscovery.ServiceDiscoveryStats stats = serviceDiscovery.getStatistics();
        assertNotNull(stats);
        assertEquals(1, stats.getTotalBrokers());
        assertEquals(1, stats.getAvailableBrokers());
        assertTrue(stats.getCachedTopics() > 0);
        
        logger.info("服务发现缓存测试通过");
    }
    
    @Test
    void testNameServerConfig() {
        logger.info("--- 测试NameServer配置 ---");
        
        NameServerConfig config = new NameServerConfig();
        
        // 测试默认配置
        assertEquals("127.0.0.1:9876", config.getNameServerAddress());
        assertEquals(9876, config.getListenPort());
        assertEquals("DefaultCluster", config.getClusterName());
        assertTrue(config.isEnableServiceDiscovery());
        assertTrue(config.isEnablePersistence());
        
        // 测试配置修改
        config.setListenPort(9877);
        config.setClusterName("TestCluster");
        config.setEnableServiceDiscovery(false);
        
        assertEquals(9877, config.getListenPort());
        assertEquals("TestCluster", config.getClusterName());
        assertFalse(config.isEnableServiceDiscovery());
        
        logger.info("NameServer配置测试通过");
    }
    
    /**
     * 注册测试Broker
     */
    private void registerTestBroker(String brokerName, String brokerAddr) {
        TopicConfigSerializeWrapper topicWrapper = new TopicConfigSerializeWrapper();
        TopicConfig topicConfig = new TopicConfig("test-topic", 4, 4, 6);
        topicWrapper.addTopicConfig("test-topic", topicConfig);
        
        serviceRegistry.registerBroker(
                "DefaultCluster",
                brokerAddr,
                brokerName,
                0L,
                brokerAddr.replace("10911", "10912"),
                topicWrapper,
                null,
                false
        );
    }
}
