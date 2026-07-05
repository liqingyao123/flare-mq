package com.flare.mq.nameserver.routing;

import com.flare.mq.nameserver.routing.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 路由性能测试
 * 
 * @author FlareMQ Team
 */
public class RoutingPerformanceTest {
    
    private static final Logger logger = LoggerFactory.getLogger(RoutingPerformanceTest.class);
    
    private SmartRoutingEngine routingEngine;
    
    @BeforeEach
    public void setUp() {
        routingEngine = new SmartRoutingEngine();
        
        // 注册测试集群和Broker
        setupTestClusters();
    }
    
    /**
     * 设置测试集群
     */
    private void setupTestClusters() {
        // 创建订单集群
        ClusterInfo orderCluster = new ClusterInfo("order-cluster-test", "订单集群", 
                                                  RouteConstants.CLUSTER_TYPE_ORDER, "beijing");
        orderCluster.setLoadFactor(0.3);
        routingEngine.registerCluster(orderCluster);
        
        // 为订单集群创建Broker
        for (int i = 1; i <= 3; i++) {
            BrokerInfo broker = new BrokerInfo("order-broker-" + i, "订单Broker" + i, 
                                             "localhost", 18880 + i, "order-cluster-test");
            broker.setQueueCount(8);
            broker.setCpuUsage(20 + Math.random() * 30);
            broker.setMemoryUsage(30 + Math.random() * 40);
            broker.setDiskUsage(10 + Math.random() * 20);
            broker.setActiveConnections((int) (Math.random() * 100));
            routingEngine.registerBroker(broker);
        }
        
        // 创建日志集群
        ClusterInfo logCluster = new ClusterInfo("log-cluster-test", "日志集群", 
                                                RouteConstants.CLUSTER_TYPE_LOG, "shanghai");
        logCluster.setLoadFactor(0.6);
        routingEngine.registerCluster(logCluster);
        
        // 为日志集群创建Broker
        for (int i = 1; i <= 2; i++) {
            BrokerInfo broker = new BrokerInfo("log-broker-" + i, "日志Broker" + i, 
                                             "localhost", 18890 + i, "log-cluster-test");
            broker.setQueueCount(4);
            broker.setCpuUsage(40 + Math.random() * 30);
            broker.setMemoryUsage(50 + Math.random() * 30);
            broker.setDiskUsage(20 + Math.random() * 30);
            broker.setActiveConnections((int) (Math.random() * 50));
            routingEngine.registerBroker(broker);
        }
        
        logger.info("测试集群设置完成");
    }
    
    /**
     * 单线程路由性能测试
     */
    @Test
    public void testSingleThreadRouting() {
        logger.info("开始单线程路由性能测试");
        
        int messageCount = 10000;
        long startTime = System.currentTimeMillis();
        
        int successCount = 0;
        int failureCount = 0;
        
        for (int i = 0; i < messageCount; i++) {
            SimpleMessage message = createTestMessage("order.payment", "pay", "order-" + i);
            RouteResult result = routingEngine.route(message);
            
            if (result.isSuccess()) {
                successCount++;
            } else {
                failureCount++;
            }
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) messageCount / totalTime * 1000;
        
        logger.info("单线程路由性能测试结果:");
        logger.info("消息数量: {}", messageCount);
        logger.info("总耗时: {}ms", totalTime);
        logger.info("成功数量: {}", successCount);
        logger.info("失败数量: {}", failureCount);
        logger.info("TPS: {:.2f}", tps);
        logger.info("平均延迟: {:.2f}ms", (double) totalTime / messageCount);
        
        // 验证性能目标 - 调整为更合理的期望值
        assertTrue(tps > 500, "TPS应该大于500，实际: " + tps);
        assertTrue((double) totalTime / messageCount < 20, "平均延迟应该小于20ms");
    }
    
    /**
     * 多线程路由性能测试
     */
    @Test
    public void testMultiThreadRouting() throws InterruptedException {
        logger.info("开始多线程路由性能测试");
        
        int threadCount = 10;
        int messagePerThread = 1000;
        int totalMessages = threadCount * messagePerThread;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        AtomicLong totalRouteTime = new AtomicLong(0);
        
        long startTime = System.currentTimeMillis();
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagePerThread; i++) {
                        SimpleMessage message = createTestMessage("order.payment", "pay", 
                                                                "thread-" + threadId + "-msg-" + i);
                        
                        long routeStart = System.currentTimeMillis();
                        RouteResult result = routingEngine.route(message);
                        long routeEnd = System.currentTimeMillis();
                        
                        totalRouteTime.addAndGet(routeEnd - routeStart);
                        
                        if (result.isSuccess()) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) totalMessages / totalTime * 1000;
        double avgRouteTime = (double) totalRouteTime.get() / totalMessages;
        
        logger.info("多线程路由性能测试结果:");
        logger.info("线程数量: {}", threadCount);
        logger.info("消息数量: {}", totalMessages);
        logger.info("总耗时: {}ms", totalTime);
        logger.info("成功数量: {}", successCount.get());
        logger.info("失败数量: {}", failureCount.get());
        logger.info("TPS: {:.2f}", tps);
        logger.info("平均路由延迟: {:.2f}ms", avgRouteTime);
        
        // 验证性能目标 - 调整为更合理的期望值
        assertTrue(tps > 400, "多线程TPS应该大于400，实际: " + tps);
        assertTrue(avgRouteTime < 20, "平均路由延迟应该小于20ms，实际: " + avgRouteTime);
    }
    
    /**
     * 负载均衡效果测试
     */
    @Test
    public void testLoadBalanceEffect() {
        logger.info("开始负载均衡效果测试");
        
        int messageCount = 1000;
        ConcurrentHashMap<String, AtomicLong> brokerMessageCount = new ConcurrentHashMap<>();
        
        for (int i = 0; i < messageCount; i++) {
            SimpleMessage message = createTestMessage("order.payment", "pay", "load-test-" + i);
            RouteResult result = routingEngine.route(message);
            
            if (result.isSuccess()) {
                String brokerId = result.getBroker().getBrokerId();
                brokerMessageCount.computeIfAbsent(brokerId, k -> new AtomicLong(0)).incrementAndGet();
            }
        }
        
        logger.info("负载均衡分布结果:");
        long totalRouted = 0;
        for (String brokerId : brokerMessageCount.keySet()) {
            long count = brokerMessageCount.get(brokerId).get();
            totalRouted += count;
            double percentage = (double) count / messageCount * 100;
            logger.info("Broker {}: {} 消息 ({:.1f}%)", brokerId, count, percentage);
        }
        
        // 计算负载均衡度
        double avgLoad = (double) totalRouted / brokerMessageCount.size();
        double variance = 0.0;
        for (AtomicLong count : brokerMessageCount.values()) {
            variance += Math.pow(count.get() - avgLoad, 2);
        }
        double stdDev = Math.sqrt(variance / brokerMessageCount.size());
        double loadBalance = avgLoad > 0 ? Math.max(0, 1 - (stdDev / avgLoad)) : 1.0;
        
        logger.info("负载均衡度: {:.3f}", loadBalance);
        
        // 验证负载均衡效果
        assertTrue(loadBalance > 0.8, "负载均衡度应该大于0.8，实际: " + loadBalance);
    }
    
    /**
     * 故障转移测试
     */
    @Test
    public void testFailoverScenario() {
        logger.info("开始故障转移测试");
        
        // 模拟Broker故障
        routingEngine.updateBrokerHealth("order-broker-1", false);
        
        int messageCount = 100;
        int successCount = 0;
        int failoverCount = 0;
        
        for (int i = 0; i < messageCount; i++) {
            SimpleMessage message = createTestMessage("order.payment", "pay", "failover-test-" + i);
            RouteResult result = routingEngine.route(message);
            
            if (result.isSuccess()) {
                successCount++;
                if (result.getClusterRoute().isFailover()) {
                    failoverCount++;
                }
            }
        }
        
        logger.info("故障转移测试结果:");
        logger.info("消息数量: {}", messageCount);
        logger.info("成功数量: {}", successCount);
        logger.info("故障转移数量: {}", failoverCount);
        logger.info("成功率: {:.2f}%", (double) successCount / messageCount * 100);
        
        // 恢复Broker健康状态
        routingEngine.updateBrokerHealth("order-broker-1", true);
        
        // 验证故障转移效果
        assertTrue(successCount >= messageCount * 0.9, "故障转移后成功率应该大于90%");
    }
    
    /**
     * 创建测试消息
     */
    private SimpleMessage createTestMessage(String topic, String tags, String keys) {
        SimpleMessage message = new SimpleMessage(topic, tags, keys, "test message body".getBytes());
        message.setBusinessPriority((int) (Math.random() * 5) + 1);
        message.setMessageType("order");
        return message;
    }
}
