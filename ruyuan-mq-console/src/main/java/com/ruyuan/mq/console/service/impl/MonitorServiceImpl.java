package com.ruyuan.mq.console.service.impl;

import com.ruyuan.mq.console.model.*;
import com.ruyuan.mq.console.service.MonitorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 监控服务实现
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class MonitorServiceImpl implements MonitorService {
    
    private static final Logger logger = LoggerFactory.getLogger(MonitorServiceImpl.class);
    
    private volatile boolean running = false;
    private final LocalDateTime startTime = LocalDateTime.now();
    
    // 模拟数据存储
    private final SystemOverview systemOverview = new SystemOverview();
    private final List<BrokerStatus> brokerStatusList = new ArrayList<>();
    private final List<TopicStats> topicStatsList = new ArrayList<>();
    private final List<ConsumerGroupStatus> consumerGroupStatusList = new ArrayList<>();
    private final PerformanceMetrics performanceMetrics = new PerformanceMetrics();
    private final ClusterHealth clusterHealth = new ClusterHealth();
    private final ConcurrentLinkedQueue<SystemAlert> systemAlerts = new ConcurrentLinkedQueue<>();
    private final TpsStatistics tpsStatistics = new TpsStatistics();
    private final StorageStatistics storageStatistics = new StorageStatistics();
    
    // 计数器
    private final AtomicLong messageCounter = new AtomicLong(0);
    private final AtomicLong operationCounter = new AtomicLong(0);
    
    @Override
    public void start() {
        if (running) {
            logger.warn("MonitorService already running");
            return;
        }
        
        logger.info("Starting MonitorService...");
        
        // 初始化模拟数据
        initializeSimulatedData();
        
        running = true;
        logger.info("MonitorService started successfully");
    }
    
    @Override
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down MonitorService...");
        running = false;
        logger.info("MonitorService shutdown completed");
    }
    
    @Override
    public void refreshSystemMetrics() {
        if (!running) {
            return;
        }
        
        logger.debug("Refreshing system metrics...");
        
        // 更新系统概览
        updateSystemOverview();
        
        // 更新性能指标
        updatePerformanceMetrics();
        
        // 更新集群健康状态
        updateClusterHealth();
        
        // 更新TPS统计
        updateTpsStatistics();
        
        // 更新存储统计
        updateStorageStatistics();
        
        // 检查告警
        checkSystemAlerts();
        
        logger.debug("System metrics refreshed");
    }
    
    /**
     * 初始化模拟数据
     */
    private void initializeSimulatedData() {
        // 初始化Broker状态
        BrokerStatus broker1 = new BrokerStatus("broker-1", "localhost:10911", "DefaultCluster");
        broker1.setRole("MASTER");
        broker1.setStatus("RUNNING");
        broker1.setHealthy(true);
        broker1.setCpuUsage(0.45);
        broker1.setMemoryUsage(0.62);
        broker1.setDiskUsage(0.35);
        broker1.setActiveConnections(25);
        broker1.setTotalMessages(10000);
        broker1.setCurrentTps(150.5);
        brokerStatusList.add(broker1);
        
        BrokerStatus broker2 = new BrokerStatus("broker-2", "localhost:10912", "DefaultCluster");
        broker2.setRole("SLAVE");
        broker2.setStatus("RUNNING");
        broker2.setHealthy(true);
        broker2.setCpuUsage(0.38);
        broker2.setMemoryUsage(0.55);
        broker2.setDiskUsage(0.32);
        broker2.setActiveConnections(18);
        broker2.setTotalMessages(9800);
        broker2.setCurrentTps(145.2);
        brokerStatusList.add(broker2);
        
        // 初始化Topic统计
        TopicStats topic1 = new TopicStats("order-topic", 4);
        topic1.setTotalMessages(5000);
        topic1.setCurrentTps(75.5);
        topic1.setTotalSize(1024 * 1024 * 50); // 50MB
        topicStatsList.add(topic1);
        
        TopicStats topic2 = new TopicStats("payment-topic", 8);
        topic2.setTotalMessages(8000);
        topic2.setCurrentTps(120.3);
        topic2.setTotalSize(1024 * 1024 * 80); // 80MB
        topicStatsList.add(topic2);
        
        // 初始化消费者组状态
        ConsumerGroupStatus group1 = new ConsumerGroupStatus("order-consumer-group", "order-topic");
        group1.setConsumerCount(3);
        group1.setTotalConsumed(4950);
        group1.setConsumeTps(74.8);
        group1.setLag(50);
        group1.setStatus("ACTIVE");
        consumerGroupStatusList.add(group1);
        
        ConsumerGroupStatus group2 = new ConsumerGroupStatus("payment-consumer-group", "payment-topic");
        group2.setConsumerCount(5);
        group2.setTotalConsumed(7900);
        group2.setConsumeTps(118.5);
        group2.setLag(100);
        group2.setStatus("ACTIVE");
        consumerGroupStatusList.add(group2);
        
        logger.info("Simulated data initialized");
    }
    
    /**
     * 更新系统概览
     */
    private void updateSystemOverview() {
        long uptime = System.currentTimeMillis() -
                     startTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        
        systemOverview.setUptime(uptime);
        systemOverview.setTotalBrokers(brokerStatusList.size());
        systemOverview.setHealthyBrokers((int) brokerStatusList.stream().mapToLong(b -> b.isHealthy() ? 1 : 0).sum());
        systemOverview.setTotalTopics(topicStatsList.size());
        systemOverview.setTotalQueues(topicStatsList.stream().mapToInt(TopicStats::getQueueCount).sum());
        systemOverview.setTotalMessages(messageCounter.get());
        systemOverview.setCurrentTps(tpsStatistics.getCurrentTps());
        
        // 模拟资源使用率
        systemOverview.setMemoryUsage(0.65 + Math.random() * 0.1 - 0.05);
        systemOverview.setCpuUsage(0.45 + Math.random() * 0.1 - 0.05);
        systemOverview.setDiskUsage(0.35 + Math.random() * 0.05 - 0.025);
        
        // 设置健康状态
        double healthRatio = (double) systemOverview.getHealthyBrokers() / systemOverview.getTotalBrokers();
        if (healthRatio >= 0.8) {
            systemOverview.setHealthStatus("HEALTHY");
        } else if (healthRatio >= 0.5) {
            systemOverview.setHealthStatus("WARNING");
        } else {
            systemOverview.setHealthStatus("CRITICAL");
        }
    }
    
    /**
     * 更新性能指标
     */
    private void updatePerformanceMetrics() {
        // 模拟性能数据
        performanceMetrics.setAvgReadLatency(2.5 + Math.random() * 2.0);
        performanceMetrics.setAvgWriteLatency(3.2 + Math.random() * 2.5);
        performanceMetrics.setStorageUtilization(0.65 + Math.random() * 0.1 - 0.05);
        performanceMetrics.setCacheHitRate(0.85 + Math.random() * 0.1 - 0.05);
        performanceMetrics.setCompressionRatio(2.3 + Math.random() * 0.5 - 0.25);
        performanceMetrics.setNetworkThroughput(150.0 + Math.random() * 50.0 - 25.0);
        performanceMetrics.setTotalOperations(operationCounter.incrementAndGet());
    }
    
    /**
     * 更新集群健康状态
     */
    private void updateClusterHealth() {
        clusterHealth.setTotalNodes(brokerStatusList.size());
        clusterHealth.setHealthyNodes((int) brokerStatusList.stream().mapToLong(b -> b.isHealthy() ? 1 : 0).sum());
        clusterHealth.setHealthRatio((double) clusterHealth.getHealthyNodes() / clusterHealth.getTotalNodes());
        clusterHealth.setMasterBroker(brokerStatusList.stream()
                .filter(b -> "MASTER".equals(b.getRole()))
                .findFirst()
                .map(BrokerStatus::getBrokerName)
                .orElse("unknown"));
        clusterHealth.setActiveConnections(brokerStatusList.stream().mapToInt(BrokerStatus::getActiveConnections).sum());
        
        // 设置整体状态
        double healthRatio = clusterHealth.getHealthRatio();
        if (healthRatio >= 0.8) {
            clusterHealth.setOverallStatus("HEALTHY");
        } else if (healthRatio >= 0.5) {
            clusterHealth.setOverallStatus("WARNING");
        } else {
            clusterHealth.setOverallStatus("CRITICAL");
        }
    }
    
    /**
     * 更新TPS统计
     */
    private void updateTpsStatistics() {
        double currentTps = 100.0 + Math.random() * 100.0; // 100-200 TPS
        tpsStatistics.addDataPoint(currentTps);
        
        // 更新消息计数
        messageCounter.addAndGet((long) currentTps);
    }
    
    /**
     * 更新存储统计
     */
    private void updateStorageStatistics() {
        long totalSize = 1024L * 1024 * 1024 * 10; // 10GB
        long usedSize = (long) (totalSize * (0.3 + Math.random() * 0.2)); // 30-50%
        
        storageStatistics.setTotalSize(totalSize);
        storageStatistics.setUsedSize(usedSize);
        storageStatistics.setTotalFiles(150 + (int) (Math.random() * 50));
        storageStatistics.setTotalMessages(messageCounter.get());
        storageStatistics.setCompressionRatio(2.1 + Math.random() * 0.4);
    }
    
    /**
     * 检查系统告警
     */
    private void checkSystemAlerts() {
        // 清理旧告警
        while (systemAlerts.size() > 10) {
            systemAlerts.poll();
        }
        
        // 模拟告警生成
        if (Math.random() < 0.1) { // 10%概率生成告警
            String[] alertTypes = {"INFO", "WARNING", "ERROR"};
            String[] titles = {"系统正常运行", "内存使用率较高", "Broker连接异常"};
            String[] messages = {
                "系统运行正常，所有组件状态良好",
                "当前内存使用率超过70%，建议关注",
                "检测到Broker连接异常，请检查网络状态"
            };
            
            int index = (int) (Math.random() * alertTypes.length);
            SystemAlert alert = new SystemAlert(alertTypes[index], titles[index], 
                                              messages[index], "MonitorService");
            systemAlerts.offer(alert);
        }
    }
    
    // 实现接口方法
    @Override
    public SystemOverview getSystemOverview() { return systemOverview; }
    
    @Override
    public List<BrokerStatus> getBrokerStatusList() { return new ArrayList<>(brokerStatusList); }
    
    @Override
    public List<TopicStats> getTopicStatsList() { return new ArrayList<>(topicStatsList); }
    
    @Override
    public List<ConsumerGroupStatus> getConsumerGroupStatusList() { return new ArrayList<>(consumerGroupStatusList); }
    
    @Override
    public PerformanceMetrics getPerformanceMetrics() { return performanceMetrics; }
    
    @Override
    public ClusterHealth getClusterHealth() { return clusterHealth; }
    
    @Override
    public List<SystemAlert> getSystemAlerts() { return new ArrayList<>(systemAlerts); }
    
    @Override
    public TpsStatistics getTpsStatistics() { return tpsStatistics; }
    
    @Override
    public StorageStatistics getStorageStatistics() { return storageStatistics; }
    
    @Override
    public boolean isRunning() { return running; }
}
