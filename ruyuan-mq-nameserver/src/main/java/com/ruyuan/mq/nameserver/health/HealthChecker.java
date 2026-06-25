package com.ruyuan.mq.nameserver.health;

import com.ruyuan.mq.nameserver.registry.ServiceRegistry;
import com.ruyuan.mq.nameserver.registry.BrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 健康检查组件 - 监控Broker服务的健康状态
 * 
 * @author RuYuan MQ Team
 */
public class HealthChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthChecker.class);
    
    private final ServiceRegistry serviceRegistry;
    
    // Broker心跳信息存储 - Key: brokerAddr, Value: HeartbeatData
    private final ConcurrentHashMap<String, HeartbeatData> heartbeatTable;
    
    // 健康检查配置
    private static final long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2; // 2分钟
    private static final long HEARTBEAT_TIMEOUT = 1000 * 30; // 30秒心跳超时
    private static final long CONSUMER_HEARTBEAT_TIMEOUT = 1000 * 60; // 60s Consumer 心跳超时
    
    // 统计信息
    private final AtomicLong totalHeartbeats = new AtomicLong(0);
    private final AtomicLong failedHeartbeats = new AtomicLong(0);
    private final AtomicLong removedBrokers = new AtomicLong(0);
    
    public HealthChecker(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.heartbeatTable = new ConcurrentHashMap<>();
        logger.info("HealthChecker initialized");
    }
    
    /**
     * 处理Broker心跳
     */
    public void processBrokerHeartbeat(String clusterName, String brokerAddr, String brokerName, 
                                     long brokerId, long timeoutMillis) {
        try {
            // 更新心跳信息
            HeartbeatData heartbeatData = heartbeatTable.computeIfAbsent(brokerAddr, 
                    k -> new HeartbeatData(clusterName, brokerName, brokerId));
            
            heartbeatData.setLastHeartbeatTime(System.currentTimeMillis());
            heartbeatData.setTimeoutMillis(timeoutMillis);
            heartbeatData.incrementHeartbeatCount();
            
            // 更新Broker的最后更新时间
            BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
            if (brokerData != null) {
                brokerData.setLastUpdateTimestamp(System.currentTimeMillis());
            }
            
            totalHeartbeats.incrementAndGet();
            
            logger.debug("Processed heartbeat from broker: cluster={}, brokerName={}, brokerAddr={}, brokerId={}", 
                        clusterName, brokerName, brokerAddr, brokerId);
            
        } catch (Exception e) {
            failedHeartbeats.incrementAndGet();
            logger.error("Failed to process heartbeat from broker: {}", brokerAddr, e);
        }
    }
    
    /**
     * 扫描不活跃的Broker
     */
    public void scanNotActiveBroker() {
        logger.debug("Starting to scan not active brokers...");
        
        long currentTime = System.currentTimeMillis();
        int removedCount = 0;
        
        // 检查心跳超时的Broker
        for (Map.Entry<String, HeartbeatData> entry : heartbeatTable.entrySet()) {
            String brokerAddr = entry.getKey();
            HeartbeatData heartbeatData = entry.getValue();
            
            long lastHeartbeat = heartbeatData.getLastHeartbeatTime();
            long timeout = Math.max(heartbeatData.getTimeoutMillis(), HEARTBEAT_TIMEOUT);
            
            if ((currentTime - lastHeartbeat) > timeout) {
                // 心跳超时，标记为不健康
                heartbeatData.setHealthy(false);
                logger.warn("Broker heartbeat timeout: brokerAddr={}, lastHeartbeat={}, timeout={}ms", 
                           brokerAddr, lastHeartbeat, timeout);
            }
        }
        
        // 检查长时间未更新的Broker
        Map<String, BrokerData> allBrokers = serviceRegistry.getAllBrokerData();
        for (Map.Entry<String, BrokerData> entry : allBrokers.entrySet()) {
            String brokerName = entry.getKey();
            BrokerData brokerData = entry.getValue();
            
            long lastUpdate = brokerData.getLastUpdateTimestamp();
            if ((currentTime - lastUpdate) > BROKER_CHANNEL_EXPIRED_TIME) {
                // Broker长时间未更新，移除
                removeBroker(brokerData);
                removedCount++;
                
                logger.warn("Removed inactive broker: brokerName={}, lastUpdate={}, expiredTime={}ms", 
                           brokerName, lastUpdate, BROKER_CHANNEL_EXPIRED_TIME);
            }
        }
        
        if (removedCount > 0) {
            removedBrokers.addAndGet(removedCount);
            logger.info("Removed {} inactive brokers", removedCount);
        }
        
        // 清理心跳超时的 Consumer
        scanNotActiveConsumers(currentTime);

        logger.debug("Scan not active brokers completed, removed: {}", removedCount);
    }
    
    /**
     * 扫描并清理心跳超时的 Consumer
     */
    private void scanNotActiveConsumers(long currentTime) {
        try {
            java.util.Set<String> groups = serviceRegistry.getAllConsumerGroups();
            for (String group : groups) {
                java.util.Map<String, ServiceRegistry.ConsumerHeartbeatData> consumers =
                        serviceRegistry.getConsumerHeartbeatData(group);
                for (java.util.Map.Entry<String, ServiceRegistry.ConsumerHeartbeatData> entry : consumers.entrySet()) {
                    String consumerId = entry.getKey();
                    ServiceRegistry.ConsumerHeartbeatData data = entry.getValue();
                    if ((currentTime - data.getLastHeartbeatTime()) > CONSUMER_HEARTBEAT_TIMEOUT) {
                        logger.warn("Consumer heartbeat timeout, removing: group={}, consumerId={}", group, consumerId);
                        serviceRegistry.unregisterConsumer(group, consumerId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning not active consumers", e);
        }
    }

    /**
     * 获取Broker健康状态
     */
    public BrokerHealthStatus getBrokerHealthStatus(String brokerAddr) {
        HeartbeatData heartbeatData = heartbeatTable.get(brokerAddr);
        if (heartbeatData == null) {
            return new BrokerHealthStatus(brokerAddr, false, 0, 0, "No heartbeat data");
        }
        
        long currentTime = System.currentTimeMillis();
        long lastHeartbeat = heartbeatData.getLastHeartbeatTime();
        long timeSinceLastHeartbeat = currentTime - lastHeartbeat;
        
        boolean isHealthy = heartbeatData.isHealthy() && 
                           timeSinceLastHeartbeat <= heartbeatData.getTimeoutMillis();
        
        String status = isHealthy ? "Healthy" : 
                       (timeSinceLastHeartbeat > heartbeatData.getTimeoutMillis() ? "Heartbeat timeout" : "Unhealthy");
        
        return new BrokerHealthStatus(brokerAddr, isHealthy, lastHeartbeat, 
                                    heartbeatData.getHeartbeatCount(), status);
    }
    
    /**
     * 获取所有Broker的健康状态
     */
    public Map<String, BrokerHealthStatus> getAllBrokerHealthStatus() {
        Map<String, BrokerHealthStatus> statusMap = new ConcurrentHashMap<>();
        
        for (String brokerAddr : heartbeatTable.keySet()) {
            statusMap.put(brokerAddr, getBrokerHealthStatus(brokerAddr));
        }
        
        return statusMap;
    }
    
    /**
     * 获取健康检查统计信息
     */
    public HealthCheckStats getStatistics() {
        int totalBrokers = heartbeatTable.size();
        int healthyBrokers = 0;
        
        for (HeartbeatData heartbeatData : heartbeatTable.values()) {
            if (heartbeatData.isHealthy()) {
                healthyBrokers++;
            }
        }
        
        return new HealthCheckStats(
            totalBrokers,
            healthyBrokers,
            totalHeartbeats.get(),
            failedHeartbeats.get(),
            removedBrokers.get()
        );
    }
    
    /**
     * 移除Broker
     */
    private void removeBroker(BrokerData brokerData) {
        String clusterName = brokerData.getCluster();
        String brokerName = brokerData.getBrokerName();
        
        // 移除所有该Broker的地址
        for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
            Long brokerId = entry.getKey();
            String brokerAddr = entry.getValue();
            
            // 从服务注册表中注销
            serviceRegistry.unregisterBroker(clusterName, brokerAddr, brokerName, brokerId);
            
            // 从心跳表中移除
            heartbeatTable.remove(brokerAddr);
        }
    }
    
    /**
     * 关闭健康检查组件
     */
    public void shutdown() {
        logger.info("Shutting down HealthChecker...");
        heartbeatTable.clear();
        logger.info("HealthChecker shutdown completed");
    }
    
    /**
     * 心跳数据
     */
    private static class HeartbeatData {
        private final String clusterName;
        private final String brokerName;
        private final long brokerId;
        private volatile long lastHeartbeatTime;
        private volatile long timeoutMillis;
        private volatile boolean healthy;
        private final AtomicLong heartbeatCount;
        
        public HeartbeatData(String clusterName, String brokerName, long brokerId) {
            this.clusterName = clusterName;
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.lastHeartbeatTime = System.currentTimeMillis();
            this.timeoutMillis = HEARTBEAT_TIMEOUT;
            this.healthy = true;
            this.heartbeatCount = new AtomicLong(0);
        }
        
        // Getters and Setters
        public String getClusterName() { return clusterName; }
        public String getBrokerName() { return brokerName; }
        public long getBrokerId() { return brokerId; }
        public long getLastHeartbeatTime() { return lastHeartbeatTime; }
        public void setLastHeartbeatTime(long lastHeartbeatTime) { this.lastHeartbeatTime = lastHeartbeatTime; }
        public long getTimeoutMillis() { return timeoutMillis; }
        public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }
        public long getHeartbeatCount() { return heartbeatCount.get(); }
        public void incrementHeartbeatCount() { heartbeatCount.incrementAndGet(); }
    }
    
    /**
     * Broker健康状态
     */
    public static class BrokerHealthStatus {
        private final String brokerAddr;
        private final boolean healthy;
        private final long lastHeartbeatTime;
        private final long heartbeatCount;
        private final String status;
        
        public BrokerHealthStatus(String brokerAddr, boolean healthy, long lastHeartbeatTime, 
                                long heartbeatCount, String status) {
            this.brokerAddr = brokerAddr;
            this.healthy = healthy;
            this.lastHeartbeatTime = lastHeartbeatTime;
            this.heartbeatCount = heartbeatCount;
            this.status = status;
        }
        
        // Getters
        public String getBrokerAddr() { return brokerAddr; }
        public boolean isHealthy() { return healthy; }
        public long getLastHeartbeatTime() { return lastHeartbeatTime; }
        public long getHeartbeatCount() { return heartbeatCount; }
        public String getStatus() { return status; }
        
        @Override
        public String toString() {
            return String.format("BrokerHealthStatus{brokerAddr='%s', healthy=%s, lastHeartbeat=%d, count=%d, status='%s'}", 
                               brokerAddr, healthy, lastHeartbeatTime, heartbeatCount, status);
        }
    }
    
    /**
     * 健康检查统计信息
     */
    public static class HealthCheckStats {
        private final int totalBrokers;
        private final int healthyBrokers;
        private final long totalHeartbeats;
        private final long failedHeartbeats;
        private final long removedBrokers;
        
        public HealthCheckStats(int totalBrokers, int healthyBrokers, long totalHeartbeats, 
                              long failedHeartbeats, long removedBrokers) {
            this.totalBrokers = totalBrokers;
            this.healthyBrokers = healthyBrokers;
            this.totalHeartbeats = totalHeartbeats;
            this.failedHeartbeats = failedHeartbeats;
            this.removedBrokers = removedBrokers;
        }
        
        // Getters
        public int getTotalBrokers() { return totalBrokers; }
        public int getHealthyBrokers() { return healthyBrokers; }
        public long getTotalHeartbeats() { return totalHeartbeats; }
        public long getFailedHeartbeats() { return failedHeartbeats; }
        public long getRemovedBrokers() { return removedBrokers; }
        
        @Override
        public String toString() {
            return String.format("HealthCheckStats{totalBrokers=%d, healthyBrokers=%d, totalHeartbeats=%d, failedHeartbeats=%d, removedBrokers=%d}", 
                               totalBrokers, healthyBrokers, totalHeartbeats, failedHeartbeats, removedBrokers);
        }
    }
}
