package com.flare.mq.nameserver.routing.cluster;

import com.flare.mq.nameserver.routing.RouteConstants;
import com.flare.mq.nameserver.routing.model.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 故障转移管理器
 * 
 * 负责Broker健康检查和故障转移
 * 
 * @author FlareMQ Team
 */
public class FailoverManager {
    
    private static final Logger logger = LoggerFactory.getLogger(FailoverManager.class);
    
    /**
     * Broker健康状态缓存
     */
    private final ConcurrentHashMap<String, BrokerHealthInfo> healthCache = new ConcurrentHashMap<>();
    
    /**
     * 故障转移记录
     */
    private final ConcurrentHashMap<String, String> failoverRecords = new ConcurrentHashMap<>();
    
    /**
     * 健康检查调度器
     */
    private final ScheduledExecutorService healthCheckScheduler;
    
    /**
     * 是否启用自动健康检查
     */
    private boolean autoHealthCheckEnabled = true;
    
    public FailoverManager() {
        this.healthCheckScheduler = Executors.newScheduledThreadPool(2);
        
        // 启动定期健康检查
        if (autoHealthCheckEnabled) {
            startPeriodicHealthCheck();
        }
    }
    
    /**
     * 检查Broker是否健康
     */
    public boolean isHealthy(BrokerInfo broker) {
        if (broker == null) {
            return false;
        }
        
        String brokerId = broker.getBrokerId();
        BrokerHealthInfo healthInfo = healthCache.get(brokerId);
        
        if (healthInfo == null) {
            // 首次检查，创建健康信息
            healthInfo = new BrokerHealthInfo(brokerId);
            healthCache.put(brokerId, healthInfo);
        }
        
        // 更新健康信息
        updateHealthInfo(healthInfo, broker);
        
        // 判断是否健康
        boolean isHealthy = evaluateHealth(healthInfo, broker);
        
        logger.debug("Broker健康检查: {} -> {}", brokerId, isHealthy);
        return isHealthy;
    }
    
    /**
     * 选择备用Broker
     */
    public BrokerInfo selectBackup(List<BrokerInfo> availableBrokers, BrokerInfo failedBroker) {
        if (availableBrokers == null || availableBrokers.isEmpty()) {
            return null;
        }
        
        String failedBrokerId = failedBroker.getBrokerId();
        logger.info("开始为故障Broker选择备用: {}", failedBrokerId);
        
        // 过滤掉故障的Broker
        List<BrokerInfo> candidateBrokers = availableBrokers.stream()
                .filter(broker -> !broker.getBrokerId().equals(failedBrokerId))
                .filter(this::isHealthy)
                .collect(java.util.stream.Collectors.toList());
        
        if (candidateBrokers.isEmpty()) {
            logger.warn("没有可用的备用Broker");
            return null;
        }
        
        // 选择负载最低的Broker作为备用
        BrokerInfo backupBroker = candidateBrokers.stream()
                .min((b1, b2) -> Double.compare(b1.calculateLoadScore(), b2.calculateLoadScore()))
                .orElse(null);
        
        if (backupBroker != null) {
            // 记录故障转移
            failoverRecords.put(failedBrokerId, backupBroker.getBrokerId());
            logger.info("选择备用Broker: {} -> {}", failedBrokerId, backupBroker.getBrokerId());
        }
        
        return backupBroker;
    }
    
    /**
     * 更新Broker健康状态
     */
    public void updateBrokerHealth(String brokerId, boolean healthy) {
        BrokerHealthInfo healthInfo = healthCache.computeIfAbsent(brokerId, BrokerHealthInfo::new);
        healthInfo.setHealthy(healthy);
        healthInfo.setLastUpdateTime(System.currentTimeMillis());
        
        if (!healthy) {
            healthInfo.incrementFailureCount();
            logger.warn("Broker健康状态更新为不健康: {}, 失败次数: {}", 
                       brokerId, healthInfo.getFailureCount());
        } else {
            healthInfo.resetFailureCount();
            logger.debug("Broker健康状态更新为健康: {}", brokerId);
        }
    }
    
    /**
     * 是否为故障转移的Broker
     */
    public boolean isFailoverBroker(BrokerInfo broker) {
        return failoverRecords.containsValue(broker.getBrokerId());
    }
    
    /**
     * 获取故障转移记录
     */
    public String getFailoverRecord(String failedBrokerId) {
        return failoverRecords.get(failedBrokerId);
    }
    
    /**
     * 清除故障转移记录
     */
    public void clearFailoverRecord(String failedBrokerId) {
        String backupBrokerId = failoverRecords.remove(failedBrokerId);
        if (backupBrokerId != null) {
            logger.info("清除故障转移记录: {} -> {}", failedBrokerId, backupBrokerId);
        }
    }
    
    /**
     * 更新健康信息
     */
    private void updateHealthInfo(BrokerHealthInfo healthInfo, BrokerInfo broker) {
        healthInfo.setLastHeartbeatTime(broker.getLastHeartbeatTime());
        healthInfo.setCpuUsage(broker.getCpuUsage());
        healthInfo.setMemoryUsage(broker.getMemoryUsage());
        healthInfo.setDiskUsage(broker.getDiskUsage());
        healthInfo.setActiveConnections(broker.getActiveConnections());
    }
    
    /**
     * 评估Broker健康状态
     */
    private boolean evaluateHealth(BrokerHealthInfo healthInfo, BrokerInfo broker) {
        long currentTime = System.currentTimeMillis();
        
        // 1. 检查心跳超时
        long heartbeatTimeout = currentTime - broker.getLastHeartbeatTime();
        if (heartbeatTimeout > RouteConstants.FAILOVER_TIMEOUT) {
            logger.warn("Broker心跳超时: {}, 超时时间: {}ms", broker.getBrokerId(), heartbeatTimeout);
            return false;
        }
        
        // 2. 检查资源使用率
        if (broker.getCpuUsage() > 90.0) {
            logger.warn("Broker CPU使用率过高: {}, CPU: {}%", broker.getBrokerId(), broker.getCpuUsage());
            return false;
        }
        
        if (broker.getMemoryUsage() > 90.0) {
            logger.warn("Broker内存使用率过高: {}, Memory: {}%", broker.getBrokerId(), broker.getMemoryUsage());
            return false;
        }
        
        if (broker.getDiskUsage() > 95.0) {
            logger.warn("Broker磁盘使用率过高: {}, Disk: {}%", broker.getBrokerId(), broker.getDiskUsage());
            return false;
        }
        
        // 3. 检查连续失败次数
        if (healthInfo.getFailureCount() >= RouteConstants.MAX_RETRY_TIMES) {
            logger.warn("Broker连续失败次数过多: {}, 失败次数: {}", 
                       broker.getBrokerId(), healthInfo.getFailureCount());
            return false;
        }
        
        return true;
    }
    
    /**
     * 启动定期健康检查
     */
    private void startPeriodicHealthCheck() {
        healthCheckScheduler.scheduleWithFixedDelay(() -> {
            try {
                performHealthCheck();
            } catch (Exception e) {
                logger.error("定期健康检查失败", e);
            }
        }, RouteConstants.HEALTH_CHECK_INTERVAL, RouteConstants.HEALTH_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
        
        logger.info("启动定期健康检查，间隔: {}ms", RouteConstants.HEALTH_CHECK_INTERVAL);
    }
    
    /**
     * 执行健康检查
     */
    private void performHealthCheck() {
        long currentTime = System.currentTimeMillis();
        int healthyCount = 0;
        int unhealthyCount = 0;
        
        for (BrokerHealthInfo healthInfo : healthCache.values()) {
            // 检查是否长时间未更新
            long lastUpdateTime = healthInfo.getLastUpdateTime();
            if (currentTime - lastUpdateTime > RouteConstants.FAILOVER_TIMEOUT) {
                healthInfo.setHealthy(false);
                healthInfo.incrementFailureCount();
                unhealthyCount++;
                logger.warn("Broker长时间未更新健康状态: {}", healthInfo.getBrokerId());
            } else if (healthInfo.isHealthy()) {
                healthyCount++;
            } else {
                unhealthyCount++;
            }
        }
        
        logger.debug("健康检查完成，健康: {}, 不健康: {}", healthyCount, unhealthyCount);
    }
    
    /**
     * 关闭故障转移管理器
     */
    public void shutdown() {
        if (healthCheckScheduler != null && !healthCheckScheduler.isShutdown()) {
            healthCheckScheduler.shutdown();
            try {
                if (!healthCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    healthCheckScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                healthCheckScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("故障转移管理器已关闭");
    }
    
    /**
     * 获取健康统计信息
     */
    public String getHealthStats() {
        int totalBrokers = healthCache.size();
        long healthyBrokers = healthCache.values().stream()
                .mapToLong(info -> info.isHealthy() ? 1 : 0)
                .sum();
        int failoverCount = failoverRecords.size();
        
        return String.format("Health Stats: total=%d, healthy=%d, unhealthy=%d, failovers=%d", 
                           totalBrokers, healthyBrokers, totalBrokers - healthyBrokers, failoverCount);
    }
    
    /**
     * Broker健康信息内部类
     */
    private static class BrokerHealthInfo {
        private final String brokerId;
        private boolean healthy = true;
        private int failureCount = 0;
        private long lastUpdateTime = System.currentTimeMillis();
        private long lastHeartbeatTime = System.currentTimeMillis();
        private double cpuUsage = 0.0;
        private double memoryUsage = 0.0;
        private double diskUsage = 0.0;
        private int activeConnections = 0;
        
        public BrokerHealthInfo(String brokerId) {
            this.brokerId = brokerId;
        }
        
        // Getters and Setters
        public String getBrokerId() { return brokerId; }
        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }
        public int getFailureCount() { return failureCount; }
        public void incrementFailureCount() { this.failureCount++; }
        public void resetFailureCount() { this.failureCount = 0; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        public void setLastUpdateTime(long lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
        public long getLastHeartbeatTime() { return lastHeartbeatTime; }
        public void setLastHeartbeatTime(long lastHeartbeatTime) { this.lastHeartbeatTime = lastHeartbeatTime; }
        public double getCpuUsage() { return cpuUsage; }
        public void setCpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; }
        public double getMemoryUsage() { return memoryUsage; }
        public void setMemoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; }
        public double getDiskUsage() { return diskUsage; }
        public void setDiskUsage(double diskUsage) { this.diskUsage = diskUsage; }
        public int getActiveConnections() { return activeConnections; }
        public void setActiveConnections(int activeConnections) { this.activeConnections = activeConnections; }
    }
}
