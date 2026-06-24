package com.ruyuan.mq.broker.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 故障转移管理器 - 负责集群故障检测和自动转移
 * 
 * @author RuYuan MQ Team
 */
public class FailoverManager {
    
    private static final Logger logger = LoggerFactory.getLogger(FailoverManager.class);
    
    private final ClusterManager clusterManager;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService failoverExecutor;
    
    // 故障检测配置
    private final ClusterConfig config;
    
    // 故障状态跟踪
    private final ConcurrentHashMap<String, FailureInfo> failureTracker;
    
    // 故障转移状态
    private volatile boolean failoverInProgress;
    private volatile long lastFailoverTime;
    
    // 统计信息
    private final AtomicLong totalFailovers;
    private final AtomicLong successfulFailovers;
    private final AtomicLong failoverTime;
    
    // 运行状态
    private volatile boolean running;
    
    public FailoverManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.config = clusterManager.clusterConfig;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FailoverManager-Scheduled");
            t.setDaemon(true);
            return t;
        });
        this.failoverExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "FailoverManager-Executor");
            t.setDaemon(true);
            return t;
        });
        this.failureTracker = new ConcurrentHashMap<>();
        this.failoverInProgress = false;
        this.lastFailoverTime = 0L;
        this.totalFailovers = new AtomicLong(0);
        this.successfulFailovers = new AtomicLong(0);
        this.failoverTime = new AtomicLong(0);
        this.running = false;
        
        logger.info("FailoverManager initialized");
    }
    
    /**
     * 启动故障转移管理器
     */
    public void start() {
        if (running) {
            logger.warn("FailoverManager already running");
            return;
        }
        
        if (!config.isEnableFailover()) {
            logger.info("Failover is disabled in configuration");
            return;
        }
        
        running = true;
        
        // 启动故障检测任务
        scheduledExecutor.scheduleAtFixedRate(this::detectFailures, 
                                            10, 15, TimeUnit.SECONDS);
        
        // 启动故障恢复检测任务
        scheduledExecutor.scheduleAtFixedRate(this::detectRecovery, 
                                            30, 30, TimeUnit.SECONDS);
        
        logger.info("FailoverManager started");
    }
    
    /**
     * 关闭故障转移管理器
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down FailoverManager...");
        running = false;
        
        // 关闭线程池
        scheduledExecutor.shutdown();
        failoverExecutor.shutdown();
        
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            if (!failoverExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                failoverExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduledExecutor.shutdownNow();
            failoverExecutor.shutdownNow();
        }
        
        // 清理故障跟踪信息
        failureTracker.clear();
        
        logger.info("FailoverManager shutdown completed");
    }
    
    /**
     * 处理集群健康度下降
     */
    public void handleClusterDegradation(double healthRatio) {
        if (!running || failoverInProgress) {
            return;
        }
        
        logger.warn("Handling cluster degradation: healthRatio={:.2f}", healthRatio);
        
        // 如果健康度过低，触发紧急故障转移
        if (healthRatio < config.getMinHealthRatio() * 0.5) {
            triggerEmergencyFailover();
        }
    }
    
    /**
     * 检测节点故障
     */
    private void detectFailures() {
        if (!running) {
            return;
        }
        
        try {
            logger.debug("Detecting node failures...");
            
            Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
            long currentTime = System.currentTimeMillis();
            
            for (BrokerNode node : clusterNodes.values()) {
                String nodeKey = node.getBrokerName();
                
                // 检查节点是否超时
                boolean isTimeout = !node.isActive(config.getNodeTimeoutMs());
                
                // 检查节点健康状态
                boolean isUnhealthy = !node.isHealthy();
                
                if (isTimeout || isUnhealthy) {
                    handleNodeFailure(node, isTimeout ? "timeout" : "unhealthy");
                } else {
                    // 节点正常，清除故障记录
                    failureTracker.remove(nodeKey);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error detecting failures", e);
        }
    }
    
    /**
     * 处理节点故障
     */
    private void handleNodeFailure(BrokerNode node, String reason) {
        String nodeKey = node.getBrokerName();
        FailureInfo failureInfo = failureTracker.computeIfAbsent(nodeKey, 
                k -> new FailureInfo(node, System.currentTimeMillis()));
        
        failureInfo.incrementFailureCount();
        failureInfo.setLastFailureTime(System.currentTimeMillis());
        failureInfo.setFailureReason(reason);
        
        logger.warn("Node failure detected: node={}, reason={}, failureCount={}", 
                   nodeKey, reason, failureInfo.getFailureCount());
        
        // 如果是Master节点故障，立即触发故障转移
        if (node.isMaster() && failureInfo.getFailureCount() >= 2) {
            triggerMasterFailover(node);
        }
        // 如果是Slave节点故障，标记为不可用
        else if (node.isSlave() && failureInfo.getFailureCount() >= 3) {
            markNodeUnavailable(node);
        }
    }
    
    /**
     * 触发Master故障转移
     */
    private void triggerMasterFailover(BrokerNode failedMaster) {
        if (failoverInProgress) {
            logger.warn("Failover already in progress, ignoring master failover request");
            return;
        }
        
        logger.warn("Triggering master failover: failedMaster={}", failedMaster.getBrokerName());
        
        failoverInProgress = true;
        totalFailovers.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        failoverExecutor.submit(() -> {
            try {
                boolean success = executeMasterFailover(failedMaster);
                
                if (success) {
                    successfulFailovers.incrementAndGet();
                    logger.info("Master failover completed successfully");
                } else {
                    logger.error("Master failover failed");
                }
                
            } catch (Exception e) {
                logger.error("Error during master failover", e);
            } finally {
                long duration = System.currentTimeMillis() - startTime;
                failoverTime.addAndGet(duration);
                lastFailoverTime = System.currentTimeMillis();
                failoverInProgress = false;
                
                logger.info("Master failover completed in {}ms", duration);
            }
        });
    }
    
    /**
     * 执行Master故障转移
     */
    private boolean executeMasterFailover(BrokerNode failedMaster) {
        logger.info("Executing master failover from: {}", failedMaster.getBrokerName());
        
        // 1. 选择新的Master
        BrokerNode newMaster = selectNewMaster(failedMaster);
        if (newMaster == null) {
            logger.error("No suitable candidate for new master");
            return false;
        }
        
        // 2. 提升新Master
        boolean promoted = promoteToMaster(newMaster);
        if (!promoted) {
            logger.error("Failed to promote new master: {}", newMaster.getBrokerName());
            return false;
        }
        
        // 3. 更新集群状态
        updateClusterAfterFailover(failedMaster, newMaster);
        
        // 4. 通知其他节点
        notifyFailoverCompletion(failedMaster, newMaster);
        
        logger.info("Master failover completed: {} -> {}", 
                   failedMaster.getBrokerName(), newMaster.getBrokerName());
        
        return true;
    }
    
    /**
     * 选择新的Master
     */
    private BrokerNode selectNewMaster(BrokerNode failedMaster) {
        Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
        
        // 选择最适合的Slave节点作为新Master
        return clusterNodes.values().stream()
                .filter(node -> node.isSlave())
                .filter(BrokerNode::isAvailable)
                .filter(node -> node.isActive(config.getNodeTimeoutMs()))
                .filter(node -> !failureTracker.containsKey(node.getBrokerName()))
                .min((n1, n2) -> {
                    // 优先选择负载低、延迟小的节点
                    double score1 = n1.calculateLoadScore() + n1.getNetworkLatency() / 1000.0;
                    double score2 = n2.calculateLoadScore() + n2.getNetworkLatency() / 1000.0;
                    return Double.compare(score1, score2);
                })
                .orElse(null);
    }
    
    /**
     * 提升节点为Master
     */
    private boolean promoteToMaster(BrokerNode node) {
        try {
            logger.info("Promoting node to master: {}", node.getBrokerName());
            
            // 更新节点角色
            node.setRole(BrokerRole.MASTER);
            node.setLastUpdateTime(System.currentTimeMillis());
            
            // 这里应该通知节点进行角色切换
            // 简化实现，直接返回成功
            return true;
            
        } catch (Exception e) {
            logger.error("Error promoting node to master: {}", node.getBrokerName(), e);
            return false;
        }
    }
    
    /**
     * 更新集群状态
     */
    private void updateClusterAfterFailover(BrokerNode failedMaster, BrokerNode newMaster) {
        // 移除失败的Master
        Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
        clusterNodes.remove(failedMaster.getBrokerName());
        
        // 清除故障记录
        failureTracker.remove(failedMaster.getBrokerName());
        
        logger.info("Cluster state updated after failover");
    }
    
    /**
     * 通知故障转移完成
     */
    private void notifyFailoverCompletion(BrokerNode failedMaster, BrokerNode newMaster) {
        // 这里应该通知所有相关组件故障转移已完成
        logger.info("Notified failover completion: {} -> {}", 
                   failedMaster.getBrokerName(), newMaster.getBrokerName());
    }
    
    /**
     * 标记节点不可用
     */
    private void markNodeUnavailable(BrokerNode node) {
        node.setHealthy(false);
        node.setHealthMessage("Node marked unavailable due to repeated failures");
        
        logger.warn("Marked node as unavailable: {}", node.getBrokerName());
    }
    
    /**
     * 触发紧急故障转移
     */
    private void triggerEmergencyFailover() {
        logger.error("Triggering emergency failover due to severe cluster degradation");
        
        // 紧急故障转移逻辑
        // 这里可以实现更激进的故障转移策略
    }
    
    /**
     * 检测故障恢复
     */
    private void detectRecovery() {
        if (!running) {
            return;
        }
        
        try {
            logger.debug("Detecting node recovery...");
            
            // 检查之前故障的节点是否已恢复
            failureTracker.entrySet().removeIf(entry -> {
                String nodeKey = entry.getKey();
                FailureInfo failureInfo = entry.getValue();
                BrokerNode node = clusterManager.getClusterNodes().get(nodeKey);
                
                if (node != null && node.isHealthy() && node.isActive(config.getNodeTimeoutMs())) {
                    logger.info("Node recovered from failure: {}", nodeKey);
                    return true; // 移除故障记录
                }
                
                return false;
            });
            
        } catch (Exception e) {
            logger.error("Error detecting recovery", e);
        }
    }
    
    /**
     * 获取故障转移统计信息
     */
    public FailoverStatistics getStatistics() {
        long total = totalFailovers.get();
        long successful = successfulFailovers.get();
        double successRate = total > 0 ? (double) successful / total : 1.0;
        double avgTime = successful > 0 ? (double) failoverTime.get() / successful : 0.0;
        
        return new FailoverStatistics(
            total, successful, successRate, avgTime,
            failureTracker.size(), failoverInProgress, lastFailoverTime
        );
    }
    
    // Getters
    public boolean isRunning() { return running; }
    public boolean isFailoverInProgress() { return failoverInProgress; }
    
    /**
     * 故障信息
     */
    private static class FailureInfo {
        private final BrokerNode node;
        private final long firstFailureTime;
        private volatile long lastFailureTime;
        private volatile int failureCount;
        private volatile String failureReason;
        
        public FailureInfo(BrokerNode node, long firstFailureTime) {
            this.node = node;
            this.firstFailureTime = firstFailureTime;
            this.lastFailureTime = firstFailureTime;
            this.failureCount = 1;
            this.failureReason = "unknown";
        }
        
        public void incrementFailureCount() { failureCount++; }
        public int getFailureCount() { return failureCount; }
        public void setLastFailureTime(long time) { this.lastFailureTime = time; }
        public void setFailureReason(String reason) { this.failureReason = reason; }
    }
}

/**
 * 故障转移统计信息
 */
class FailoverStatistics {
    private final long totalFailovers;
    private final long successfulFailovers;
    private final double successRate;
    private final double averageTime;
    private final int currentFailures;
    private final boolean failoverInProgress;
    private final long lastFailoverTime;
    
    public FailoverStatistics(long totalFailovers, long successfulFailovers, 
                            double successRate, double averageTime, 
                            int currentFailures, boolean failoverInProgress, 
                            long lastFailoverTime) {
        this.totalFailovers = totalFailovers;
        this.successfulFailovers = successfulFailovers;
        this.successRate = successRate;
        this.averageTime = averageTime;
        this.currentFailures = currentFailures;
        this.failoverInProgress = failoverInProgress;
        this.lastFailoverTime = lastFailoverTime;
    }
    
    // Getters
    public long getTotalFailovers() { return totalFailovers; }
    public long getSuccessfulFailovers() { return successfulFailovers; }
    public double getSuccessRate() { return successRate; }
    public double getAverageTime() { return averageTime; }
    public int getCurrentFailures() { return currentFailures; }
    public boolean isFailoverInProgress() { return failoverInProgress; }
    public long getLastFailoverTime() { return lastFailoverTime; }
    
    @Override
    public String toString() {
        return String.format("FailoverStatistics{total=%d, successful=%d, successRate=%.2f%%, " +
                           "avgTime=%.2fms, currentFailures=%d, inProgress=%s}", 
                           totalFailovers, successfulFailovers, successRate * 100, 
                           averageTime, currentFailures, failoverInProgress);
    }
}
