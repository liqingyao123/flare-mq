package com.ruyuan.mq.nameserver.routing.local;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地负载均衡器
 * 
 * 在单机内进行队列级别的负载均衡优化
 * 
 * @author RuYuan MQ Team
 */
public class LocalLoadBalancer {
    
    private static final Logger logger = LoggerFactory.getLogger(LocalLoadBalancer.class);
    
    /**
     * 负载均衡阈值
     */
    private double loadBalanceThreshold = RouteConstants.DEFAULT_LOAD_THRESHOLD;
    
    /**
     * 是否启用负载均衡优化
     */
    private boolean loadBalanceEnabled = true;
    
    /**
     * 优化队列选择
     */
    public int optimize(int originalQueueId, LocalRouter.QueueLoadInfo loadInfo, int queueCount) {
        if (!loadBalanceEnabled || loadInfo == null || queueCount <= 1) {
            return originalQueueId;
        }
        
        try {
            // 1. 检查当前负载均衡度
            double loadBalance = loadInfo.calculateLoadBalance(queueCount);
            
            if (loadBalance >= loadBalanceThreshold) {
                // 负载均衡良好，不需要优化
                logger.debug("负载均衡良好: {}, 无需优化", loadBalance);
                return originalQueueId;
            }
            
            // 2. 检查原始队列的负载
            long originalQueueLoad = loadInfo.getQueueMessageCount(originalQueueId);
            long avgLoad = loadInfo.getTotalMessages() / queueCount;
            
            // 如果原始队列负载不高，则不优化
            if (originalQueueLoad <= avgLoad * 1.5) {
                logger.debug("原始队列负载不高: {}, 平均负载: {}", originalQueueLoad, avgLoad);
                return originalQueueId;
            }
            
            // 3. 寻找负载较轻的队列
            int optimizedQueueId = findLightLoadQueue(loadInfo, queueCount, originalQueueId);
            
            if (optimizedQueueId != originalQueueId) {
                logger.debug("负载均衡优化: {} -> {}, 原负载: {}, 新负载: {}", 
                           originalQueueId, optimizedQueueId, 
                           originalQueueLoad, loadInfo.getQueueMessageCount(optimizedQueueId));
                return optimizedQueueId;
            }
            
            return originalQueueId;
            
        } catch (Exception e) {
            logger.error("负载均衡优化失败", e);
            return originalQueueId;
        }
    }
    
    /**
     * 寻找负载较轻的队列
     */
    private int findLightLoadQueue(LocalRouter.QueueLoadInfo loadInfo, int queueCount, int excludeQueue) {
        int lightestQueue = -1;
        long minLoad = Long.MAX_VALUE;
        
        for (int i = 0; i < queueCount; i++) {
            if (i == excludeQueue) {
                continue; // 跳过原始队列
            }
            
            long queueLoad = loadInfo.getQueueMessageCount(i);
            if (queueLoad < minLoad) {
                minLoad = queueLoad;
                lightestQueue = i;
            }
        }
        
        // 如果找到的最轻负载队列确实比原始队列轻很多，则返回它
        long originalLoad = loadInfo.getQueueMessageCount(excludeQueue);
        if (lightestQueue != -1 && minLoad < originalLoad * 0.7) {
            return lightestQueue;
        }
        
        return excludeQueue; // 没有找到明显更好的队列
    }
    
    /**
     * 基于时间窗口的负载均衡
     */
    public int optimizeByTimeWindow(int originalQueueId, LocalRouter.QueueLoadInfo loadInfo, 
                                   int queueCount, long timeWindowMs) {
        if (!loadBalanceEnabled || loadInfo == null || queueCount <= 1) {
            return originalQueueId;
        }
        
        long currentTime = System.currentTimeMillis();
        
        // 检查各队列在时间窗口内的活跃度
        int mostIdleQueue = -1;
        long maxIdleTime = 0;
        
        for (int i = 0; i < queueCount; i++) {
            long lastAccessTime = loadInfo.getQueueLastAccessTime(i);
            long idleTime = currentTime - lastAccessTime;
            
            if (idleTime > maxIdleTime) {
                maxIdleTime = idleTime;
                mostIdleQueue = i;
            }
        }
        
        // 如果找到空闲时间超过时间窗口的队列，优先使用
        if (mostIdleQueue != -1 && maxIdleTime > timeWindowMs) {
            logger.debug("时间窗口优化: {} -> {}, 空闲时间: {}ms", 
                       originalQueueId, mostIdleQueue, maxIdleTime);
            return mostIdleQueue;
        }
        
        return originalQueueId;
    }
    
    /**
     * 基于队列容量的负载均衡
     */
    public int optimizeByCapacity(int originalQueueId, LocalRouter.QueueLoadInfo loadInfo, 
                                 int queueCount, long maxQueueCapacity) {
        if (!loadBalanceEnabled || loadInfo == null || queueCount <= 1) {
            return originalQueueId;
        }
        
        // 检查原始队列是否接近容量上限
        long originalQueueLoad = loadInfo.getQueueMessageCount(originalQueueId);
        if (originalQueueLoad < maxQueueCapacity * 0.8) {
            return originalQueueId; // 容量充足，不需要优化
        }
        
        // 寻找容量充足的队列
        for (int i = 0; i < queueCount; i++) {
            if (i == originalQueueId) {
                continue;
            }
            
            long queueLoad = loadInfo.getQueueMessageCount(i);
            if (queueLoad < maxQueueCapacity * 0.5) {
                logger.debug("容量优化: {} -> {}, 原容量: {}, 新容量: {}", 
                           originalQueueId, i, originalQueueLoad, queueLoad);
                return i;
            }
        }
        
        return originalQueueId; // 没有找到容量充足的队列
    }
    
    /**
     * 智能负载均衡优化
     */
    public int smartOptimize(int originalQueueId, LocalRouter.QueueLoadInfo loadInfo, 
                           int queueCount, OptimizeContext context) {
        if (!loadBalanceEnabled || loadInfo == null || queueCount <= 1) {
            return originalQueueId;
        }
        
        // 综合考虑多个因素进行优化
        int optimizedQueue = originalQueueId;
        
        // 1. 基础负载均衡
        optimizedQueue = optimize(optimizedQueue, loadInfo, queueCount);
        
        // 2. 时间窗口优化
        if (context.getTimeWindowMs() > 0) {
            optimizedQueue = optimizeByTimeWindow(optimizedQueue, loadInfo, queueCount, context.getTimeWindowMs());
        }
        
        // 3. 容量优化
        if (context.getMaxQueueCapacity() > 0) {
            optimizedQueue = optimizeByCapacity(optimizedQueue, loadInfo, queueCount, context.getMaxQueueCapacity());
        }
        
        return optimizedQueue;
    }
    
    /**
     * 计算负载均衡效果
     */
    public LoadBalanceMetrics calculateMetrics(LocalRouter.QueueLoadInfo loadInfo, int queueCount) {
        if (loadInfo == null || queueCount <= 0) {
            return new LoadBalanceMetrics();
        }
        
        double loadBalance = loadInfo.calculateLoadBalance(queueCount);
        long totalMessages = loadInfo.getTotalMessages();
        double avgLoad = totalMessages > 0 ? (double) totalMessages / queueCount : 0;
        
        // 计算负载方差
        double variance = 0.0;
        for (int i = 0; i < queueCount; i++) {
            long queueLoad = loadInfo.getQueueMessageCount(i);
            variance += Math.pow(queueLoad - avgLoad, 2);
        }
        variance /= queueCount;
        
        return new LoadBalanceMetrics(loadBalance, avgLoad, Math.sqrt(variance), totalMessages);
    }
    
    /**
     * 设置负载均衡阈值
     */
    public void setLoadBalanceThreshold(double threshold) {
        this.loadBalanceThreshold = threshold;
        logger.info("设置负载均衡阈值: {}", threshold);
    }
    
    /**
     * 启用/禁用负载均衡
     */
    public void setLoadBalanceEnabled(boolean enabled) {
        this.loadBalanceEnabled = enabled;
        logger.info("负载均衡状态: {}", enabled ? "启用" : "禁用");
    }
    
    /**
     * 获取负载均衡器状态
     */
    public String getBalancerStatus() {
        return String.format("LocalLoadBalancer: enabled=%s, threshold=%.2f", 
                           loadBalanceEnabled, loadBalanceThreshold);
    }
    
    /**
     * 优化上下文
     */
    public static class OptimizeContext {
        private long timeWindowMs = 0;
        private long maxQueueCapacity = 0;
        
        public OptimizeContext() {}
        
        public OptimizeContext(long timeWindowMs, long maxQueueCapacity) {
            this.timeWindowMs = timeWindowMs;
            this.maxQueueCapacity = maxQueueCapacity;
        }
        
        // Getters and Setters
        public long getTimeWindowMs() { return timeWindowMs; }
        public void setTimeWindowMs(long timeWindowMs) { this.timeWindowMs = timeWindowMs; }
        public long getMaxQueueCapacity() { return maxQueueCapacity; }
        public void setMaxQueueCapacity(long maxQueueCapacity) { this.maxQueueCapacity = maxQueueCapacity; }
    }
    
    /**
     * 负载均衡指标
     */
    public static class LoadBalanceMetrics {
        private final double loadBalance;
        private final double avgLoad;
        private final double stdDev;
        private final long totalMessages;
        
        public LoadBalanceMetrics() {
            this(0, 0, 0, 0);
        }
        
        public LoadBalanceMetrics(double loadBalance, double avgLoad, double stdDev, long totalMessages) {
            this.loadBalance = loadBalance;
            this.avgLoad = avgLoad;
            this.stdDev = stdDev;
            this.totalMessages = totalMessages;
        }
        
        // Getters
        public double getLoadBalance() { return loadBalance; }
        public double getAvgLoad() { return avgLoad; }
        public double getStdDev() { return stdDev; }
        public long getTotalMessages() { return totalMessages; }
        
        @Override
        public String toString() {
            return String.format("LoadBalanceMetrics{balance=%.3f, avgLoad=%.1f, stdDev=%.1f, total=%d}", 
                               loadBalance, avgLoad, stdDev, totalMessages);
        }
    }
}
