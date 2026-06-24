package com.ruyuan.mq.nameserver.routing;

import com.ruyuan.mq.nameserver.routing.cluster.ClusterRouter;
import com.ruyuan.mq.nameserver.routing.global.GlobalRouter;
import com.ruyuan.mq.nameserver.routing.local.LocalRouter;
import com.ruyuan.mq.nameserver.routing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 智能路由引擎
 * 
 * 统一的三层路由决策引擎
 * 
 * @author RuYuan MQ Team
 */
public class SmartRoutingEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(SmartRoutingEngine.class);
    
    /**
     * 全局路由器
     */
    private final GlobalRouter globalRouter;
    
    /**
     * 集群路由器
     */
    private final ClusterRouter clusterRouter;
    
    /**
     * 本地路由器
     */
    private final LocalRouter localRouter;
    
    /**
     * 异步执行器
     */
    private final ExecutorService executorService;
    
    /**
     * 路由统计
     */
    private final RoutingMetrics metrics;
    
    public SmartRoutingEngine() {
        this.globalRouter = new GlobalRouter();
        this.clusterRouter = new ClusterRouter();
        this.localRouter = new LocalRouter();
        this.executorService = Executors.newFixedThreadPool(10);
        this.metrics = new RoutingMetrics();
        
        logger.info("智能路由引擎初始化完成");
    }
    
    /**
     * 执行完整的路由决策
     */
    public RouteResult route(RoutableMessage message) {
        long startTime = System.currentTimeMillis();
        
        try {
            logger.debug("开始智能路由决策，Topic: {}", message.getTopic());
            
            // 第一层：全局路由决策
            GlobalRoute globalRoute = globalRouter.route(message);
            if (globalRoute.getCluster() == null) {
                metrics.incrementFailureCount();
                return RouteResult.failure("全局路由失败: " + globalRoute.getRouteReason());
            }
            
            // 第二层：集群路由决策
            ClusterRoute clusterRoute = clusterRouter.route(message, globalRoute);
            if (clusterRoute.getBroker() == null) {
                metrics.incrementFailureCount();
                return RouteResult.failure("集群路由失败: " + clusterRoute.getRouteReason());
            }
            
            // 第三层：本地路由决策
            RouteResult result = localRouter.route(message, clusterRoute);
            if (!result.isSuccess()) {
                metrics.incrementFailureCount();
                return result;
            }
            
            // 设置路由层级信息
            result.setGlobalRoute(globalRoute);
            result.setClusterRoute(clusterRoute);
            
            long totalTime = System.currentTimeMillis() - startTime;
            result.setTotalRouteTime(totalTime);
            
            // 更新统计信息
            metrics.incrementSuccessCount();
            metrics.updateRouteTime(totalTime);
            
            logger.debug("智能路由决策完成，耗时: {}ms, 路径: {}", totalTime, result.getRoutePath());
            
            return result;
            
        } catch (Exception e) {
            logger.error("智能路由决策异常", e);
            metrics.incrementFailureCount();
            return RouteResult.failure("路由决策异常: " + e.getMessage());
        }
    }
    
    /**
     * 异步路由决策
     */
    public CompletableFuture<RouteResult> routeAsync(RoutableMessage message) {
        return CompletableFuture.supplyAsync(() -> route(message), executorService);
    }
    
    /**
     * 批量路由决策
     */
    public CompletableFuture<RouteResult[]> routeBatch(RoutableMessage[] messages) {
        CompletableFuture<RouteResult>[] futures = new CompletableFuture[messages.length];
        
        for (int i = 0; i < messages.length; i++) {
            futures[i] = routeAsync(messages[i]);
        }
        
        return CompletableFuture.allOf(futures)
                .thenApply(v -> {
                    RouteResult[] results = new RouteResult[futures.length];
                    for (int i = 0; i < futures.length; i++) {
                        try {
                            results[i] = futures[i].get();
                        } catch (Exception e) {
                            logger.error("批量路由获取结果失败", e);
                            results[i] = RouteResult.failure("批量路由异常: " + e.getMessage());
                        }
                    }
                    return results;
                });
    }
    
    /**
     * 带超时的路由决策
     */
    public RouteResult routeWithTimeout(RoutableMessage message, long timeoutMs) {
        try {
            return routeAsync(message).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("路由决策超时", e);
            metrics.incrementFailureCount();
            return RouteResult.failure("路由决策超时: " + timeoutMs + "ms");
        }
    }
    
    /**
     * 注册集群信息
     */
    public void registerCluster(ClusterInfo clusterInfo) {
        globalRouter.registerCluster(clusterInfo);
        logger.info("注册集群到路由引擎: {}", clusterInfo.getClusterId());
    }
    
    /**
     * 注册Broker信息
     */
    public void registerBroker(BrokerInfo brokerInfo) {
        clusterRouter.registerBroker(brokerInfo);
        logger.info("注册Broker到路由引擎: {}", brokerInfo.getBrokerId());
    }
    
    /**
     * 更新集群健康状态
     */
    public void updateClusterHealth(String clusterId, boolean healthy) {
        globalRouter.updateClusterHealth(clusterId, healthy);
    }
    
    /**
     * 更新Broker健康状态
     */
    public void updateBrokerHealth(String brokerId, boolean healthy) {
        clusterRouter.updateBrokerHealth(brokerId, healthy);
    }
    
    /**
     * 更新Broker负载信息
     */
    public void updateBrokerLoad(String brokerId, double cpuUsage, double memoryUsage, 
                                double diskUsage, int activeConnections) {
        clusterRouter.updateBrokerLoad(brokerId, cpuUsage, memoryUsage, diskUsage, activeConnections);
    }
    
    /**
     * 获取路由统计信息
     */
    public RoutingMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * 获取路由引擎状态
     */
    public String getEngineStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("Smart Routing Engine Status:\n");
        sb.append("Clusters: ").append(globalRouter.getAllClusters().size()).append("\n");
        sb.append("Brokers: ").append(clusterRouter.getAllBrokers().size()).append("\n");
        sb.append("Metrics: ").append(metrics.toString()).append("\n");
        return sb.toString();
    }
    
    /**
     * 关闭路由引擎
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("智能路由引擎已关闭");
    }
    
    /**
     * 路由统计指标
     */
    public static class RoutingMetrics {
        private long successCount = 0;
        private long failureCount = 0;
        private long totalRouteTime = 0;
        private long maxRouteTime = 0;
        private long minRouteTime = Long.MAX_VALUE;
        
        public synchronized void incrementSuccessCount() {
            successCount++;
        }
        
        public synchronized void incrementFailureCount() {
            failureCount++;
        }
        
        public synchronized void updateRouteTime(long routeTime) {
            totalRouteTime += routeTime;
            maxRouteTime = Math.max(maxRouteTime, routeTime);
            minRouteTime = Math.min(minRouteTime, routeTime);
        }
        
        public synchronized double getSuccessRate() {
            long total = successCount + failureCount;
            return total > 0 ? (double) successCount / total : 0.0;
        }
        
        public synchronized double getAvgRouteTime() {
            return successCount > 0 ? (double) totalRouteTime / successCount : 0.0;
        }
        
        public synchronized void reset() {
            successCount = 0;
            failureCount = 0;
            totalRouteTime = 0;
            maxRouteTime = 0;
            minRouteTime = Long.MAX_VALUE;
        }
        
        // Getters
        public long getSuccessCount() { return successCount; }
        public long getFailureCount() { return failureCount; }
        public long getTotalRouteTime() { return totalRouteTime; }
        public long getMaxRouteTime() { return maxRouteTime; }
        public long getMinRouteTime() { return minRouteTime == Long.MAX_VALUE ? 0 : minRouteTime; }
        
        @Override
        public String toString() {
            return String.format("RoutingMetrics{success=%d, failure=%d, successRate=%.2f%%, avgTime=%.1fms, maxTime=%dms}", 
                               successCount, failureCount, getSuccessRate() * 100, getAvgRouteTime(), maxRouteTime);
        }
    }
}
