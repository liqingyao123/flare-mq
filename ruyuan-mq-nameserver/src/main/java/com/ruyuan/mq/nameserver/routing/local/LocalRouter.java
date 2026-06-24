package com.ruyuan.mq.nameserver.routing.local;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import com.ruyuan.mq.nameserver.routing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地路由器
 * 
 * 负责单机内队列选择和优化
 * 
 * @author RuYuan MQ Team
 */
public class LocalRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(LocalRouter.class);
    
    /**
     * 队列选择器
     */
    private final QueueSelector queueSelector;
    
    /**
     * 本地负载均衡器
     */
    private final LocalLoadBalancer localBalancer;
    
    /**
     * 队列负载统计
     */
    private final ConcurrentHashMap<String, QueueLoadInfo> queueLoadStats = new ConcurrentHashMap<>();
    
    public LocalRouter() {
        this.queueSelector = new QueueSelector();
        this.localBalancer = new LocalLoadBalancer();
    }
    
    /**
     * 执行本地路由决策
     */
    public RouteResult route(RoutableMessage message, ClusterRoute clusterRoute) {
        long startTime = System.currentTimeMillis();
        
        try {
            logger.debug("开始本地路由决策，Broker: {}", 
                        clusterRoute.getBroker() != null ? clusterRoute.getBroker().getBrokerId() : "null");
            
            // 1. 基于消息Key选择队列
            int queueId = queueSelector.selectQueue(message, clusterRoute.getQueueCount());
            
            // 2. 本地负载均衡优化
            QueueLoadInfo currentLoad = getCurrentLoad(clusterRoute.getBroker().getBrokerId());
            int optimizedQueueId = localBalancer.optimize(queueId, currentLoad, clusterRoute.getQueueCount());
            
            // 3. 更新队列负载统计
            updateQueueLoad(clusterRoute.getBroker().getBrokerId(), optimizedQueueId);
            
            // 4. 构建最终路由结果
            RouteResult result = new RouteResult(clusterRoute.getBroker(), optimizedQueueId, 
                                               clusterRoute.getGlobalRoute(), clusterRoute);
            result.setQueueSelectStrategy(queueSelector.getStrategy());
            
            long routeTime = System.currentTimeMillis() - startTime;
            result.setTotalRouteTime(routeTime);
            
            logger.debug("本地路由决策完成，耗时: {}ms, 选择队列: {}", routeTime, optimizedQueueId);
            
            return result;
            
        } catch (Exception e) {
            logger.error("本地路由决策失败", e);
            return RouteResult.failure("本地路由决策异常: " + e.getMessage());
        }
    }
    
    /**
     * 获取当前负载信息
     */
    private QueueLoadInfo getCurrentLoad(String brokerId) {
        return queueLoadStats.computeIfAbsent(brokerId, k -> new QueueLoadInfo(brokerId));
    }
    
    /**
     * 更新队列负载统计
     */
    private void updateQueueLoad(String brokerId, int queueId) {
        QueueLoadInfo loadInfo = queueLoadStats.get(brokerId);
        if (loadInfo != null) {
            loadInfo.incrementQueueLoad(queueId);
        }
    }
    
    /**
     * 获取队列负载统计
     */
    public QueueLoadInfo getQueueLoadInfo(String brokerId) {
        return queueLoadStats.get(brokerId);
    }
    
    /**
     * 重置队列负载统计
     */
    public void resetQueueLoadStats(String brokerId) {
        QueueLoadInfo loadInfo = queueLoadStats.get(brokerId);
        if (loadInfo != null) {
            loadInfo.reset();
            logger.info("重置Broker队列负载统计: {}", brokerId);
        }
    }
    
    /**
     * 获取所有队列负载统计
     */
    public String getAllQueueLoadStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("Queue Load Statistics:\n");
        
        for (QueueLoadInfo loadInfo : queueLoadStats.values()) {
            sb.append(loadInfo.toString()).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * 设置队列选择策略
     */
    public void setQueueSelectStrategy(String strategy) {
        queueSelector.setStrategy(strategy);
    }
    
    /**
     * 获取队列选择策略
     */
    public String getQueueSelectStrategy() {
        return queueSelector.getStrategy();
    }
    
    /**
     * 队列负载信息内部类
     */
    public static class QueueLoadInfo {
        private final String brokerId;
        private final ConcurrentHashMap<Integer, Long> queueMessageCount = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, Long> queueLastAccessTime = new ConcurrentHashMap<>();
        private long totalMessages = 0;
        private long lastUpdateTime = System.currentTimeMillis();
        
        public QueueLoadInfo(String brokerId) {
            this.brokerId = brokerId;
        }
        
        /**
         * 增加队列负载
         */
        public void incrementQueueLoad(int queueId) {
            queueMessageCount.merge(queueId, 1L, Long::sum);
            queueLastAccessTime.put(queueId, System.currentTimeMillis());
            totalMessages++;
            lastUpdateTime = System.currentTimeMillis();
        }
        
        /**
         * 获取队列消息数量
         */
        public long getQueueMessageCount(int queueId) {
            return queueMessageCount.getOrDefault(queueId, 0L);
        }
        
        /**
         * 获取队列最后访问时间
         */
        public long getQueueLastAccessTime(int queueId) {
            return queueLastAccessTime.getOrDefault(queueId, 0L);
        }
        
        /**
         * 获取最少负载的队列
         */
        public int getLeastLoadedQueue(int maxQueueCount) {
            int leastLoadedQueue = 0;
            long minLoad = Long.MAX_VALUE;
            
            for (int i = 0; i < maxQueueCount; i++) {
                long load = getQueueMessageCount(i);
                if (load < minLoad) {
                    minLoad = load;
                    leastLoadedQueue = i;
                }
            }
            
            return leastLoadedQueue;
        }
        
        /**
         * 计算队列负载均衡度
         */
        public double calculateLoadBalance(int maxQueueCount) {
            if (maxQueueCount <= 1 || totalMessages == 0) {
                return 1.0; // 完全均衡
            }
            
            double avgLoad = (double) totalMessages / maxQueueCount;
            double variance = 0.0;
            
            for (int i = 0; i < maxQueueCount; i++) {
                long queueLoad = getQueueMessageCount(i);
                variance += Math.pow(queueLoad - avgLoad, 2);
            }
            
            double stdDev = Math.sqrt(variance / maxQueueCount);
            
            // 负载均衡度 = 1 - (标准差 / 平均负载)
            return avgLoad > 0 ? Math.max(0, 1 - (stdDev / avgLoad)) : 1.0;
        }
        
        /**
         * 重置统计信息
         */
        public void reset() {
            queueMessageCount.clear();
            queueLastAccessTime.clear();
            totalMessages = 0;
            lastUpdateTime = System.currentTimeMillis();
        }
        
        // Getters
        public String getBrokerId() { return brokerId; }
        public long getTotalMessages() { return totalMessages; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        
        @Override
        public String toString() {
            return String.format("QueueLoadInfo{brokerId='%s', totalMessages=%d, queueCount=%d, lastUpdate=%d}", 
                               brokerId, totalMessages, queueMessageCount.size(), lastUpdateTime);
        }
    }
}
