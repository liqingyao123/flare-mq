package com.flare.mq.nameserver.routing.global;

import com.flare.mq.nameserver.routing.RouteConstants;
import com.flare.mq.nameserver.routing.model.ClusterInfo;
import com.flare.mq.nameserver.routing.model.RoutableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 全局负载均衡器
 * 
 * 在集群级别进行负载均衡
 * 
 * @author FlareMQ Team
 */
public class GlobalLoadBalancer {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalLoadBalancer.class);
    
    /**
     * 轮询计数器
     */
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    /**
     * 当前负载均衡策略
     */
    private String strategy = RouteConstants.LOAD_BALANCE_LEAST_ACTIVE;
    
    /**
     * 选择最优集群
     */
    public ClusterInfo selectCluster(List<ClusterInfo> clusters, RoutableMessage message) {
        if (clusters == null || clusters.isEmpty()) {
            return null;
        }
        
        if (clusters.size() == 1) {
            return clusters.get(0);
        }
        
        // 过滤健康的集群
        List<ClusterInfo> healthyClusters = clusters.stream()
                .filter(ClusterInfo::isHealthy)
                .collect(java.util.stream.Collectors.toList());
        
        if (healthyClusters.isEmpty()) {
            logger.warn("没有健康的集群可用");
            return null;
        }
        
        if (healthyClusters.size() == 1) {
            return healthyClusters.get(0);
        }
        
        // 根据策略选择集群
        switch (strategy) {
            case RouteConstants.LOAD_BALANCE_ROUND_ROBIN:
                return selectByRoundRobin(healthyClusters);
            case RouteConstants.LOAD_BALANCE_RANDOM:
                return selectByRandom(healthyClusters);
            case RouteConstants.LOAD_BALANCE_CONSISTENT_HASH:
                return selectByConsistentHash(healthyClusters, message);
            case RouteConstants.LOAD_BALANCE_LEAST_ACTIVE:
                return selectByLeastActive(healthyClusters);
            case RouteConstants.LOAD_BALANCE_WEIGHTED_ROUND_ROBIN:
                return selectByWeightedRoundRobin(healthyClusters);
            default:
                logger.warn("未知的负载均衡策略: {}, 使用默认策略", strategy);
                return selectByLeastActive(healthyClusters);
        }
    }
    
    /**
     * 轮询选择
     */
    private ClusterInfo selectByRoundRobin(List<ClusterInfo> clusters) {
        int index = roundRobinCounter.getAndIncrement() % clusters.size();
        ClusterInfo selected = clusters.get(index);
        logger.debug("轮询选择集群: {}", selected.getClusterId());
        return selected;
    }
    
    /**
     * 随机选择
     */
    private ClusterInfo selectByRandom(List<ClusterInfo> clusters) {
        int index = ThreadLocalRandom.current().nextInt(clusters.size());
        ClusterInfo selected = clusters.get(index);
        logger.debug("随机选择集群: {}", selected.getClusterId());
        return selected;
    }
    
    /**
     * 一致性哈希选择
     */
    private ClusterInfo selectByConsistentHash(List<ClusterInfo> clusters, RoutableMessage message) {
        ConsistentHashRouter hashRouter = new ConsistentHashRouter();
        ClusterInfo selected = hashRouter.selectCluster(clusters, message);
        logger.debug("一致性哈希选择集群: {}", selected != null ? selected.getClusterId() : "null");
        return selected;
    }
    
    /**
     * 最少活跃连接选择
     */
    private ClusterInfo selectByLeastActive(List<ClusterInfo> clusters) {
        ClusterInfo selected = null;
        double minLoadFactor = Double.MAX_VALUE;
        
        for (ClusterInfo cluster : clusters) {
            double loadFactor = cluster.getLoadFactor();
            if (loadFactor < minLoadFactor) {
                minLoadFactor = loadFactor;
                selected = cluster;
            }
        }
        
        logger.debug("最少活跃连接选择集群: {}, 负载因子: {}", 
                    selected != null ? selected.getClusterId() : "null", minLoadFactor);
        return selected;
    }
    
    /**
     * 加权轮询选择
     */
    private ClusterInfo selectByWeightedRoundRobin(List<ClusterInfo> clusters) {
        // 计算总权重（基于负载因子的倒数）
        double totalWeight = 0;
        for (ClusterInfo cluster : clusters) {
            // 负载因子越低，权重越高
            double weight = 1.0 / (cluster.getLoadFactor() + 0.1); // 避免除零
            totalWeight += weight;
        }
        
        // 生成随机数
        double random = ThreadLocalRandom.current().nextDouble() * totalWeight;
        
        // 选择集群
        double currentWeight = 0;
        for (ClusterInfo cluster : clusters) {
            double weight = 1.0 / (cluster.getLoadFactor() + 0.1);
            currentWeight += weight;
            if (random <= currentWeight) {
                logger.debug("加权轮询选择集群: {}, 权重: {}", cluster.getClusterId(), weight);
                return cluster;
            }
        }
        
        // 兜底返回第一个
        ClusterInfo fallback = clusters.get(0);
        logger.debug("加权轮询兜底选择集群: {}", fallback.getClusterId());
        return fallback;
    }
    
    /**
     * 根据地理位置选择最近的集群
     */
    public ClusterInfo selectByRegion(List<ClusterInfo> clusters, String targetRegion) {
        if (targetRegion == null || targetRegion.isEmpty()) {
            return selectByLeastActive(clusters);
        }
        
        // 优先选择同地区的集群
        for (ClusterInfo cluster : clusters) {
            if (cluster.isHealthy() && targetRegion.equals(cluster.getRegion())) {
                logger.debug("地理位置选择集群: {}, 地区: {}", cluster.getClusterId(), targetRegion);
                return cluster;
            }
        }
        
        // 如果没有同地区的集群，则使用负载均衡策略
        logger.debug("未找到同地区集群，使用负载均衡策略");
        return selectByLeastActive(clusters);
    }
    
    /**
     * 设置负载均衡策略
     */
    public void setStrategy(String strategy) {
        this.strategy = strategy;
        logger.info("设置负载均衡策略: {}", strategy);
    }
    
    /**
     * 获取当前负载均衡策略
     */
    public String getStrategy() {
        return strategy;
    }
    
    /**
     * 获取负载均衡统计信息
     */
    public String getLoadBalanceStats() {
        return String.format("LoadBalancer Stats: strategy=%s, roundRobinCounter=%d", 
                           strategy, roundRobinCounter.get());
    }
}
