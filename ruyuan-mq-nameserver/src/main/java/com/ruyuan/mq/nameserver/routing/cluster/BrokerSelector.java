package com.ruyuan.mq.nameserver.routing.cluster;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import com.ruyuan.mq.nameserver.routing.model.BrokerInfo;
import com.ruyuan.mq.nameserver.routing.model.RoutableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Broker选择器
 * 
 * 在集群内选择最优的Broker
 * 
 * @author RuYuan MQ Team
 */
public class BrokerSelector {
    
    private static final Logger logger = LoggerFactory.getLogger(BrokerSelector.class);
    
    /**
     * 轮询计数器
     */
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    /**
     * 当前选择策略
     */
    private String strategy = RouteConstants.LOAD_BALANCE_LEAST_ACTIVE;
    
    /**
     * 选择最优Broker
     */
    public BrokerInfo select(List<BrokerInfo> brokers, RoutableMessage message) {
        if (brokers == null || brokers.isEmpty()) {
            return null;
        }
        
        if (brokers.size() == 1) {
            return brokers.get(0);
        }
        
        // 过滤健康的Broker
        List<BrokerInfo> healthyBrokers = brokers.stream()
                .filter(BrokerInfo::isHealthy)
                .collect(java.util.stream.Collectors.toList());
        
        if (healthyBrokers.isEmpty()) {
            logger.warn("没有健康的Broker可用");
            return null;
        }
        
        if (healthyBrokers.size() == 1) {
            return healthyBrokers.get(0);
        }
        
        // 根据策略选择Broker
        switch (strategy) {
            case RouteConstants.LOAD_BALANCE_ROUND_ROBIN:
                return selectByRoundRobin(healthyBrokers);
            case RouteConstants.LOAD_BALANCE_RANDOM:
                return selectByRandom(healthyBrokers);
            case RouteConstants.LOAD_BALANCE_CONSISTENT_HASH:
                return selectByConsistentHash(healthyBrokers, message);
            case RouteConstants.LOAD_BALANCE_LEAST_ACTIVE:
                return selectByLeastActive(healthyBrokers);
            case RouteConstants.LOAD_BALANCE_WEIGHTED_ROUND_ROBIN:
                return selectByWeightedRoundRobin(healthyBrokers);
            default:
                logger.warn("未知的负载均衡策略: {}, 使用默认策略", strategy);
                return selectByLeastActive(healthyBrokers);
        }
    }
    
    /**
     * 轮询选择
     */
    private BrokerInfo selectByRoundRobin(List<BrokerInfo> brokers) {
        int index = roundRobinCounter.getAndIncrement() % brokers.size();
        BrokerInfo selected = brokers.get(index);
        logger.debug("轮询选择Broker: {}", selected.getBrokerId());
        return selected;
    }
    
    /**
     * 随机选择
     */
    private BrokerInfo selectByRandom(List<BrokerInfo> brokers) {
        int index = ThreadLocalRandom.current().nextInt(brokers.size());
        BrokerInfo selected = brokers.get(index);
        logger.debug("随机选择Broker: {}", selected.getBrokerId());
        return selected;
    }
    
    /**
     * 一致性哈希选择
     */
    private BrokerInfo selectByConsistentHash(List<BrokerInfo> brokers, RoutableMessage message) {
        // 使用消息的Key进行哈希
        String routingKey = buildRoutingKey(message);
        int hash = routingKey.hashCode();
        int index = Math.abs(hash) % brokers.size();
        
        BrokerInfo selected = brokers.get(index);
        logger.debug("一致性哈希选择Broker: {}, key: {}, hash: {}", 
                    selected.getBrokerId(), routingKey, hash);
        return selected;
    }
    
    /**
     * 最少活跃连接选择
     */
    private BrokerInfo selectByLeastActive(List<BrokerInfo> brokers) {
        BrokerInfo selected = null;
        double minLoadScore = Double.MAX_VALUE;
        
        for (BrokerInfo broker : brokers) {
            double loadScore = broker.calculateLoadScore();
            if (loadScore < minLoadScore) {
                minLoadScore = loadScore;
                selected = broker;
            }
        }
        
        logger.debug("最少活跃连接选择Broker: {}, 负载评分: {}", 
                    selected != null ? selected.getBrokerId() : "null", minLoadScore);
        return selected;
    }
    
    /**
     * 加权轮询选择
     */
    private BrokerInfo selectByWeightedRoundRobin(List<BrokerInfo> brokers) {
        // 计算总权重
        int totalWeight = 0;
        for (BrokerInfo broker : brokers) {
            totalWeight += broker.getWeight();
        }
        
        if (totalWeight <= 0) {
            // 如果没有权重配置，使用轮询
            return selectByRoundRobin(brokers);
        }
        
        // 生成随机数
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        
        // 选择Broker
        int currentWeight = 0;
        for (BrokerInfo broker : brokers) {
            currentWeight += broker.getWeight();
            if (random < currentWeight) {
                logger.debug("加权轮询选择Broker: {}, 权重: {}", broker.getBrokerId(), broker.getWeight());
                return broker;
            }
        }
        
        // 兜底返回第一个
        BrokerInfo fallback = brokers.get(0);
        logger.debug("加权轮询兜底选择Broker: {}", fallback.getBrokerId());
        return fallback;
    }
    
    /**
     * 基于消息特征选择Broker
     */
    public BrokerInfo selectByMessageCharacteristics(List<BrokerInfo> brokers, RoutableMessage message) {
        // 顺序消息优先选择负载较低的Broker
        if (message.isOrderedMessage()) {
            return selectByLeastActive(brokers);
        }
        
        // 事务消息优先选择性能较好的Broker
        if (message.isTransactionMessage()) {
            return selectByPerformance(brokers);
        }
        
        // 大消息优先选择磁盘空间充足的Broker
        if (message.getMessageSize() > 1024 * 1024) { // 1MB
            return selectByDiskSpace(brokers);
        }
        
        // 默认使用配置的策略
        return select(brokers, message);
    }
    
    /**
     * 基于性能选择Broker
     */
    private BrokerInfo selectByPerformance(List<BrokerInfo> brokers) {
        BrokerInfo selected = null;
        double bestPerformanceScore = Double.MAX_VALUE;
        
        for (BrokerInfo broker : brokers) {
            // 性能评分：CPU和内存使用率越低越好
            double performanceScore = (broker.getCpuUsage() + broker.getMemoryUsage()) / 2.0;
            if (performanceScore < bestPerformanceScore) {
                bestPerformanceScore = performanceScore;
                selected = broker;
            }
        }
        
        logger.debug("性能选择Broker: {}, 性能评分: {}", 
                    selected != null ? selected.getBrokerId() : "null", bestPerformanceScore);
        return selected;
    }
    
    /**
     * 基于磁盘空间选择Broker
     */
    private BrokerInfo selectByDiskSpace(List<BrokerInfo> brokers) {
        BrokerInfo selected = null;
        double minDiskUsage = Double.MAX_VALUE;
        
        for (BrokerInfo broker : brokers) {
            double diskUsage = broker.getDiskUsage();
            if (diskUsage < minDiskUsage) {
                minDiskUsage = diskUsage;
                selected = broker;
            }
        }
        
        logger.debug("磁盘空间选择Broker: {}, 磁盘使用率: {}%", 
                    selected != null ? selected.getBrokerId() : "null", minDiskUsage);
        return selected;
    }
    
    /**
     * 构建路由键
     */
    private String buildRoutingKey(RoutableMessage message) {
        // 优先使用消息的Key
        String keys = message.getKeys();
        if (keys != null && !keys.isEmpty()) {
            return keys;
        }
        
        // 其次使用Topic + Tags
        String topic = message.getTopic();
        String tags = message.getTags();
        
        if (tags != null && !tags.isEmpty()) {
            return topic + "#" + tags;
        }
        
        // 最后使用Topic
        return topic;
    }
    
    /**
     * 设置选择策略
     */
    public void setStrategy(String strategy) {
        this.strategy = strategy;
        logger.info("设置Broker选择策略: {}", strategy);
    }
    
    /**
     * 获取当前选择策略
     */
    public String getStrategy() {
        return strategy;
    }
    
    /**
     * 获取选择器统计信息
     */
    public String getSelectorStats() {
        return String.format("BrokerSelector Stats: strategy=%s, roundRobinCounter=%d", 
                           strategy, roundRobinCounter.get());
    }
}
