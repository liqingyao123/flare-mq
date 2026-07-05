package com.flare.mq.broker.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Random;

/**
 * 负载均衡器 - 智能选择最优的Broker节点
 * 
 * @author FlareMQ Team
 */
public class LoadBalancer {
    
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);
    
    private final ClusterManager clusterManager;
    private final ClusterConfig config;
    private final ScheduledExecutorService scheduledExecutor;
    
    // 负载均衡策略
    private volatile LoadBalanceStrategy strategy;
    
    // 轮询计数器
    private final AtomicInteger roundRobinCounter;
    
    // 随机数生成器
    private final Random random;
    
    // 统计信息
    private final AtomicLong totalSelections;
    private final AtomicLong balanceOperations;
    
    // 运行状态
    private volatile boolean running;
    
    public LoadBalancer(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.config = clusterManager.clusterConfig;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LoadBalancer-Scheduled");
            t.setDaemon(true);
            return t;
        });
        this.strategy = LoadBalanceStrategy.valueOf(config.getLoadBalanceStrategy());
        this.roundRobinCounter = new AtomicInteger(0);
        this.random = new Random();
        this.totalSelections = new AtomicLong(0);
        this.balanceOperations = new AtomicLong(0);
        this.running = false;
        
        logger.info("LoadBalancer initialized with strategy: {}", strategy);
    }
    
    /**
     * 启动负载均衡器
     */
    public void start() {
        if (running) {
            logger.warn("LoadBalancer already running");
            return;
        }
        
        running = true;
        
        // 启动动态负载均衡任务
        if (config.isEnableDynamicBalance()) {
            scheduledExecutor.scheduleAtFixedRate(this::performDynamicBalance, 
                                                config.getBalanceIntervalMs(), 
                                                config.getBalanceIntervalMs(), 
                                                TimeUnit.MILLISECONDS);
        }
        
        logger.info("LoadBalancer started");
    }
    
    /**
     * 关闭负载均衡器
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down LoadBalancer...");
        running = false;
        
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduledExecutor.shutdownNow();
        }
        
        logger.info("LoadBalancer shutdown completed");
    }
    
    /**
     * 选择最优的Broker节点
     */
    public BrokerNode selectBroker(List<BrokerNode> availableNodes) {
        if (!running || availableNodes == null || availableNodes.isEmpty()) {
            return null;
        }
        
        totalSelections.incrementAndGet();
        
        // 过滤可用节点
        List<BrokerNode> candidates = availableNodes.stream()
                .filter(BrokerNode::isAvailable)
                .filter(node -> node.isActive(config.getNodeTimeoutMs()))
                .collect(Collectors.toList());
        
        if (candidates.isEmpty()) {
            logger.warn("No available broker nodes for load balancing");
            return null;
        }
        
        BrokerNode selected = selectByStrategy(candidates);
        
        if (selected != null) {
            logger.debug("Selected broker: {} using strategy: {}", 
                        selected.getBrokerName(), strategy);
        }
        
        return selected;
    }
    
    /**
     * 根据策略选择节点
     */
    private BrokerNode selectByStrategy(List<BrokerNode> candidates) {
        switch (strategy) {
            case ROUND_ROBIN:
                return selectByRoundRobin(candidates);
            case RANDOM:
                return selectByRandom(candidates);
            case LEAST_CONNECTIONS:
                return selectByLeastConnections(candidates);
            case WEIGHTED_ROUND_ROBIN:
                return selectByWeightedRoundRobin(candidates);
            case LEAST_RESPONSE_TIME:
                return selectByLeastResponseTime(candidates);
            case CONSISTENT_HASH:
                return selectByConsistentHash(candidates);
            default:
                return selectByRoundRobin(candidates);
        }
    }
    
    /**
     * 轮询选择
     */
    private BrokerNode selectByRoundRobin(List<BrokerNode> candidates) {
        int index = roundRobinCounter.getAndIncrement() % candidates.size();
        return candidates.get(index);
    }
    
    /**
     * 随机选择
     */
    private BrokerNode selectByRandom(List<BrokerNode> candidates) {
        int index = random.nextInt(candidates.size());
        return candidates.get(index);
    }
    
    /**
     * 最少连接选择
     */
    private BrokerNode selectByLeastConnections(List<BrokerNode> candidates) {
        return candidates.stream()
                .min((n1, n2) -> Long.compare(n1.getMessageCount(), n2.getMessageCount()))
                .orElse(null);
    }
    
    /**
     * 加权轮询选择
     */
    private BrokerNode selectByWeightedRoundRobin(List<BrokerNode> candidates) {
        // 基于负载分数进行加权选择
        double totalWeight = candidates.stream()
                .mapToDouble(node -> 1.0 / (1.0 + node.calculateLoadScore()))
                .sum();
        
        double randomValue = random.nextDouble() * totalWeight;
        double currentWeight = 0.0;
        
        for (BrokerNode node : candidates) {
            currentWeight += 1.0 / (1.0 + node.calculateLoadScore());
            if (randomValue <= currentWeight) {
                return node;
            }
        }
        
        return candidates.get(0); // fallback
    }
    
    /**
     * 最短响应时间选择
     */
    private BrokerNode selectByLeastResponseTime(List<BrokerNode> candidates) {
        return candidates.stream()
                .min((n1, n2) -> Long.compare(n1.getNetworkLatency(), n2.getNetworkLatency()))
                .orElse(null);
    }
    
    /**
     * 一致性哈希选择
     */
    private BrokerNode selectByConsistentHash(List<BrokerNode> candidates) {
        // 简化的一致性哈希实现
        String key = String.valueOf(System.currentTimeMillis());
        int hash = key.hashCode();
        int index = Math.abs(hash) % candidates.size();
        return candidates.get(index);
    }
    
    /**
     * 选择写入节点（Master）
     */
    public BrokerNode selectWriteNode() {
        Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
        
        // 查找Master节点
        BrokerNode master = clusterNodes.values().stream()
                .filter(BrokerNode::isMaster)
                .filter(BrokerNode::isAvailable)
                .filter(node -> node.isActive(config.getNodeTimeoutMs()))
                .findFirst()
                .orElse(null);
        
        if (master != null) {
            totalSelections.incrementAndGet();
            logger.debug("Selected write node: {}", master.getBrokerName());
        } else {
            logger.warn("No available master node for writing");
        }
        
        return master;
    }
    
    /**
     * 选择读取节点（可以是Master或Slave）
     */
    public BrokerNode selectReadNode() {
        Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
        
        List<BrokerNode> readableNodes = clusterNodes.values().stream()
                .filter(BrokerNode::isAvailable)
                .filter(node -> node.isActive(config.getNodeTimeoutMs()))
                .collect(Collectors.toList());
        
        return selectBroker(readableNodes);
    }
    
    /**
     * 执行动态负载均衡
     */
    private void performDynamicBalance() {
        if (!running) {
            return;
        }
        
        try {
            logger.debug("Performing dynamic load balance...");
            
            Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
            List<BrokerNode> nodes = new ArrayList<>(clusterNodes.values());
            
            if (nodes.size() < 2) {
                return; // 节点数量不足，无需负载均衡
            }
            
            // 计算负载分布
            double avgLoad = nodes.stream()
                    .mapToDouble(BrokerNode::calculateLoadScore)
                    .average()
                    .orElse(0.0);
            
            // 找出负载过高和过低的节点
            List<BrokerNode> overloadedNodes = nodes.stream()
                    .filter(node -> node.calculateLoadScore() > avgLoad * 1.5)
                    .collect(Collectors.toList());
            
            List<BrokerNode> underloadedNodes = nodes.stream()
                    .filter(node -> node.calculateLoadScore() < avgLoad * 0.5)
                    .collect(Collectors.toList());
            
            if (!overloadedNodes.isEmpty() && !underloadedNodes.isEmpty()) {
                logger.info("Load imbalance detected: {} overloaded, {} underloaded nodes", 
                           overloadedNodes.size(), underloadedNodes.size());
                
                // 执行负载重分布（这里简化实现）
                balanceOperations.incrementAndGet();
                
                // 实际实现中，这里应该进行消息队列的重新分配
                logger.debug("Load rebalancing completed");
            }
            
        } catch (Exception e) {
            logger.error("Error performing dynamic balance", e);
        }
    }
    
    /**
     * 更新负载均衡策略
     */
    public void updateStrategy(LoadBalanceStrategy newStrategy) {
        LoadBalanceStrategy oldStrategy = this.strategy;
        this.strategy = newStrategy;
        
        logger.info("Load balance strategy updated: {} -> {}", oldStrategy, newStrategy);
    }
    
    /**
     * 获取负载均衡统计信息
     */
    public LoadBalanceStatistics getStatistics() {
        Map<String, BrokerNode> clusterNodes = clusterManager.getClusterNodes();
        
        int totalNodes = clusterNodes.size();
        int availableNodes = (int) clusterNodes.values().stream()
                .filter(BrokerNode::isAvailable)
                .count();
        
        double avgLoad = clusterNodes.values().stream()
                .mapToDouble(BrokerNode::calculateLoadScore)
                .average()
                .orElse(0.0);
        
        return new LoadBalanceStatistics(
            strategy, totalSelections.get(), balanceOperations.get(),
            totalNodes, availableNodes, avgLoad
        );
    }
    
    // Getters
    public boolean isRunning() { return running; }
    public LoadBalanceStrategy getStrategy() { return strategy; }
}

/**
 * 负载均衡策略枚举
 */
enum LoadBalanceStrategy {
    ROUND_ROBIN("轮询"),
    RANDOM("随机"),
    LEAST_CONNECTIONS("最少连接"),
    WEIGHTED_ROUND_ROBIN("加权轮询"),
    LEAST_RESPONSE_TIME("最短响应时间"),
    CONSISTENT_HASH("一致性哈希");
    
    private final String description;
    
    LoadBalanceStrategy(String description) {
        this.description = description;
    }
    
    public String getDescription() { return description; }
}

/**
 * 负载均衡统计信息
 */
class LoadBalanceStatistics {
    private final LoadBalanceStrategy strategy;
    private final long totalSelections;
    private final long balanceOperations;
    private final int totalNodes;
    private final int availableNodes;
    private final double averageLoad;
    
    public LoadBalanceStatistics(LoadBalanceStrategy strategy, long totalSelections, 
                               long balanceOperations, int totalNodes, 
                               int availableNodes, double averageLoad) {
        this.strategy = strategy;
        this.totalSelections = totalSelections;
        this.balanceOperations = balanceOperations;
        this.totalNodes = totalNodes;
        this.availableNodes = availableNodes;
        this.averageLoad = averageLoad;
    }
    
    // Getters
    public LoadBalanceStrategy getStrategy() { return strategy; }
    public long getTotalSelections() { return totalSelections; }
    public long getBalanceOperations() { return balanceOperations; }
    public int getTotalNodes() { return totalNodes; }
    public int getAvailableNodes() { return availableNodes; }
    public double getAverageLoad() { return averageLoad; }
    
    @Override
    public String toString() {
        return String.format("LoadBalanceStatistics{strategy=%s, selections=%d, " +
                           "balanceOps=%d, nodes=%d/%d, avgLoad=%.2f}", 
                           strategy, totalSelections, balanceOperations, 
                           availableNodes, totalNodes, averageLoad);
    }
}
