package com.ruyuan.mq.nameserver.routing.cluster;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import com.ruyuan.mq.nameserver.routing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 集群路由器
 * 
 * 负责集群内的智能负载均衡和故障转移
 * 
 * @author RuYuan MQ Team
 */
public class ClusterRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterRouter.class);
    
    /**
     * Broker信息缓存
     */
    private final ConcurrentHashMap<String, BrokerInfo> brokerCache = new ConcurrentHashMap<>();
    
    /**
     * 集群到Broker的映射关系
     */
    private final ConcurrentHashMap<String, List<String>> clusterBrokerMapping = new ConcurrentHashMap<>();
    
    /**
     * Broker选择器
     */
    private final BrokerSelector brokerSelector;
    
    /**
     * 故障转移管理器
     */
    private final FailoverManager failoverManager;
    
    public ClusterRouter() {
        this.brokerSelector = new BrokerSelector();
        this.failoverManager = new FailoverManager();
        
        // 初始化默认Broker配置
        initializeDefaultBrokers();
    }
    
    /**
     * 执行集群路由决策
     */
    public ClusterRoute route(RoutableMessage message, GlobalRoute globalRoute) {
        long startTime = System.currentTimeMillis();
        
        try {
            logger.debug("开始集群路由决策，集群: {}", 
                        globalRoute.getCluster() != null ? globalRoute.getCluster().getClusterId() : "null");
            
            // 1. 获取集群内可用的Broker列表
            List<BrokerInfo> availableBrokers = getAvailableBrokers(globalRoute.getCluster());
            if (availableBrokers.isEmpty()) {
                logger.warn("集群内没有可用的Broker: {}", globalRoute.getCluster().getClusterId());
                return createFailedRoute("集群内没有可用的Broker");
            }
            
            // 2. 智能Broker选择
            BrokerInfo selectedBroker = brokerSelector.select(availableBrokers, message);
            if (selectedBroker == null) {
                logger.warn("Broker选择器未能选择到合适的Broker");
                return createFailedRoute("Broker选择失败");
            }
            
            // 3. 故障转移检查
            if (!failoverManager.isHealthy(selectedBroker)) {
                logger.warn("选择的Broker不健康，尝试故障转移: {}", selectedBroker.getBrokerId());
                BrokerInfo backupBroker = failoverManager.selectBackup(availableBrokers, selectedBroker);
                if (backupBroker != null) {
                    selectedBroker = backupBroker;
                    logger.info("故障转移成功，选择备用Broker: {}", selectedBroker.getBrokerId());
                } else {
                    logger.error("故障转移失败，没有可用的备用Broker");
                    return createFailedRoute("故障转移失败");
                }
            }
            
            // 4. 计算队列数量
            int queueCount = calculateQueueCount(message, selectedBroker);
            
            // 5. 构建路由结果
            ClusterRoute route = new ClusterRoute(selectedBroker, queueCount, brokerSelector.getStrategy());
            route.setFailover(failoverManager.isFailoverBroker(selectedBroker));
            route.setRouteReason(String.format("负载均衡策略: %s, 健康检查: %s", 
                                             brokerSelector.getStrategy(), 
                                             failoverManager.isHealthy(selectedBroker) ? "通过" : "故障转移"));
            
            long routeTime = System.currentTimeMillis() - startTime;
            logger.debug("集群路由决策完成，耗时: {}ms, 选择Broker: {}", routeTime, selectedBroker.getBrokerId());
            
            return route;
            
        } catch (Exception e) {
            logger.error("集群路由决策失败", e);
            return createFailedRoute("路由决策异常: " + e.getMessage());
        }
    }
    
    /**
     * 获取集群内可用的Broker列表
     */
    private List<BrokerInfo> getAvailableBrokers(ClusterInfo cluster) {
        if (cluster == null) {
            return new CopyOnWriteArrayList<>();
        }
        
        String clusterId = cluster.getClusterId();
        List<String> brokerIds = clusterBrokerMapping.get(clusterId);
        
        if (brokerIds == null || brokerIds.isEmpty()) {
            return new CopyOnWriteArrayList<>();
        }
        
        return brokerIds.stream()
                .map(brokerCache::get)
                .filter(broker -> broker != null && broker.isHealthy())
                .collect(Collectors.toList());
    }
    
    /**
     * 计算队列数量
     */
    private int calculateQueueCount(RoutableMessage message, BrokerInfo broker) {
        // 基于Broker配置的队列数量
        int brokerQueueCount = broker.getQueueCount();
        if (brokerQueueCount > 0) {
            return brokerQueueCount;
        }
        
        // 基于消息类型调整队列数量
        if (message.isOrderedMessage()) {
            // 顺序消息使用较少的队列以保证顺序
            return Math.max(1, RouteConstants.DEFAULT_QUEUE_COUNT / 2);
        }
        
        if (message.isTransactionMessage()) {
            // 事务消息使用更多队列以提高并发
            return RouteConstants.DEFAULT_QUEUE_COUNT * 2;
        }
        
        return RouteConstants.DEFAULT_QUEUE_COUNT;
    }
    
    /**
     * 创建失败的路由结果
     */
    private ClusterRoute createFailedRoute(String reason) {
        ClusterRoute route = new ClusterRoute();
        route.setRouteReason("失败: " + reason);
        return route;
    }
    
    /**
     * 注册Broker信息
     */
    public void registerBroker(BrokerInfo brokerInfo) {
        brokerCache.put(brokerInfo.getBrokerId(), brokerInfo);
        
        // 更新集群映射关系
        String clusterId = brokerInfo.getClusterId();
        clusterBrokerMapping.computeIfAbsent(clusterId, k -> new CopyOnWriteArrayList<>())
                           .add(brokerInfo.getBrokerId());
        
        logger.info("注册Broker: {}", brokerInfo);
    }
    
    /**
     * 注销Broker信息
     */
    public void unregisterBroker(String brokerId) {
        BrokerInfo removed = brokerCache.remove(brokerId);
        if (removed != null) {
            // 从集群映射中移除
            String clusterId = removed.getClusterId();
            List<String> brokerIds = clusterBrokerMapping.get(clusterId);
            if (brokerIds != null) {
                brokerIds.remove(brokerId);
            }
            
            logger.info("注销Broker: {}", removed);
        }
    }
    
    /**
     * 更新Broker健康状态
     */
    public void updateBrokerHealth(String brokerId, boolean healthy) {
        BrokerInfo broker = brokerCache.get(brokerId);
        if (broker != null) {
            broker.setHealthy(healthy);
            broker.setLastHeartbeatTime(System.currentTimeMillis());
            
            // 通知故障转移管理器
            failoverManager.updateBrokerHealth(brokerId, healthy);
            
            logger.debug("更新Broker健康状态: {} -> {}", brokerId, healthy);
        }
    }
    
    /**
     * 更新Broker负载信息
     */
    public void updateBrokerLoad(String brokerId, double cpuUsage, double memoryUsage, 
                                double diskUsage, int activeConnections) {
        BrokerInfo broker = brokerCache.get(brokerId);
        if (broker != null) {
            broker.setCpuUsage(cpuUsage);
            broker.setMemoryUsage(memoryUsage);
            broker.setDiskUsage(diskUsage);
            broker.setActiveConnections(activeConnections);
            
            logger.debug("更新Broker负载信息: {}, CPU: {}%, Memory: {}%, Disk: {}%, Connections: {}", 
                        brokerId, cpuUsage, memoryUsage, diskUsage, activeConnections);
        }
    }
    
    /**
     * 获取集群内所有Broker信息
     */
    public List<BrokerInfo> getBrokersByCluster(String clusterId) {
        List<String> brokerIds = clusterBrokerMapping.get(clusterId);
        if (brokerIds == null) {
            return new CopyOnWriteArrayList<>();
        }
        
        return brokerIds.stream()
                .map(brokerCache::get)
                .filter(broker -> broker != null)
                .collect(Collectors.toList());
    }
    
    /**
     * 获取所有Broker信息
     */
    public List<BrokerInfo> getAllBrokers() {
        return new CopyOnWriteArrayList<>(brokerCache.values());
    }
    
    /**
     * 初始化默认Broker配置
     */
    private void initializeDefaultBrokers() {
        // 为默认集群创建Broker
        createDefaultBrokersForCluster("default-cluster-1", "default", 18881);
        createDefaultBrokersForCluster("order-cluster-1", "order", 18891);
        createDefaultBrokersForCluster("log-cluster-1", "log", 18901);
        
        logger.info("初始化默认Broker配置完成，Broker数量: {}", brokerCache.size());
    }
    
    /**
     * 为指定集群创建默认Broker
     */
    private void createDefaultBrokersForCluster(String clusterId, String prefix, int basePort) {
        for (int i = 1; i <= RouteConstants.DEFAULT_BROKER_COUNT; i++) {
            String brokerId = prefix + "-broker-" + i;
            String brokerName = prefix.toUpperCase() + " Broker " + i;
            
            BrokerInfo broker = new BrokerInfo(brokerId, brokerName, "localhost", basePort + i, clusterId);
            broker.setQueueCount(RouteConstants.DEFAULT_QUEUE_COUNT);
            broker.setCpuUsage(Math.random() * 50); // 模拟负载
            broker.setMemoryUsage(Math.random() * 60);
            broker.setDiskUsage(Math.random() * 40);
            broker.setActiveConnections((int) (Math.random() * 100));
            
            registerBroker(broker);
        }
    }
}
