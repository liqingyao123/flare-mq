package com.ruyuan.mq.nameserver.routing.global;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import com.ruyuan.mq.nameserver.routing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 全局路由器
 * 
 * 负责跨集群的智能路由决策
 * 
 * @author RuYuan MQ Team
 */
public class GlobalRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalRouter.class);
    
    /**
     * 集群信息缓存
     */
    private final ConcurrentHashMap<String, ClusterInfo> clusterCache = new ConcurrentHashMap<>();
    
    /**
     * Topic到集群的映射关系
     */
    private final ConcurrentHashMap<String, List<String>> topicClusterMapping = new ConcurrentHashMap<>();
    
    /**
     * 一致性哈希路由器
     */
    private final ConsistentHashRouter hashRouter;
    
    /**
     * 负载均衡器
     */
    private final GlobalLoadBalancer loadBalancer;
    
    public GlobalRouter() {
        this.hashRouter = new ConsistentHashRouter();
        this.loadBalancer = new GlobalLoadBalancer();
        
        // 初始化默认集群配置
        initializeDefaultClusters();
    }
    
    /**
     * 执行全局路由决策
     */
    public GlobalRoute route(RoutableMessage message) {
        long startTime = System.currentTimeMillis();
        
        try {
            logger.debug("开始全局路由决策，Topic: {}, Tags: {}", message.getTopic(), message.getTags());
            
            // 1. 基于消息特征选择目标集群类型
            String targetClusterType = selectTargetClusterType(message);
            logger.debug("选择的集群类型: {}", targetClusterType);
            
            // 2. 获取该类型的可用集群列表
            List<ClusterInfo> availableClusters = getAvailableClusters(targetClusterType);
            if (availableClusters.isEmpty()) {
                logger.warn("没有找到可用的集群，集群类型: {}", targetClusterType);
                return createFailedRoute("没有可用的集群: " + targetClusterType);
            }
            
            // 3. 考虑地理位置和负载情况选择最优集群
            ClusterInfo selectedCluster = loadBalancer.selectCluster(availableClusters, message);
            if (selectedCluster == null) {
                logger.warn("负载均衡器未能选择到合适的集群");
                return createFailedRoute("负载均衡失败");
            }
            
            // 4. 计算路由优先级
            int priority = calculatePriority(message);
            
            // 5. 构建路由结果
            String routeReason = String.format("集群类型: %s, 负载均衡策略: %s", 
                                             targetClusterType, loadBalancer.getStrategy());
            
            GlobalRoute route = new GlobalRoute(selectedCluster, priority, routeReason);
            
            long routeTime = System.currentTimeMillis() - startTime;
            logger.debug("全局路由决策完成，耗时: {}ms, 选择集群: {}", routeTime, selectedCluster.getClusterId());
            
            return route;
            
        } catch (Exception e) {
            logger.error("全局路由决策失败", e);
            return createFailedRoute("路由决策异常: " + e.getMessage());
        }
    }
    
    /**
     * 基于消息特征选择目标集群类型
     */
    private String selectTargetClusterType(RoutableMessage message) {
        String topic = message.getTopic();
        
        // 基于Topic前缀进行集群选择
        if (topic.startsWith("order.")) {
            return RouteConstants.CLUSTER_TYPE_ORDER;
        } else if (topic.startsWith("log.") || topic.startsWith("audit.")) {
            return RouteConstants.CLUSTER_TYPE_LOG;
        }
        
        // 基于消息类型选择
        String messageType = message.getMessageType();
        if ("order".equals(messageType)) {
            return RouteConstants.CLUSTER_TYPE_ORDER;
        } else if ("log".equals(messageType) || "audit".equals(messageType)) {
            return RouteConstants.CLUSTER_TYPE_LOG;
        }
        
        // 基于业务优先级选择
        int priority = message.getBusinessPriority();
        if (priority <= 2) { // 高优先级消息
            return RouteConstants.CLUSTER_TYPE_ORDER; // 使用性能更好的订单集群
        }
        
        return RouteConstants.CLUSTER_TYPE_DEFAULT;
    }
    
    /**
     * 获取指定类型的可用集群列表
     */
    private List<ClusterInfo> getAvailableClusters(String clusterType) {
        List<ClusterInfo> availableClusters = new CopyOnWriteArrayList<>();
        
        for (ClusterInfo cluster : clusterCache.values()) {
            if (clusterType.equals(cluster.getClusterType()) && cluster.isHealthy()) {
                availableClusters.add(cluster);
            }
        }
        
        return availableClusters;
    }
    
    /**
     * 计算消息的路由优先级
     */
    private int calculatePriority(RoutableMessage message) {
        int businessPriority = message.getBusinessPriority();
        
        // 事务消息和顺序消息优先级更高
        if (message.isTransactionMessage() || message.isOrderedMessage()) {
            return Math.max(1, businessPriority - 1);
        }
        
        // 大消息优先级稍低
        if (message.getMessageSize() > 1024 * 1024) { // 1MB
            return Math.min(3, businessPriority + 1);
        }
        
        return businessPriority;
    }
    
    /**
     * 创建失败的路由结果
     */
    private GlobalRoute createFailedRoute(String reason) {
        GlobalRoute route = new GlobalRoute();
        route.setRouteReason("失败: " + reason);
        return route;
    }
    
    /**
     * 注册集群信息
     */
    public void registerCluster(ClusterInfo clusterInfo) {
        clusterCache.put(clusterInfo.getClusterId(), clusterInfo);
        logger.info("注册集群: {}", clusterInfo);
    }
    
    /**
     * 注销集群信息
     */
    public void unregisterCluster(String clusterId) {
        ClusterInfo removed = clusterCache.remove(clusterId);
        if (removed != null) {
            logger.info("注销集群: {}", removed);
        }
    }
    
    /**
     * 更新集群健康状态
     */
    public void updateClusterHealth(String clusterId, boolean healthy) {
        ClusterInfo cluster = clusterCache.get(clusterId);
        if (cluster != null) {
            cluster.setHealthy(healthy);
            cluster.setLastUpdateTime(System.currentTimeMillis());
            logger.debug("更新集群健康状态: {} -> {}", clusterId, healthy);
        }
    }
    
    /**
     * 获取所有集群信息
     */
    public List<ClusterInfo> getAllClusters() {
        return new CopyOnWriteArrayList<>(clusterCache.values());
    }
    
    /**
     * 初始化默认集群配置
     */
    private void initializeDefaultClusters() {
        // 创建默认集群
        ClusterInfo defaultCluster = new ClusterInfo("default-cluster-1", "默认集群", 
                                                    RouteConstants.CLUSTER_TYPE_DEFAULT, "default");
        defaultCluster.setLoadFactor(0.5);
        registerCluster(defaultCluster);
        
        // 创建订单集群
        ClusterInfo orderCluster = new ClusterInfo("order-cluster-1", "订单集群", 
                                                  RouteConstants.CLUSTER_TYPE_ORDER, "default");
        orderCluster.setLoadFactor(0.3);
        registerCluster(orderCluster);
        
        // 创建日志集群
        ClusterInfo logCluster = new ClusterInfo("log-cluster-1", "日志集群", 
                                                RouteConstants.CLUSTER_TYPE_LOG, "default");
        logCluster.setLoadFactor(0.7);
        registerCluster(logCluster);
        
        logger.info("初始化默认集群配置完成，集群数量: {}", clusterCache.size());
    }
}
