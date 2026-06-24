package com.ruyuan.mq.nameserver.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 服务发现组件 - 提供Broker服务发现和路由查询功能
 * 
 * @author RuYuan MQ Team
 */
public class ServiceDiscovery {
    
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);
    
    private final ServiceRegistry serviceRegistry;
    
    // 缓存最近查询的路由信息
    private final ConcurrentHashMap<String, CachedRouteData> routeCache;
    private static final long CACHE_EXPIRE_TIME = 30 * 1000; // 30秒缓存过期时间
    
    public ServiceDiscovery(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.routeCache = new ConcurrentHashMap<>();
        logger.info("ServiceDiscovery initialized");
    }
    
    /**
     * 根据Topic获取路由信息
     */
    public TopicRouteData getTopicRouteData(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            logger.warn("Topic is null or empty");
            return null;
        }
        
        // 先检查缓存
        CachedRouteData cachedData = routeCache.get(topic);
        if (cachedData != null && !cachedData.isExpired()) {
            logger.debug("Return cached route data for topic: {}", topic);
            return cachedData.getRouteData();
        }
        
        // 从注册表获取路由信息
        TopicRouteData routeData = serviceRegistry.getTopicRouteData(topic);
        if (routeData != null) {
            // 填充Broker地址信息
            fillBrokerAddrs(routeData);
            
            // 更新缓存
            routeCache.put(topic, new CachedRouteData(routeData));
            
            logger.debug("Found route data for topic: {}, brokers: {}", 
                        topic, routeData.getBrokerDatas().size());
        } else {
            logger.debug("No route data found for topic: {}", topic);
        }
        
        return routeData;
    }
    
    /**
     * 获取所有可用的Broker列表
     */
    public List<BrokerData> getAllAvailableBrokers() {
        Map<String, BrokerData> allBrokers = serviceRegistry.getAllBrokerData();
        
        return allBrokers.values().stream()
                .filter(this::isBrokerAvailable)
                .collect(Collectors.toList());
    }
    
    /**
     * 根据集群名称获取Broker列表
     */
    public List<BrokerData> getBrokersByCluster(String clusterName) {
        if (clusterName == null || clusterName.trim().isEmpty()) {
            return new ArrayList<>();
        }
        
        ClusterInfo clusterInfo = serviceRegistry.getClusterInfo(clusterName);
        if (clusterInfo == null) {
            return new ArrayList<>();
        }
        
        List<BrokerData> brokers = new ArrayList<>();
        for (String brokerName : clusterInfo.getBrokerNames()) {
            BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
            if (brokerData != null && isBrokerAvailable(brokerData)) {
                brokers.add(brokerData);
            }
        }
        
        return brokers;
    }
    
    /**
     * 查找Master Broker
     */
    public BrokerData findMasterBroker(String brokerName) {
        BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
        if (brokerData != null && brokerData.getBrokerAddrs().containsKey(0L)) {
            return brokerData;
        }
        return null;
    }
    
    /**
     * 查找Slave Broker列表
     */
    public List<String> findSlaveBrokers(String brokerName) {
        BrokerData brokerData = serviceRegistry.getBrokerData(brokerName);
        if (brokerData == null) {
            return new ArrayList<>();
        }
        
        return brokerData.getBrokerAddrs().entrySet().stream()
                .filter(entry -> entry.getKey() > 0) // Slave的brokerId > 0
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }
    
    /**
     * 获取Topic的写队列信息
     */
    public List<QueueData> getWriteQueuesByTopic(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }
        
        return routeData.getQueueDatas().stream()
                .filter(qd -> qd.getWriteQueueNums() > 0)
                .collect(Collectors.toList());
    }
    
    /**
     * 获取Topic的读队列信息
     */
    public List<QueueData> getReadQueuesByTopic(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }
        
        return routeData.getQueueDatas().stream()
                .filter(qd -> qd.getReadQueueNums() > 0)
                .collect(Collectors.toList());
    }
    
    /**
     * 获取服务发现统计信息
     */
    public ServiceDiscoveryStats getStatistics() {
        Map<String, BrokerData> allBrokers = serviceRegistry.getAllBrokerData();
        
        int totalBrokers = allBrokers.size();
        int availableBrokers = (int) allBrokers.values().stream()
                .filter(this::isBrokerAvailable)
                .count();
        
        int cachedTopics = routeCache.size();
        
        return new ServiceDiscoveryStats(totalBrokers, availableBrokers, cachedTopics);
    }
    
    /**
     * 清理过期的缓存
     */
    public void cleanupExpiredCache() {
        int removedCount = 0;
        Iterator<Map.Entry<String, CachedRouteData>> iterator = routeCache.entrySet().iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<String, CachedRouteData> entry = iterator.next();
            if (entry.getValue().isExpired()) {
                iterator.remove();
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            logger.debug("Cleaned up {} expired route cache entries", removedCount);
        }
    }
    
    /**
     * 填充Broker地址信息
     */
    private void fillBrokerAddrs(TopicRouteData routeData) {
        List<BrokerData> brokerDatas = new ArrayList<>();
        
        for (QueueData queueData : routeData.getQueueDatas()) {
            BrokerData brokerData = serviceRegistry.getBrokerData(queueData.getBrokerName());
            if (brokerData != null && !brokerDatas.contains(brokerData)) {
                brokerDatas.add(brokerData);
            }
        }
        
        routeData.setBrokerDatas(brokerDatas);
    }
    
    /**
     * 检查Broker是否可用
     */
    private boolean isBrokerAvailable(BrokerData brokerData) {
        if (brokerData == null || brokerData.getBrokerAddrs().isEmpty()) {
            return false;
        }
        
        // 检查是否有Master Broker
        boolean hasMaster = brokerData.getBrokerAddrs().containsKey(0L);
        
        // 检查最后更新时间（2分钟内更新过认为是活跃的）
        long lastUpdate = brokerData.getLastUpdateTimestamp();
        boolean isActive = (System.currentTimeMillis() - lastUpdate) < 120000;
        
        return hasMaster && isActive;
    }
    
    /**
     * 缓存的路由数据
     */
    private static class CachedRouteData {
        private final TopicRouteData routeData;
        private final long cacheTime;
        
        public CachedRouteData(TopicRouteData routeData) {
            this.routeData = routeData;
            this.cacheTime = System.currentTimeMillis();
        }
        
        public TopicRouteData getRouteData() {
            return routeData;
        }
        
        public boolean isExpired() {
            return (System.currentTimeMillis() - cacheTime) > CACHE_EXPIRE_TIME;
        }
    }
    
    /**
     * 服务发现统计信息
     */
    public static class ServiceDiscoveryStats {
        private final int totalBrokers;
        private final int availableBrokers;
        private final int cachedTopics;
        
        public ServiceDiscoveryStats(int totalBrokers, int availableBrokers, int cachedTopics) {
            this.totalBrokers = totalBrokers;
            this.availableBrokers = availableBrokers;
            this.cachedTopics = cachedTopics;
        }
        
        public int getTotalBrokers() { return totalBrokers; }
        public int getAvailableBrokers() { return availableBrokers; }
        public int getCachedTopics() { return cachedTopics; }
        
        @Override
        public String toString() {
            return String.format("ServiceDiscoveryStats{totalBrokers=%d, availableBrokers=%d, cachedTopics=%d}", 
                               totalBrokers, availableBrokers, cachedTopics);
        }
    }
}
