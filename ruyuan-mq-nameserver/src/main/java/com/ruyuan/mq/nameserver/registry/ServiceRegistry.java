package com.ruyuan.mq.nameserver.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * 服务注册组件 - 管理Broker服务的注册和注销
 * 
 * @author RuYuan MQ Team
 */
public class ServiceRegistry {
    
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);
    
    // Broker信息存储 - Key: brokerName, Value: BrokerData
    private final ConcurrentHashMap<String, BrokerData> brokerAddrTable;
    
    // 集群信息存储 - Key: clusterName, Value: Set<brokerName>
    private final ConcurrentHashMap<String, ClusterInfo> clusterAddrTable;
    
    // Topic路由信息存储 - Key: topic, Value: TopicRouteData
    private final ConcurrentHashMap<String, TopicRouteData> topicRouteTable;
    
    // 读写锁保护
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    public ServiceRegistry() {
        this.brokerAddrTable = new ConcurrentHashMap<>();
        this.clusterAddrTable = new ConcurrentHashMap<>();
        this.topicRouteTable = new ConcurrentHashMap<>();
        logger.info("ServiceRegistry initialized");
    }
    
    /**
     * 注册Broker
     */
    public RegisterBrokerResult registerBroker(
            String clusterName,
            String brokerAddr,
            String brokerName,
            long brokerId,
            String haServerAddr,
            TopicConfigSerializeWrapper topicConfigWrapper,
            List<String> filterServerList,
            boolean compressed) {
        
        RegisterBrokerResult result = new RegisterBrokerResult();
        
        lock.writeLock().lock();
        try {
            // 更新集群信息
            ClusterInfo clusterInfo = clusterAddrTable.get(clusterName);
            if (clusterInfo == null) {
                clusterInfo = new ClusterInfo();
                clusterAddrTable.put(clusterName, clusterInfo);
            }
            clusterInfo.getBrokerNames().add(brokerName);
            
            // 更新Broker信息
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData == null) {
                brokerData = new BrokerData(clusterName, brokerName);
                brokerAddrTable.put(brokerName, brokerData);
            }
            
            // 更新Broker地址
            String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
            boolean registerFirst = oldAddr == null;
            
            // 更新Topic配置
            if (topicConfigWrapper != null && topicConfigWrapper.getTopicConfigTable() != null) {
                updateTopicRouteInfo(brokerName, topicConfigWrapper.getTopicConfigTable());
            }
            
            // 更新最后更新时间
            brokerData.setLastUpdateTimestamp(System.currentTimeMillis());
            
            result.setHaServerAddr(haServerAddr);
            result.setMasterAddr(brokerData.getBrokerAddrs().get(0L)); // Master的brokerId为0
            
            logger.info("Register broker success: cluster={}, brokerName={}, brokerAddr={}, brokerId={}, registerFirst={}", 
                       clusterName, brokerName, brokerAddr, brokerId, registerFirst);
            
        } finally {
            lock.writeLock().unlock();
        }
        
        return result;
    }
    
    /**
     * 注销Broker
     */
    public void unregisterBroker(String clusterName, String brokerAddr, String brokerName, long brokerId) {
        lock.writeLock().lock();
        try {
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData != null) {
                String removedAddr = brokerData.getBrokerAddrs().remove(brokerId);
                
                if (brokerData.getBrokerAddrs().isEmpty()) {
                    // 如果Broker没有任何地址了，移除整个BrokerData
                    brokerAddrTable.remove(brokerName);
                    
                    // 从集群信息中移除
                    ClusterInfo clusterInfo = clusterAddrTable.get(clusterName);
                    if (clusterInfo != null) {
                        clusterInfo.getBrokerNames().remove(brokerName);
                        if (clusterInfo.getBrokerNames().isEmpty()) {
                            clusterAddrTable.remove(clusterName);
                        }
                    }
                    
                    // 移除相关的Topic路由信息
                    removeTopicByBrokerName(brokerName);
                }
                
                logger.info("Unregister broker success: cluster={}, brokerName={}, brokerAddr={}, brokerId={}", 
                           clusterName, brokerName, removedAddr, brokerId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取Broker信息
     */
    public BrokerData getBrokerData(String brokerName) {
        lock.readLock().lock();
        try {
            return brokerAddrTable.get(brokerName);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有Broker信息
     */
    public Map<String, BrokerData> getAllBrokerData() {
        lock.readLock().lock();
        try {
            return new ConcurrentHashMap<>(brokerAddrTable);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取集群信息
     */
    public ClusterInfo getClusterInfo(String clusterName) {
        lock.readLock().lock();
        try {
            return clusterAddrTable.get(clusterName);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取Topic路由信息
     */
    public TopicRouteData getTopicRouteData(String topic) {
        lock.readLock().lock();
        try {
            return topicRouteTable.get(topic);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取Broker数量
     */
    public int getBrokerCount() {
        lock.readLock().lock();
        try {
            return brokerAddrTable.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 更新Topic路由信息
     */
    private void updateTopicRouteInfo(String brokerName, Map<String, TopicConfig> topicConfigTable) {
        for (Map.Entry<String, TopicConfig> entry : topicConfigTable.entrySet()) {
            String topic = entry.getKey();
            TopicConfig topicConfig = entry.getValue();
            
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            if (topicRouteData == null) {
                topicRouteData = new TopicRouteData();
                topicRouteTable.put(topic, topicRouteData);
            }
            
            // 更新队列数据
            QueueData queueData = new QueueData();
            queueData.setBrokerName(brokerName);
            queueData.setReadQueueNums(topicConfig.getReadQueueNums());
            queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
            queueData.setPerm(topicConfig.getPerm());
            queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());
            
            // 更新或添加队列数据
            topicRouteData.getQueueDatas().removeIf(qd -> qd.getBrokerName().equals(brokerName));
            topicRouteData.getQueueDatas().add(queueData);
        }
    }
    
    /**
     * 根据Broker名称移除Topic
     */
    private void removeTopicByBrokerName(String brokerName) {
        for (TopicRouteData topicRouteData : topicRouteTable.values()) {
            topicRouteData.getQueueDatas().removeIf(qd -> qd.getBrokerName().equals(brokerName));
        }
        
        // 移除没有队列数据的Topic
        topicRouteTable.entrySet().removeIf(entry -> entry.getValue().getQueueDatas().isEmpty());
    }
    
    /**
     * 关闭服务注册组件
     */
    public void shutdown() {
        logger.info("Shutting down ServiceRegistry...");
        
        lock.writeLock().lock();
        try {
            brokerAddrTable.clear();
            clusterAddrTable.clear();
            topicRouteTable.clear();
        } finally {
            lock.writeLock().unlock();
        }
        
        logger.info("ServiceRegistry shutdown completed");
    }
}
