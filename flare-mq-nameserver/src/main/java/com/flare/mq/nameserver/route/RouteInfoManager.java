package com.flare.mq.nameserver.route;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * 路由信息管理器 - 管理Topic和Queue的路由信息
 * 
 * @author FlareMQ Team
 */
public class RouteInfoManager {
    
    private static final Logger logger = LoggerFactory.getLogger(RouteInfoManager.class);
    
    // Topic路由信息存储
    private final ConcurrentHashMap<String, TopicRouteInfo> topicRouteTable;
    
    // Queue路由信息存储
    private final ConcurrentHashMap<String, QueueRouteInfo> queueRouteTable;
    
    // 路由版本管理
    private final AtomicInteger routeVersion = new AtomicInteger(0);
    
    // 读写锁保护
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // 路由过期时间配置
    private static final long ROUTE_EXPIRED_TIME = 1000 * 60 * 5; // 5分钟
    
    public RouteInfoManager() {
        this.topicRouteTable = new ConcurrentHashMap<>();
        this.queueRouteTable = new ConcurrentHashMap<>();
        logger.info("RouteInfoManager initialized");
    }
    
    /**
     * 更新Topic路由信息
     */
    public void updateTopicRouteInfo(String topic, String brokerName, int readQueueNums, 
                                   int writeQueueNums, int perm) {
        lock.writeLock().lock();
        try {
            TopicRouteInfo routeInfo = topicRouteTable.computeIfAbsent(topic, 
                    k -> new TopicRouteInfo(topic));
            
            // 更新Broker信息
            BrokerRouteInfo brokerRouteInfo = new BrokerRouteInfo(brokerName, readQueueNums, 
                                                                writeQueueNums, perm);
            routeInfo.getBrokerRoutes().put(brokerName, brokerRouteInfo);
            routeInfo.setLastUpdateTime(System.currentTimeMillis());
            
            // 更新队列路由信息
            updateQueueRouteInfo(topic, brokerName, readQueueNums, writeQueueNums);
            
            // 增加路由版本
            routeVersion.incrementAndGet();
            
            logger.debug("Updated topic route info: topic={}, broker={}, readQueues={}, writeQueues={}", 
                        topic, brokerName, readQueueNums, writeQueueNums);
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 移除Topic路由信息
     */
    public void removeTopicRouteInfo(String topic, String brokerName) {
        lock.writeLock().lock();
        try {
            TopicRouteInfo routeInfo = topicRouteTable.get(topic);
            if (routeInfo != null) {
                routeInfo.getBrokerRoutes().remove(brokerName);
                
                // 如果没有Broker了，移除整个Topic
                if (routeInfo.getBrokerRoutes().isEmpty()) {
                    topicRouteTable.remove(topic);
                }
                
                // 移除相关的队列路由信息
                removeQueueRouteInfo(topic, brokerName);
                
                // 增加路由版本
                routeVersion.incrementAndGet();
                
                logger.debug("Removed topic route info: topic={}, broker={}", topic, brokerName);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取Topic路由信息
     */
    public TopicRouteInfo getTopicRouteInfo(String topic) {
        lock.readLock().lock();
        try {
            return topicRouteTable.get(topic);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有Topic列表
     */
    public Set<String> getAllTopics() {
        lock.readLock().lock();
        try {
            return new HashSet<>(topicRouteTable.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取Topic的写队列列表
     */
    public Set<String> getWriteQueuesByTopic(String topic) {
        lock.readLock().lock();
        try {
            Set<String> writeQueues = new HashSet<>();
            String queueKey = topic + "_write";
            QueueRouteInfo queueRouteInfo = queueRouteTable.get(queueKey);
            
            if (queueRouteInfo != null) {
                writeQueues.addAll(queueRouteInfo.getQueueNames());
            }
            
            return writeQueues;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取Topic的读队列列表
     */
    public Set<String> getReadQueuesByTopic(String topic) {
        lock.readLock().lock();
        try {
            Set<String> readQueues = new HashSet<>();
            String queueKey = topic + "_read";
            QueueRouteInfo queueRouteInfo = queueRouteTable.get(queueKey);
            
            if (queueRouteInfo != null) {
                readQueues.addAll(queueRouteInfo.getQueueNames());
            }
            
            return readQueues;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 清理过期的路由信息
     */
    public void cleanupExpiredRoutes() {
        lock.writeLock().lock();
        try {
            long currentTime = System.currentTimeMillis();
            final AtomicInteger removedTopics = new AtomicInteger(0);
            final AtomicInteger removedQueues = new AtomicInteger(0);

            // 清理过期的Topic路由
            topicRouteTable.entrySet().removeIf(entry -> {
                TopicRouteInfo routeInfo = entry.getValue();
                boolean expired = (currentTime - routeInfo.getLastUpdateTime()) > ROUTE_EXPIRED_TIME;
                if (expired) {
                    removedTopics.incrementAndGet();
                    logger.debug("Removed expired topic route: {}", entry.getKey());
                }
                return expired;
            });

            // 清理过期的Queue路由
            queueRouteTable.entrySet().removeIf(entry -> {
                QueueRouteInfo routeInfo = entry.getValue();
                boolean expired = (currentTime - routeInfo.getLastUpdateTime()) > ROUTE_EXPIRED_TIME;
                if (expired) {
                    removedQueues.incrementAndGet();
                    logger.debug("Removed expired queue route: {}", entry.getKey());
                }
                return expired;
            });

            if (removedTopics.get() > 0 || removedQueues.get() > 0) {
                routeVersion.incrementAndGet();
                logger.info("Cleaned up expired routes: topics={}, queues={}", removedTopics.get(), removedQueues.get());
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取Topic数量
     */
    public int getTopicCount() {
        return topicRouteTable.size();
    }
    
    /**
     * 获取Queue数量
     */
    public int getQueueCount() {
        return queueRouteTable.size();
    }
    
    /**
     * 获取路由版本
     */
    public int getRouteVersion() {
        return routeVersion.get();
    }
    
    /**
     * 获取路由统计信息
     */
    public RouteStatistics getStatistics() {
        lock.readLock().lock();
        try {
            int topicCount = topicRouteTable.size();
            int queueCount = queueRouteTable.size();
            int version = routeVersion.get();
            
            // 计算总的Broker数量
            Set<String> allBrokers = new HashSet<>();
            for (TopicRouteInfo routeInfo : topicRouteTable.values()) {
                allBrokers.addAll(routeInfo.getBrokerRoutes().keySet());
            }
            
            return new RouteStatistics(topicCount, queueCount, allBrokers.size(), version);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 更新队列路由信息
     */
    private void updateQueueRouteInfo(String topic, String brokerName, int readQueueNums, int writeQueueNums) {
        // 更新写队列路由
        if (writeQueueNums > 0) {
            String writeQueueKey = topic + "_write";
            QueueRouteInfo writeQueueRoute = queueRouteTable.computeIfAbsent(writeQueueKey, 
                    k -> new QueueRouteInfo(topic, "write"));
            
            for (int i = 0; i < writeQueueNums; i++) {
                String queueName = brokerName + "_" + i;
                writeQueueRoute.getQueueNames().add(queueName);
            }
            writeQueueRoute.setLastUpdateTime(System.currentTimeMillis());
        }
        
        // 更新读队列路由
        if (readQueueNums > 0) {
            String readQueueKey = topic + "_read";
            QueueRouteInfo readQueueRoute = queueRouteTable.computeIfAbsent(readQueueKey, 
                    k -> new QueueRouteInfo(topic, "read"));
            
            for (int i = 0; i < readQueueNums; i++) {
                String queueName = brokerName + "_" + i;
                readQueueRoute.getQueueNames().add(queueName);
            }
            readQueueRoute.setLastUpdateTime(System.currentTimeMillis());
        }
    }
    
    /**
     * 移除队列路由信息
     */
    private void removeQueueRouteInfo(String topic, String brokerName) {
        // 移除写队列路由
        String writeQueueKey = topic + "_write";
        QueueRouteInfo writeQueueRoute = queueRouteTable.get(writeQueueKey);
        if (writeQueueRoute != null) {
            writeQueueRoute.getQueueNames().removeIf(queueName -> queueName.startsWith(brokerName + "_"));
            if (writeQueueRoute.getQueueNames().isEmpty()) {
                queueRouteTable.remove(writeQueueKey);
            }
        }
        
        // 移除读队列路由
        String readQueueKey = topic + "_read";
        QueueRouteInfo readQueueRoute = queueRouteTable.get(readQueueKey);
        if (readQueueRoute != null) {
            readQueueRoute.getQueueNames().removeIf(queueName -> queueName.startsWith(brokerName + "_"));
            if (readQueueRoute.getQueueNames().isEmpty()) {
                queueRouteTable.remove(readQueueKey);
            }
        }
    }
    
    /**
     * 关闭路由信息管理器
     */
    public void shutdown() {
        logger.info("Shutting down RouteInfoManager...");
        
        lock.writeLock().lock();
        try {
            topicRouteTable.clear();
            queueRouteTable.clear();
            routeVersion.set(0);
        } finally {
            lock.writeLock().unlock();
        }
        
        logger.info("RouteInfoManager shutdown completed");
    }
    
    /**
     * Topic路由信息
     */
    public static class TopicRouteInfo {
        private final String topic;
        private final Map<String, BrokerRouteInfo> brokerRoutes;
        private volatile long lastUpdateTime;
        
        public TopicRouteInfo(String topic) {
            this.topic = topic;
            this.brokerRoutes = new ConcurrentHashMap<>();
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        // Getters and Setters
        public String getTopic() { return topic; }
        public Map<String, BrokerRouteInfo> getBrokerRoutes() { return brokerRoutes; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        public void setLastUpdateTime(long lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    }
    
    /**
     * Broker路由信息
     */
    public static class BrokerRouteInfo {
        private final String brokerName;
        private final int readQueueNums;
        private final int writeQueueNums;
        private final int perm;
        
        public BrokerRouteInfo(String brokerName, int readQueueNums, int writeQueueNums, int perm) {
            this.brokerName = brokerName;
            this.readQueueNums = readQueueNums;
            this.writeQueueNums = writeQueueNums;
            this.perm = perm;
        }
        
        // Getters
        public String getBrokerName() { return brokerName; }
        public int getReadQueueNums() { return readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public int getPerm() { return perm; }
    }
    
    /**
     * 队列路由信息
     */
    public static class QueueRouteInfo {
        private final String topic;
        private final String queueType; // "read" or "write"
        private final Set<String> queueNames;
        private volatile long lastUpdateTime;
        
        public QueueRouteInfo(String topic, String queueType) {
            this.topic = topic;
            this.queueType = queueType;
            this.queueNames = new HashSet<>();
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        // Getters and Setters
        public String getTopic() { return topic; }
        public String getQueueType() { return queueType; }
        public Set<String> getQueueNames() { return queueNames; }
        public long getLastUpdateTime() { return lastUpdateTime; }
        public void setLastUpdateTime(long lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    }
    
    /**
     * 路由统计信息
     */
    public static class RouteStatistics {
        private final int topicCount;
        private final int queueCount;
        private final int brokerCount;
        private final int version;
        
        public RouteStatistics(int topicCount, int queueCount, int brokerCount, int version) {
            this.topicCount = topicCount;
            this.queueCount = queueCount;
            this.brokerCount = brokerCount;
            this.version = version;
        }
        
        // Getters
        public int getTopicCount() { return topicCount; }
        public int getQueueCount() { return queueCount; }
        public int getBrokerCount() { return brokerCount; }
        public int getVersion() { return version; }
        
        @Override
        public String toString() {
            return String.format("RouteStatistics{topicCount=%d, queueCount=%d, brokerCount=%d, version=%d}", 
                               topicCount, queueCount, brokerCount, version);
        }
    }
}
