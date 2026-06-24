package com.ruyuan.mq.client.producer;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Topic路由信息
 *
 * @author RuYuan MQ Team
 */
public class TopicRouteInfo {
    
    private String topic;
    private List<QueueInfo> queueInfos;
    private List<BrokerInfo> brokerInfos;
    private long lastUpdateTime;
    private final AtomicInteger queueSelector = new AtomicInteger(0);
    
    public TopicRouteInfo() {
        this.queueInfos = new ArrayList<>();
        this.brokerInfos = new ArrayList<>();
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public TopicRouteInfo(String topic) {
        this();
        this.topic = topic;
    }
    
    /**
     * 选择一个队列用于发送消息（轮询策略）
     */
    public QueueInfo selectQueue() {
        if (queueInfos.isEmpty()) {
            return null;
        }
        
        int index = Math.abs(queueSelector.getAndIncrement()) % queueInfos.size();
        return queueInfos.get(index);
    }
    
    /**
     * 根据Broker名称获取Broker信息
     */
    public BrokerInfo getBrokerInfo(String brokerName) {
        for (BrokerInfo brokerInfo : brokerInfos) {
            if (brokerInfo.getBrokerName().equals(brokerName)) {
                return brokerInfo;
            }
        }
        return null;
    }
    
    /**
     * 检查路由信息是否过期（5分钟）
     */
    public boolean isExpired() {
        return (System.currentTimeMillis() - lastUpdateTime) > 300000;
    }
    
    /**
     * 更新时间戳
     */
    public void updateTimestamp() {
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public List<QueueInfo> getQueueInfos() {
        return queueInfos;
    }
    
    public void setQueueInfos(List<QueueInfo> queueInfos) {
        this.queueInfos = queueInfos;
    }
    
    public List<BrokerInfo> getBrokerInfos() {
        return brokerInfos;
    }
    
    public void setBrokerInfos(List<BrokerInfo> brokerInfos) {
        this.brokerInfos = brokerInfos;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    /**
     * 队列信息
     */
    public static class QueueInfo {
        private String brokerName;
        private int queueId;
        private boolean readable;
        private boolean writable;
        
        public QueueInfo() {}
        
        public QueueInfo(String brokerName, int queueId, boolean readable, boolean writable) {
            this.brokerName = brokerName;
            this.queueId = queueId;
            this.readable = readable;
            this.writable = writable;
        }
        
        // Getters and Setters
        public String getBrokerName() {
            return brokerName;
        }
        
        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }
        
        public int getQueueId() {
            return queueId;
        }
        
        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }
        
        public boolean isReadable() {
            return readable;
        }
        
        public void setReadable(boolean readable) {
            this.readable = readable;
        }
        
        public boolean isWritable() {
            return writable;
        }
        
        public void setWritable(boolean writable) {
            this.writable = writable;
        }
        
        @Override
        public String toString() {
            return String.format("QueueInfo{brokerName='%s', queueId=%d, readable=%s, writable=%s}", 
                               brokerName, queueId, readable, writable);
        }
    }
    
    /**
     * Broker信息
     */
    public static class BrokerInfo {
        private String brokerName;
        private String cluster;
        private Map<Long, String> brokerAddrs; // brokerId -> address
        
        public BrokerInfo() {}
        
        public BrokerInfo(String brokerName, String cluster, Map<Long, String> brokerAddrs) {
            this.brokerName = brokerName;
            this.cluster = cluster;
            this.brokerAddrs = brokerAddrs;
        }
        
        /**
         * 获取Master Broker地址
         */
        public String getMasterAddr() {
            return brokerAddrs != null ? brokerAddrs.get(0L) : null;
        }
        
        // Getters and Setters
        public String getBrokerName() {
            return brokerName;
        }
        
        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }
        
        public String getCluster() {
            return cluster;
        }
        
        public void setCluster(String cluster) {
            this.cluster = cluster;
        }
        
        public Map<Long, String> getBrokerAddrs() {
            return brokerAddrs;
        }
        
        public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
            this.brokerAddrs = brokerAddrs;
        }
        
        @Override
        public String toString() {
            return String.format("BrokerInfo{brokerName='%s', cluster='%s', masterAddr='%s'}", 
                               brokerName, cluster, getMasterAddr());
        }
    }
    
    @Override
    public String toString() {
        return String.format("TopicRouteInfo{topic='%s', queues=%d, brokers=%d, lastUpdate=%d}", 
                           topic, queueInfos.size(), brokerInfos.size(), lastUpdateTime);
    }
}
