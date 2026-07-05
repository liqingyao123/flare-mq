package com.flare.mq.nameserver.registry;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群信息模型
 * 
 * @author FlareMQ Team
 */
public class ClusterInfo {
    
    private Set<String> brokerNames;
    private volatile long lastUpdateTimestamp;
    
    public ClusterInfo() {
        this.brokerNames = ConcurrentHashMap.newKeySet();
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public Set<String> getBrokerNames() {
        return brokerNames;
    }
    
    public void setBrokerNames(Set<String> brokerNames) {
        this.brokerNames = brokerNames;
    }
    
    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }
    
    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }
    
    /**
     * 添加Broker
     */
    public void addBroker(String brokerName) {
        brokerNames.add(brokerName);
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }
    
    /**
     * 移除Broker
     */
    public void removeBroker(String brokerName) {
        brokerNames.remove(brokerName);
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }
    
    /**
     * 获取Broker数量
     */
    public int getBrokerCount() {
        return brokerNames.size();
    }
    
    /**
     * 检查是否包含指定Broker
     */
    public boolean containsBroker(String brokerName) {
        return brokerNames.contains(brokerName);
    }
    
    @Override
    public String toString() {
        return "ClusterInfo{" +
                "brokerNames=" + brokerNames +
                ", lastUpdateTimestamp=" + lastUpdateTimestamp +
                '}';
    }
}
