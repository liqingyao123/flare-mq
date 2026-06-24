package com.ruyuan.mq.nameserver.registry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Broker数据模型
 * 
 * @author RuYuan MQ Team
 */
public class BrokerData {
    
    private String cluster;
    private String brokerName;
    private Map<Long, String> brokerAddrs; // Key: brokerId, Value: brokerAddr
    private volatile long lastUpdateTimestamp;
    
    public BrokerData() {
        this.brokerAddrs = new ConcurrentHashMap<>();
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }
    
    public BrokerData(String cluster, String brokerName) {
        this();
        this.cluster = cluster;
        this.brokerName = brokerName;
    }
    
    // Getters and Setters
    public String getCluster() {
        return cluster;
    }
    
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
    
    public String getBrokerName() {
        return brokerName;
    }
    
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
    
    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }
    
    public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }
    
    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }
    
    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }
    
    /**
     * 获取Master Broker地址
     */
    public String getMasterAddr() {
        return brokerAddrs.get(0L);
    }
    
    /**
     * 检查是否有Master Broker
     */
    public boolean hasMaster() {
        return brokerAddrs.containsKey(0L);
    }
    
    /**
     * 获取Slave Broker数量
     */
    public int getSlaveCount() {
        return (int) brokerAddrs.keySet().stream().filter(id -> id > 0).count();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        BrokerData that = (BrokerData) obj;
        return brokerName != null ? brokerName.equals(that.brokerName) : that.brokerName == null;
    }
    
    @Override
    public int hashCode() {
        return brokerName != null ? brokerName.hashCode() : 0;
    }
    
    @Override
    public String toString() {
        return "BrokerData{" +
                "cluster='" + cluster + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerAddrs=" + brokerAddrs +
                ", lastUpdateTimestamp=" + lastUpdateTimestamp +
                '}';
    }
}
