package com.flare.mq.nameserver.registry;

import java.util.List;
import java.util.ArrayList;

/**
 * Topic路由数据模型
 * 
 * @author FlareMQ Team
 */
public class TopicRouteData {
    
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;
    private String filterServerTable;
    
    public TopicRouteData() {
        this.queueDatas = new ArrayList<>();
        this.brokerDatas = new ArrayList<>();
    }
    
    // Getters and Setters
    public String getOrderTopicConf() {
        return orderTopicConf;
    }
    
    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }
    
    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }
    
    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }
    
    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }
    
    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }
    
    public String getFilterServerTable() {
        return filterServerTable;
    }
    
    public void setFilterServerTable(String filterServerTable) {
        this.filterServerTable = filterServerTable;
    }
    
    /**
     * 添加队列数据
     */
    public void addQueueData(QueueData queueData) {
        if (queueData != null) {
            this.queueDatas.add(queueData);
        }
    }
    
    /**
     * 添加Broker数据
     */
    public void addBrokerData(BrokerData brokerData) {
        if (brokerData != null && !this.brokerDatas.contains(brokerData)) {
            this.brokerDatas.add(brokerData);
        }
    }
    
    /**
     * 获取写队列总数
     */
    public int getTotalWriteQueueNums() {
        return queueDatas.stream().mapToInt(QueueData::getWriteQueueNums).sum();
    }
    
    /**
     * 获取读队列总数
     */
    public int getTotalReadQueueNums() {
        return queueDatas.stream().mapToInt(QueueData::getReadQueueNums).sum();
    }
    
    /**
     * 检查是否有可写队列
     */
    public boolean hasWritableQueues() {
        return queueDatas.stream().anyMatch(qd -> qd.getWriteQueueNums() > 0);
    }
    
    /**
     * 检查是否有可读队列
     */
    public boolean hasReadableQueues() {
        return queueDatas.stream().anyMatch(qd -> qd.getReadQueueNums() > 0);
    }
    
    @Override
    public String toString() {
        return "TopicRouteData{" +
                "orderTopicConf='" + orderTopicConf + '\'' +
                ", queueDatas=" + queueDatas.size() +
                ", brokerDatas=" + brokerDatas.size() +
                ", filterServerTable='" + filterServerTable + '\'' +
                '}';
    }
}
