package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;

/**
 * 消费者组状态
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class ConsumerGroupStatus {
    private String groupName;
    private String subscriptionTopic;
    private int consumerCount;
    private long totalConsumed;
    private double consumeTps;
    private long lag;
    private String status; // ACTIVE, INACTIVE, ERROR
    private LocalDateTime lastUpdateTime;
    
    public ConsumerGroupStatus() {}
    
    public ConsumerGroupStatus(String groupName, String subscriptionTopic) {
        this.groupName = groupName;
        this.subscriptionTopic = subscriptionTopic;
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getGroupName() { return groupName; }
    public void setGroupName(String groupName) { this.groupName = groupName; }
    
    public String getSubscriptionTopic() { return subscriptionTopic; }
    public void setSubscriptionTopic(String subscriptionTopic) { this.subscriptionTopic = subscriptionTopic; }
    
    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int consumerCount) { this.consumerCount = consumerCount; }
    
    public long getTotalConsumed() { return totalConsumed; }
    public void setTotalConsumed(long totalConsumed) { this.totalConsumed = totalConsumed; }
    
    public double getConsumeTps() { return consumeTps; }
    public void setConsumeTps(double consumeTps) { this.consumeTps = consumeTps; }
    
    public long getLag() { return lag; }
    public void setLag(long lag) { this.lag = lag; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    @Override
    public String toString() {
        return String.format("ConsumerGroupStatus{group='%s', topic='%s', consumers=%d, tps=%.2f, lag=%d, status='%s'}",
                groupName, subscriptionTopic, consumerCount, consumeTps, lag, status);
    }
}
