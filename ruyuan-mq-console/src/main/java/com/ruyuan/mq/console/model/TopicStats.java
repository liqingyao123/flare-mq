package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;

/**
 * Topic统计信息
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class TopicStats {
    private String topicName;
    private int queueCount;
    private long totalMessages;
    private double currentTps;
    private long totalSize;
    private LocalDateTime lastUpdateTime;
    
    public TopicStats() {}
    
    public TopicStats(String topicName, int queueCount) {
        this.topicName = topicName;
        this.queueCount = queueCount;
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public String getTopicName() { return topicName; }
    public void setTopicName(String topicName) { this.topicName = topicName; }
    
    public int getQueueCount() { return queueCount; }
    public void setQueueCount(int queueCount) { this.queueCount = queueCount; }
    
    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long totalMessages) { this.totalMessages = totalMessages; }
    
    public double getCurrentTps() { return currentTps; }
    public void setCurrentTps(double currentTps) { this.currentTps = currentTps; }
    
    public long getTotalSize() { return totalSize; }
    public void setTotalSize(long totalSize) { this.totalSize = totalSize; }
    
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    @Override
    public String toString() {
        return String.format("TopicStats{name='%s', queues=%d, messages=%d, tps=%.2f, size=%d}",
                topicName, queueCount, totalMessages, currentTps, totalSize);
    }
}
