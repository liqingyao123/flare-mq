package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 消费者组状态
 */
public class ConsumerGroupStatus {
    private String groupName;
    private String topic;
    private int consumerCount;
    private int activeConsumers;
    private long totalConsumed;
    private double consumeTps;
    private long totalLag;
    private String status;
    private LocalDateTime lastUpdateTime;
    private List<QueueInfo> queues;
    private List<ConsumerInfo> consumers;

    public ConsumerGroupStatus() {}

    // Getters and Setters
    public String getGroupName() { return groupName; }
    public void setGroupName(String v) { this.groupName = v; }
    public String getTopic() { return topic; }
    public void setTopic(String v) { this.topic = v; }
    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int v) { this.consumerCount = v; }
    public int getActiveConsumers() { return activeConsumers; }
    public void setActiveConsumers(int v) { this.activeConsumers = v; }
    public long getTotalConsumed() { return totalConsumed; }
    public void setTotalConsumed(long v) { this.totalConsumed = v; }
    public double getConsumeTps() { return consumeTps; }
    public void setConsumeTps(double v) { this.consumeTps = v; }
    public long getTotalLag() { return totalLag; }
    public void setTotalLag(long v) { this.totalLag = v; }
    public String getStatus() { return status; }
    public void setStatus(String v) { this.status = v; }
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime v) { this.lastUpdateTime = v; }
    public List<QueueInfo> getQueues() { return queues; }
    public void setQueues(List<QueueInfo> v) { this.queues = v; }
    public List<ConsumerInfo> getConsumers() { return consumers; }
    public void setConsumers(List<ConsumerInfo> v) { this.consumers = v; }

    public static class QueueInfo {
        private int queueId;
        private long maxOffset;
        private long consumedOffset;
        private long lag;

        public int getQueueId() { return queueId; }
        public void setQueueId(int v) { this.queueId = v; }
        public long getMaxOffset() { return maxOffset; }
        public void setMaxOffset(long v) { this.maxOffset = v; }
        public long getConsumedOffset() { return consumedOffset; }
        public void setConsumedOffset(long v) { this.consumedOffset = v; }
        public long getLag() { return lag; }
        public void setLag(long v) { this.lag = v; }
    }

    public static class ConsumerInfo {
        private String consumerId;
        private long lastHeartbeat;
        private boolean alive;

        public String getConsumerId() { return consumerId; }
        public void setConsumerId(String v) { this.consumerId = v; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public void setLastHeartbeat(long v) { this.lastHeartbeat = v; }
        public boolean isAlive() { return alive; }
        public void setAlive(boolean v) { this.alive = v; }
    }
}
