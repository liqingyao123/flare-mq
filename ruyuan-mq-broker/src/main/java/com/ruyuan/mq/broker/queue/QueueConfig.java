package com.ruyuan.mq.broker.queue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Queue配置信息
 * 
 * @author RuYuan MQ Team
 */
public class QueueConfig {
    
    /**
     * 所属Topic名称
     */
    private String topicName;
    
    /**
     * Queue ID
     */
    private int queueId;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * 是否启用
     */
    private boolean enabled;
    
    /**
     * 消息保留时间（毫秒）
     */
    private long messageRetentionTime;
    
    /**
     * 最大消息大小（字节）
     */
    private int maxMessageSize;
    
    /**
     * 当前消息数量
     */
    private final AtomicLong messageCount = new AtomicLong(0);
    
    /**
     * 总消息字节数
     */
    private final AtomicLong totalMessageBytes = new AtomicLong(0);
    
    /**
     * 最后消息时间
     */
    private volatile long lastMessageTime;
    
    /**
     * 消费位置
     */
    private final AtomicLong consumeOffset = new AtomicLong(0);
    
    /**
     * 最大消息偏移量
     */
    private final AtomicLong maxOffset = new AtomicLong(0);
    
    /**
     * Queue状态
     */
    private volatile QueueStatus status;
    
    /**
     * 描述信息
     */
    private String description;
    
    /**
     * 默认构造函数
     */
    public QueueConfig() {
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
        this.enabled = true;
        this.messageRetentionTime = 72 * 60 * 60 * 1000L; // 默认72小时
        this.maxMessageSize = 4 * 1024 * 1024; // 默认4MB
        this.status = QueueStatus.ACTIVE;
    }
    
    /**
     * 构造函数
     */
    public QueueConfig(String topicName, int queueId) {
        this();
        this.topicName = topicName;
        this.queueId = queueId;
    }
    
    // Getter和Setter方法
    
    public String getTopicName() {
        return topicName;
    }
    
    public void setTopicName(String topicName) {
        this.topicName = topicName;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getMessageRetentionTime() {
        return messageRetentionTime;
    }
    
    public void setMessageRetentionTime(long messageRetentionTime) {
        this.messageRetentionTime = messageRetentionTime;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getMessageCount() {
        return messageCount.get();
    }
    
    public long getTotalMessageBytes() {
        return totalMessageBytes.get();
    }
    
    public long getLastMessageTime() {
        return lastMessageTime;
    }
    
    public void setLastMessageTime(long lastMessageTime) {
        this.lastMessageTime = lastMessageTime;
    }
    
    public long getConsumeOffset() {
        return consumeOffset.get();
    }
    
    public long getMaxOffset() {
        return maxOffset.get();
    }
    
    public QueueStatus getStatus() {
        return status;
    }
    
    public void setStatus(QueueStatus status) {
        this.status = status;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 增加消息计数
     */
    public void incrementMessageCount() {
        messageCount.incrementAndGet();
        this.lastMessageTime = System.currentTimeMillis();
    }
    
    /**
     * 增加消息字节数
     */
    public void addMessageBytes(long bytes) {
        totalMessageBytes.addAndGet(bytes);
    }
    
    /**
     * 更新消费位置
     */
    public void updateConsumeOffset(long offset) {
        consumeOffset.set(offset);
    }
    
    /**
     * 更新最大偏移量
     */
    public void updateMaxOffset(long offset) {
        maxOffset.set(offset);
    }
    
    /**
     * 获取未消费消息数量
     */
    public long getUnconsumedMessageCount() {
        return Math.max(0, maxOffset.get() - consumeOffset.get());
    }
    
    /**
     * 获取消费进度百分比
     */
    public double getConsumeProgress() {
        long max = maxOffset.get();
        if (max == 0) {
            return 100.0;
        }
        return (double) consumeOffset.get() / max * 100.0;
    }
    
    /**
     * 检查Queue是否活跃
     */
    public boolean isActive() {
        return status == QueueStatus.ACTIVE && enabled;
    }
    
    /**
     * 获取Queue的唯一标识
     */
    public String getQueueKey() {
        return topicName + ":" + queueId;
    }
    
    /**
     * 复制配置
     */
    public QueueConfig copy() {
        QueueConfig copy = new QueueConfig();
        copy.topicName = this.topicName;
        copy.queueId = this.queueId;
        copy.createTime = this.createTime;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.enabled = this.enabled;
        copy.messageRetentionTime = this.messageRetentionTime;
        copy.maxMessageSize = this.maxMessageSize;
        copy.messageCount.set(this.messageCount.get());
        copy.totalMessageBytes.set(this.totalMessageBytes.get());
        copy.lastMessageTime = this.lastMessageTime;
        copy.consumeOffset.set(this.consumeOffset.get());
        copy.maxOffset.set(this.maxOffset.get());
        copy.status = this.status;
        copy.description = this.description;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueConfig that = (QueueConfig) o;
        return queueId == that.queueId && Objects.equals(topicName, that.topicName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topicName, queueId);
    }
    
    @Override
    public String toString() {
        return "QueueConfig{" +
                "topicName='" + topicName + '\'' +
                ", queueId=" + queueId +
                ", enabled=" + enabled +
                ", status=" + status +
                ", messageCount=" + messageCount.get() +
                ", totalMessageBytes=" + totalMessageBytes.get() +
                ", consumeOffset=" + consumeOffset.get() +
                ", maxOffset=" + maxOffset.get() +
                ", unconsumedCount=" + getUnconsumedMessageCount() +
                ", consumeProgress=" + String.format("%.2f%%", getConsumeProgress()) +
                '}';
    }
}
