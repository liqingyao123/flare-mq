package com.ruyuan.mq.broker.ack;

import java.util.Objects;

/**
 * 重试记录
 * 
 * @author RuYuan MQ Team
 */
public class RetryRecord {
    
    /**
     * 消息ID
     */
    private String messageId;
    
    /**
     * 消费者组
     */
    private String consumerGroup;
    
    /**
     * Topic名称
     */
    private String topic;
    
    /**
     * 队列ID
     */
    private int queueId;
    
    /**
     * 重试次数
     */
    private int retryCount;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 最后重试时间
     */
    private long lastRetryTime;
    
    /**
     * 下次重试时间
     */
    private long nextRetryTime;
    
    /**
     * 失败原因
     */
    private String failureReason;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * 重试级别
     */
    private RetryLevel retryLevel;
    
    /**
     * 消费者ID
     */
    private String consumerId;
    
    /**
     * 消息偏移量
     */
    private long messageOffset;
    
    /**
     * 默认构造函数
     */
    public RetryRecord() {
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
        this.retryCount = 0;
        this.retryLevel = RetryLevel.NORMAL;
    }
    
    /**
     * 构造函数
     */
    public RetryRecord(String messageId, String consumerGroup, String topic, int queueId) {
        this();
        this.messageId = messageId;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.queueId = queueId;
    }
    
    /**
     * 完整构造函数
     */
    public RetryRecord(String messageId, String consumerGroup, String topic, int queueId,
                      String consumerId, long messageOffset) {
        this(messageId, consumerGroup, topic, queueId);
        this.consumerId = consumerId;
        this.messageOffset = messageOffset;
    }
    
    // Getter和Setter方法
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getConsumerGroup() {
        return consumerGroup;
    }
    
    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        this.lastUpdateTime = System.currentTimeMillis();
        updateRetryLevel();
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public long getLastRetryTime() {
        return lastRetryTime;
    }
    
    public void setLastRetryTime(long lastRetryTime) {
        this.lastRetryTime = lastRetryTime;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getNextRetryTime() {
        return nextRetryTime;
    }
    
    public void setNextRetryTime(long nextRetryTime) {
        this.nextRetryTime = nextRetryTime;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getFailureReason() {
        return failureReason;
    }
    
    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public RetryLevel getRetryLevel() {
        return retryLevel;
    }
    
    public void setRetryLevel(RetryLevel retryLevel) {
        this.retryLevel = retryLevel;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getConsumerId() {
        return consumerId;
    }
    
    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getMessageOffset() {
        return messageOffset;
    }
    
    public void setMessageOffset(long messageOffset) {
        this.messageOffset = messageOffset;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 增加重试次数
     */
    public void incrementRetryCount() {
        this.retryCount++;
        this.lastUpdateTime = System.currentTimeMillis();
        updateRetryLevel();
    }
    
    /**
     * 检查是否可以重试
     */
    public boolean canRetry(int maxRetryTimes) {
        return retryCount < maxRetryTimes;
    }
    
    /**
     * 检查是否到了重试时间
     */
    public boolean isRetryTime() {
        return System.currentTimeMillis() >= nextRetryTime;
    }
    
    /**
     * 获取重试等待时间（毫秒）
     */
    public long getRetryWaitTime() {
        return Math.max(0, nextRetryTime - System.currentTimeMillis());
    }
    
    /**
     * 获取总重试时间（毫秒）
     */
    public long getTotalRetryTime() {
        if (lastRetryTime > 0) {
            return lastRetryTime - createTime;
        } else {
            return System.currentTimeMillis() - createTime;
        }
    }
    
    /**
     * 更新重试级别
     */
    private void updateRetryLevel() {
        if (retryCount <= 3) {
            this.retryLevel = RetryLevel.NORMAL;
        } else if (retryCount <= 8) {
            this.retryLevel = RetryLevel.HIGH;
        } else {
            this.retryLevel = RetryLevel.CRITICAL;
        }
    }
    
    /**
     * 获取记录的唯一标识
     */
    public String getRecordKey() {
        return messageId + ":" + consumerGroup;
    }
    
    /**
     * 复制记录
     */
    public RetryRecord copy() {
        RetryRecord copy = new RetryRecord();
        copy.messageId = this.messageId;
        copy.consumerGroup = this.consumerGroup;
        copy.topic = this.topic;
        copy.queueId = this.queueId;
        copy.retryCount = this.retryCount;
        copy.createTime = this.createTime;
        copy.lastRetryTime = this.lastRetryTime;
        copy.nextRetryTime = this.nextRetryTime;
        copy.failureReason = this.failureReason;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.retryLevel = this.retryLevel;
        copy.consumerId = this.consumerId;
        copy.messageOffset = this.messageOffset;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RetryRecord that = (RetryRecord) o;
        return Objects.equals(messageId, that.messageId) && 
               Objects.equals(consumerGroup, that.consumerGroup);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(messageId, consumerGroup);
    }
    
    @Override
    public String toString() {
        return "RetryRecord{" +
                "messageId='" + messageId + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", retryCount=" + retryCount +
                ", retryLevel=" + retryLevel +
                ", createTime=" + createTime +
                ", lastRetryTime=" + lastRetryTime +
                ", nextRetryTime=" + nextRetryTime +
                ", retryWaitTime=" + getRetryWaitTime() + "ms" +
                ", totalRetryTime=" + getTotalRetryTime() + "ms" +
                ", failureReason='" + failureReason + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", messageOffset=" + messageOffset +
                '}';
    }
}
