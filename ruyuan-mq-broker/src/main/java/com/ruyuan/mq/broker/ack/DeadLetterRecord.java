package com.ruyuan.mq.broker.ack;

import java.util.Objects;

/**
 * 死信记录
 * 
 * @author RuYuan MQ Team
 */
public class DeadLetterRecord {
    
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
     * 失败原因
     */
    private String failureReason;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * 死信类型
     */
    private DeadLetterType deadLetterType;
    
    /**
     * 消费者ID
     */
    private String consumerId;
    
    /**
     * 消息偏移量
     */
    private long messageOffset;
    
    /**
     * 原始消息内容（可选）
     */
    private String originalMessage;
    
    /**
     * 处理状态
     */
    private DeadLetterStatus status;
    
    /**
     * 备注信息
     */
    private String remark;
    
    /**
     * 默认构造函数
     */
    public DeadLetterRecord() {
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
        this.deadLetterType = DeadLetterType.RETRY_EXHAUSTED;
        this.status = DeadLetterStatus.PENDING;
    }
    
    /**
     * 构造函数
     */
    public DeadLetterRecord(String messageId, String consumerGroup, String topic, 
                           int queueId, int retryCount, String failureReason) {
        this();
        this.messageId = messageId;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.queueId = queueId;
        this.retryCount = retryCount;
        this.failureReason = failureReason;
    }
    
    /**
     * 完整构造函数
     */
    public DeadLetterRecord(String messageId, String consumerGroup, String topic, 
                           int queueId, int retryCount, String failureReason,
                           DeadLetterType deadLetterType, String consumerId, long messageOffset) {
        this(messageId, consumerGroup, topic, queueId, retryCount, failureReason);
        this.deadLetterType = deadLetterType;
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
    }
    
    public String getFailureReason() {
        return failureReason;
    }
    
    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
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
    
    public DeadLetterType getDeadLetterType() {
        return deadLetterType;
    }
    
    public void setDeadLetterType(DeadLetterType deadLetterType) {
        this.deadLetterType = deadLetterType;
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
    
    public String getOriginalMessage() {
        return originalMessage;
    }
    
    public void setOriginalMessage(String originalMessage) {
        this.originalMessage = originalMessage;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public DeadLetterStatus getStatus() {
        return status;
    }
    
    public void setStatus(DeadLetterStatus status) {
        this.status = status;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public String getRemark() {
        return remark;
    }
    
    public void setRemark(String remark) {
        this.remark = remark;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 获取死信存活时间（毫秒）
     */
    public long getDeadLetterAge() {
        return System.currentTimeMillis() - createTime;
    }
    
    /**
     * 检查是否可以重新投递
     */
    public boolean canRedeliver() {
        return status == DeadLetterStatus.PENDING || status == DeadLetterStatus.REDELIVERY_FAILED;
    }
    
    /**
     * 检查是否已处理
     */
    public boolean isProcessed() {
        return status == DeadLetterStatus.REDELIVERED || status == DeadLetterStatus.DISCARDED;
    }
    
    /**
     * 获取死信队列名称
     */
    public String getDeadLetterQueueName() {
        return topic + "_DLQ_" + consumerGroup;
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
    public DeadLetterRecord copy() {
        DeadLetterRecord copy = new DeadLetterRecord();
        copy.messageId = this.messageId;
        copy.consumerGroup = this.consumerGroup;
        copy.topic = this.topic;
        copy.queueId = this.queueId;
        copy.retryCount = this.retryCount;
        copy.failureReason = this.failureReason;
        copy.createTime = this.createTime;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.deadLetterType = this.deadLetterType;
        copy.consumerId = this.consumerId;
        copy.messageOffset = this.messageOffset;
        copy.originalMessage = this.originalMessage;
        copy.status = this.status;
        copy.remark = this.remark;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeadLetterRecord that = (DeadLetterRecord) o;
        return Objects.equals(messageId, that.messageId) && 
               Objects.equals(consumerGroup, that.consumerGroup);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(messageId, consumerGroup);
    }
    
    @Override
    public String toString() {
        return "DeadLetterRecord{" +
                "messageId='" + messageId + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", retryCount=" + retryCount +
                ", deadLetterType=" + deadLetterType +
                ", status=" + status +
                ", createTime=" + createTime +
                ", deadLetterAge=" + getDeadLetterAge() + "ms" +
                ", failureReason='" + failureReason + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", messageOffset=" + messageOffset +
                ", remark='" + remark + '\'' +
                '}';
    }
}
