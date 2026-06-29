package com.ruyuan.mq.broker.ack;

import java.util.Objects;

/**
 * 消息确认记录
 * 
 * @author RuYuan MQ Team
 */
public class AckRecord {
    
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
     * 确认状态
     */
    private AckStatus status;
    
    /**
     * 创建时间
     */
    private long createTime;
    
    /**
     * 确认时间
     */
    private long ackTime;
    
    /**
     * 最后更新时间
     */
    private long lastUpdateTime;
    
    /**
     * 消费者ID
     */
    private String consumerId;
    
    /**
     * 消息偏移量
     */
    private long messageOffset;

    /**
     * 消息在CommitLog中的存储大小
     */
    private long storeSize;

    /**
     * 备注信息
     */
    private String remark;
    
    /**
     * 默认构造函数
     */
    public AckRecord() {
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = this.createTime;
        this.status = AckStatus.PENDING;
    }
    
    /**
     * 构造函数
     */
    public AckRecord(String messageId, String consumerGroup, String topic, int queueId) {
        this();
        this.messageId = messageId;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.queueId = queueId;
    }
    
    /**
     * 完整构造函数
     */
    public AckRecord(String messageId, String consumerGroup, String topic, int queueId, 
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
    
    public AckStatus getStatus() {
        return status;
    }
    
    public void setStatus(AckStatus status) {
        this.status = status;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    public long getAckTime() {
        return ackTime;
    }
    
    public void setAckTime(long ackTime) {
        this.ackTime = ackTime;
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
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

    public long getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(long storeSize) {
        this.storeSize = storeSize;
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
     * 检查是否已确认
     */
    public boolean isAcked() {
        return status == AckStatus.ACKED;
    }
    
    /**
     * 检查是否待确认
     */
    public boolean isPending() {
        return status == AckStatus.PENDING;
    }
    
    /**
     * 检查是否为死信
     */
    public boolean isDeadLetter() {
        return status == AckStatus.DEAD_LETTER;
    }
    
    /**
     * 获取等待确认时间（毫秒）
     */
    public long getWaitingAckTime() {
        if (isAcked()) {
            return ackTime - createTime;
        } else {
            return System.currentTimeMillis() - createTime;
        }
    }
    
    /**
     * 检查是否超时
     */
    public boolean isTimeout(long timeoutMs) {
        return isPending() && getWaitingAckTime() > timeoutMs;
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
    public AckRecord copy() {
        AckRecord copy = new AckRecord();
        copy.messageId = this.messageId;
        copy.consumerGroup = this.consumerGroup;
        copy.topic = this.topic;
        copy.queueId = this.queueId;
        copy.status = this.status;
        copy.createTime = this.createTime;
        copy.ackTime = this.ackTime;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.consumerId = this.consumerId;
        copy.messageOffset = this.messageOffset;
        copy.remark = this.remark;
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AckRecord ackRecord = (AckRecord) o;
        return Objects.equals(messageId, ackRecord.messageId) && 
               Objects.equals(consumerGroup, ackRecord.consumerGroup);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(messageId, consumerGroup);
    }
    
    @Override
    public String toString() {
        return "AckRecord{" +
                "messageId='" + messageId + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", status=" + status +
                ", createTime=" + createTime +
                ", ackTime=" + ackTime +
                ", lastUpdateTime=" + lastUpdateTime +
                ", consumerId='" + consumerId + '\'' +
                ", messageOffset=" + messageOffset +
                ", waitingTime=" + getWaitingAckTime() + "ms" +
                ", remark='" + remark + '\'' +
                '}';
    }
}
