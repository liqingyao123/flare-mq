package com.flare.mq.client.consumer;

/**
 * Consumer配置类
 * 
 * @author FlareMQ Team
 */
public class ConsumerConfig {
    
    /**
     * Consumer组名
     */
    private String consumerGroup;
    
    /**
     * NameServer地址
     */
    private String nameServerAddr;
    
    /**
     * 消费模式
     */
    private ConsumeMode consumeMode;
    
    /**
     * 消费类型
     */
    private ConsumeType consumeType;
    
    /**
     * 拉取消息超时时间（毫秒）
     */
    private long pullMsgTimeout;
    
    /**
     * 消费超时时间（毫秒）
     */
    private long consumeTimeout;
    
    /**
     * 最大重试次数
     */
    private int maxRetryTimes;
    
    /**
     * 批量拉取最大消息数量
     */
    private int pullBatchSize;
    
    /**
     * 消费线程数量
     */
    private int consumeThreadNums;
    
    /**
     * 消费线程最大数量
     */
    private int consumeThreadMax;
    
    /**
     * 消费队列阈值
     */
    private int consumeQueueThreshold;
    
    /**
     * 拉取间隔时间（毫秒）
     */
    private long pullInterval;
    
    /**
     * 是否启用顺序消费
     */
    private boolean orderedConsume;
    
    /**
     * 消费起始位置
     */
    private ConsumeFromWhere consumeFromWhere;
    
    /**
     * 消费时间戳（当consumeFromWhere为CONSUME_FROM_TIMESTAMP时使用）
     */
    private String consumeTimestamp;
    
    /**
     * 默认构造函数
     */
    public ConsumerConfig() {
        this.consumerGroup = "DEFAULT_CONSUMER";
        this.nameServerAddr = "localhost:9876";
        this.consumeMode = ConsumeMode.CLUSTERING;
        this.consumeType = ConsumeType.CONSUME_ACTIVELY;
        this.pullMsgTimeout = 3000;
        this.consumeTimeout = 15000;
        this.maxRetryTimes = 16;
        this.pullBatchSize = 32;
        this.consumeThreadNums = 20;
        this.consumeThreadMax = 64;
        this.consumeQueueThreshold = 1000;
        this.pullInterval = 0;
        this.orderedConsume = false;
        this.consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        this.consumeTimestamp = "20231201080000";
    }
    
    // Getter和Setter方法
    
    public String getConsumerGroup() {
        return consumerGroup;
    }
    
    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
    
    public String getNameServerAddr() {
        return nameServerAddr;
    }
    
    public void setNameServerAddr(String nameServerAddr) {
        this.nameServerAddr = nameServerAddr;
    }
    
    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }
    
    public void setConsumeMode(ConsumeMode consumeMode) {
        this.consumeMode = consumeMode;
    }
    
    public ConsumeType getConsumeType() {
        return consumeType;
    }
    
    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }
    
    public long getPullMsgTimeout() {
        return pullMsgTimeout;
    }
    
    public void setPullMsgTimeout(long pullMsgTimeout) {
        this.pullMsgTimeout = pullMsgTimeout;
    }
    
    public long getConsumeTimeout() {
        return consumeTimeout;
    }
    
    public void setConsumeTimeout(long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }
    
    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }
    
    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }
    
    public int getPullBatchSize() {
        return pullBatchSize;
    }
    
    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }
    
    public int getConsumeThreadNums() {
        return consumeThreadNums;
    }
    
    public void setConsumeThreadNums(int consumeThreadNums) {
        this.consumeThreadNums = consumeThreadNums;
    }
    
    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }
    
    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }
    
    public int getConsumeQueueThreshold() {
        return consumeQueueThreshold;
    }
    
    public void setConsumeQueueThreshold(int consumeQueueThreshold) {
        this.consumeQueueThreshold = consumeQueueThreshold;
    }
    
    public long getPullInterval() {
        return pullInterval;
    }
    
    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }
    
    public boolean isOrderedConsume() {
        return orderedConsume;
    }
    
    public void setOrderedConsume(boolean orderedConsume) {
        this.orderedConsume = orderedConsume;
    }
    
    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }
    
    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
    
    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }
    
    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }
    
    /**
     * 验证配置是否有效
     */
    public boolean isValid() {
        return consumerGroup != null && !consumerGroup.trim().isEmpty() &&
               nameServerAddr != null && !nameServerAddr.trim().isEmpty() &&
               pullMsgTimeout > 0 &&
               consumeTimeout > 0 &&
               maxRetryTimes >= 0 &&
               pullBatchSize > 0 &&
               consumeThreadNums > 0 &&
               consumeThreadMax >= consumeThreadNums;
    }
    
    /**
     * 复制配置
     */
    public ConsumerConfig copy() {
        ConsumerConfig copy = new ConsumerConfig();
        copy.consumerGroup = this.consumerGroup;
        copy.nameServerAddr = this.nameServerAddr;
        copy.consumeMode = this.consumeMode;
        copy.consumeType = this.consumeType;
        copy.pullMsgTimeout = this.pullMsgTimeout;
        copy.consumeTimeout = this.consumeTimeout;
        copy.maxRetryTimes = this.maxRetryTimes;
        copy.pullBatchSize = this.pullBatchSize;
        copy.consumeThreadNums = this.consumeThreadNums;
        copy.consumeThreadMax = this.consumeThreadMax;
        copy.consumeQueueThreshold = this.consumeQueueThreshold;
        copy.pullInterval = this.pullInterval;
        copy.orderedConsume = this.orderedConsume;
        copy.consumeFromWhere = this.consumeFromWhere;
        copy.consumeTimestamp = this.consumeTimestamp;
        return copy;
    }
    
    @Override
    public String toString() {
        return "ConsumerConfig{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", nameServerAddr='" + nameServerAddr + '\'' +
                ", consumeMode=" + consumeMode +
                ", consumeType=" + consumeType +
                ", pullMsgTimeout=" + pullMsgTimeout +
                ", consumeTimeout=" + consumeTimeout +
                ", maxRetryTimes=" + maxRetryTimes +
                ", pullBatchSize=" + pullBatchSize +
                ", consumeThreadNums=" + consumeThreadNums +
                ", consumeThreadMax=" + consumeThreadMax +
                ", consumeQueueThreshold=" + consumeQueueThreshold +
                ", pullInterval=" + pullInterval +
                ", orderedConsume=" + orderedConsume +
                ", consumeFromWhere=" + consumeFromWhere +
                ", consumeTimestamp='" + consumeTimestamp + '\'' +
                '}';
    }
}
