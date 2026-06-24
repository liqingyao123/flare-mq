package com.ruyuan.mq.client.producer;

/**
 * Producer配置类
 * 
 * @author RuYuan MQ Team
 */
public class ProducerConfig {
    
    /**
     * Producer组名
     */
    private String producerGroup;
    
    /**
     * NameServer地址
     */
    private String nameServerAddr;
    
    /**
     * 发送超时时间（毫秒）
     */
    private long sendMsgTimeout;
    
    /**
     * 最大重试次数
     */
    private int retryTimesWhenSendFailed;
    
    /**
     * 异步发送时最大重试次数
     */
    private int retryTimesWhenSendAsyncFailed;
    
    /**
     * 消息最大大小（字节）
     */
    private int maxMessageSize;
    
    /**
     * 压缩消息体阈值（字节）
     */
    private int compressMsgBodyOverHowmuch;
    
    /**
     * 默认Topic队列数量
     */
    private int defaultTopicQueueNums;
    
    /**
     * 发送延迟容错开关
     */
    private boolean sendLatencyFaultEnable;
    
    /**
     * 批量发送最大消息数量
     */
    private int maxBatchSize;
    
    /**
     * 批量发送最大等待时间（毫秒）
     */
    private long batchMaxWaitTime;
    
    /**
     * 是否启用VIP通道
     */
    private boolean vipChannelEnabled;
    
    /**
     * 客户端回调执行线程数
     */
    private int clientCallbackExecutorThreads;
    
    /**
     * 默认构造函数
     */
    public ProducerConfig() {
        this.producerGroup = "DEFAULT_PRODUCER";
        this.nameServerAddr = "localhost:9876";
        this.sendMsgTimeout = 3000;
        this.retryTimesWhenSendFailed = 2;
        this.retryTimesWhenSendAsyncFailed = 2;
        this.maxMessageSize = 4 * 1024 * 1024; // 4MB
        this.compressMsgBodyOverHowmuch = 4 * 1024; // 4KB
        this.defaultTopicQueueNums = 4;
        this.sendLatencyFaultEnable = false;
        this.maxBatchSize = 32;
        this.batchMaxWaitTime = 10;
        this.vipChannelEnabled = false;
        this.clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    }
    
    // Getter和Setter方法
    
    public String getProducerGroup() {
        return producerGroup;
    }
    
    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
    
    public String getNameServerAddr() {
        return nameServerAddr;
    }
    
    public void setNameServerAddr(String nameServerAddr) {
        this.nameServerAddr = nameServerAddr;
    }
    
    public long getSendMsgTimeout() {
        return sendMsgTimeout;
    }
    
    public void setSendMsgTimeout(long sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }
    
    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }
    
    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }
    
    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }
    
    public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }
    
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
    
    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }
    
    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }
    
    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }
    
    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }
    
    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }
    
    public void setSendLatencyFaultEnable(boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }
    
    public int getMaxBatchSize() {
        return maxBatchSize;
    }
    
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
    
    public long getBatchMaxWaitTime() {
        return batchMaxWaitTime;
    }
    
    public void setBatchMaxWaitTime(long batchMaxWaitTime) {
        this.batchMaxWaitTime = batchMaxWaitTime;
    }
    
    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }
    
    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }
    
    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }
    
    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }
    
    /**
     * 验证配置是否有效
     */
    public boolean isValid() {
        return producerGroup != null && !producerGroup.trim().isEmpty() &&
               nameServerAddr != null && !nameServerAddr.trim().isEmpty() &&
               sendMsgTimeout > 0 &&
               maxMessageSize > 0 &&
               retryTimesWhenSendFailed >= 0 &&
               retryTimesWhenSendAsyncFailed >= 0;
    }
    
    /**
     * 复制配置
     */
    public ProducerConfig copy() {
        ProducerConfig copy = new ProducerConfig();
        copy.producerGroup = this.producerGroup;
        copy.nameServerAddr = this.nameServerAddr;
        copy.sendMsgTimeout = this.sendMsgTimeout;
        copy.retryTimesWhenSendFailed = this.retryTimesWhenSendFailed;
        copy.retryTimesWhenSendAsyncFailed = this.retryTimesWhenSendAsyncFailed;
        copy.maxMessageSize = this.maxMessageSize;
        copy.compressMsgBodyOverHowmuch = this.compressMsgBodyOverHowmuch;
        copy.defaultTopicQueueNums = this.defaultTopicQueueNums;
        copy.sendLatencyFaultEnable = this.sendLatencyFaultEnable;
        copy.maxBatchSize = this.maxBatchSize;
        copy.batchMaxWaitTime = this.batchMaxWaitTime;
        copy.vipChannelEnabled = this.vipChannelEnabled;
        copy.clientCallbackExecutorThreads = this.clientCallbackExecutorThreads;
        return copy;
    }
    
    @Override
    public String toString() {
        return "ProducerConfig{" +
                "producerGroup='" + producerGroup + '\'' +
                ", nameServerAddr='" + nameServerAddr + '\'' +
                ", sendMsgTimeout=" + sendMsgTimeout +
                ", retryTimesWhenSendFailed=" + retryTimesWhenSendFailed +
                ", retryTimesWhenSendAsyncFailed=" + retryTimesWhenSendAsyncFailed +
                ", maxMessageSize=" + maxMessageSize +
                ", compressMsgBodyOverHowmuch=" + compressMsgBodyOverHowmuch +
                ", defaultTopicQueueNums=" + defaultTopicQueueNums +
                ", sendLatencyFaultEnable=" + sendLatencyFaultEnable +
                ", maxBatchSize=" + maxBatchSize +
                ", batchMaxWaitTime=" + batchMaxWaitTime +
                ", vipChannelEnabled=" + vipChannelEnabled +
                ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads +
                '}';
    }
}
