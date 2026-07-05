package com.flare.mq.client.producer;

/**
 * 发送结果
 * 
 * @author FlareMQ Team
 */
public class SendResult {
    
    /**
     * 发送状态
     */
    private SendStatus sendStatus;
    
    /**
     * 消息ID
     */
    private String messageId;
    
    /**
     * 消息队列ID
     */
    private int queueId;
    
    /**
     * 消息偏移量
     */
    private long queueOffset;
    
    /**
     * Broker地址
     */
    private String brokerAddr;
    
    /**
     * 发送耗时（毫秒）
     */
    private long costTime;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 错误码
     */
    private int errorCode;
    
    /**
     * 发送时间
     */
    private long sendTime;
    
    /**
     * 默认构造函数
     */
    public SendResult() {
        this.sendTime = System.currentTimeMillis();
    }
    
    /**
     * 成功结果构造函数
     */
    public SendResult(SendStatus sendStatus, String messageId, int queueId, long queueOffset) {
        this();
        this.sendStatus = sendStatus;
        this.messageId = messageId;
        this.queueId = queueId;
        this.queueOffset = queueOffset;
    }
    
    /**
     * 失败结果构造函数
     */
    public SendResult(SendStatus sendStatus, String errorMessage, int errorCode) {
        this();
        this.sendStatus = sendStatus;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }
    
    // Getter和Setter方法
    
    public SendStatus getSendStatus() {
        return sendStatus;
    }
    
    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
    
    public long getQueueOffset() {
        return queueOffset;
    }
    
    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }
    
    public String getBrokerAddr() {
        return brokerAddr;
    }
    
    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }
    
    public long getCostTime() {
        return costTime;
    }
    
    public void setCostTime(long costTime) {
        this.costTime = costTime;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public int getErrorCode() {
        return errorCode;
    }
    
    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }
    
    public long getSendTime() {
        return sendTime;
    }
    
    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }
    
    /**
     * 检查发送是否成功
     */
    public boolean isSuccess() {
        return sendStatus == SendStatus.SEND_OK;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return sendStatus == SendStatus.FLUSH_DISK_TIMEOUT ||
               sendStatus == SendStatus.FLUSH_SLAVE_TIMEOUT ||
               sendStatus == SendStatus.SLAVE_NOT_AVAILABLE;
    }
    
    /**
     * 创建成功结果
     */
    public static SendResult success(String messageId, int queueId, long queueOffset) {
        return new SendResult(SendStatus.SEND_OK, messageId, queueId, queueOffset);
    }
    
    /**
     * 创建失败结果
     */
    public static SendResult failure(String errorMessage) {
        return new SendResult(SendStatus.SEND_FAILED, errorMessage, -1);
    }
    
    /**
     * 创建失败结果（带错误码）
     */
    public static SendResult failure(String errorMessage, int errorCode) {
        return new SendResult(SendStatus.SEND_FAILED, errorMessage, errorCode);
    }
    
    /**
     * 创建超时结果
     */
    public static SendResult timeout(String errorMessage) {
        return new SendResult(SendStatus.SEND_TIMEOUT, errorMessage, -2);
    }
    
    /**
     * 创建刷盘超时结果
     */
    public static SendResult flushDiskTimeout() {
        return new SendResult(SendStatus.FLUSH_DISK_TIMEOUT, "刷盘超时", -3);
    }
    
    /**
     * 创建从节点超时结果
     */
    public static SendResult flushSlaveTimeout() {
        return new SendResult(SendStatus.FLUSH_SLAVE_TIMEOUT, "从节点同步超时", -4);
    }
    
    /**
     * 创建从节点不可用结果
     */
    public static SendResult slaveNotAvailable() {
        return new SendResult(SendStatus.SLAVE_NOT_AVAILABLE, "从节点不可用", -5);
    }
    
    @Override
    public String toString() {
        return "SendResult{" +
                "sendStatus=" + sendStatus +
                ", messageId='" + messageId + '\'' +
                ", queueId=" + queueId +
                ", queueOffset=" + queueOffset +
                ", brokerAddr='" + brokerAddr + '\'' +
                ", costTime=" + costTime +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorCode=" + errorCode +
                ", sendTime=" + sendTime +
                '}';
    }
}
