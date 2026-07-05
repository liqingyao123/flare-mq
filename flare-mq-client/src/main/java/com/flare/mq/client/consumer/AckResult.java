package com.flare.mq.client.consumer;

import java.util.List;
import java.util.ArrayList;

/**
 * 确认结果
 * 
 * @author FlareMQ Team
 */
public class AckResult {
    
    /**
     * 确认状态
     */
    private AckStatus ackStatus;
    
    /**
     * 成功确认的消息ID列表
     */
    private List<String> successMessageIds;
    
    /**
     * 失败确认的消息ID列表
     */
    private List<String> failedMessageIds;
    
    /**
     * 确认耗时（毫秒）
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
     * 确认时间
     */
    private long ackTime;
    
    /**
     * 默认构造函数
     */
    public AckResult() {
        this.successMessageIds = new ArrayList<>();
        this.failedMessageIds = new ArrayList<>();
        this.ackTime = System.currentTimeMillis();
    }
    
    /**
     * 成功结果构造函数
     */
    public AckResult(AckStatus ackStatus, List<String> successMessageIds) {
        this();
        this.ackStatus = ackStatus;
        this.successMessageIds = successMessageIds != null ? successMessageIds : new ArrayList<>();
    }
    
    /**
     * 失败结果构造函数
     */
    public AckResult(AckStatus ackStatus, String errorMessage, int errorCode) {
        this();
        this.ackStatus = ackStatus;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }
    
    // Getter和Setter方法
    
    public AckStatus getAckStatus() {
        return ackStatus;
    }
    
    public void setAckStatus(AckStatus ackStatus) {
        this.ackStatus = ackStatus;
    }
    
    public List<String> getSuccessMessageIds() {
        return successMessageIds;
    }
    
    public void setSuccessMessageIds(List<String> successMessageIds) {
        this.successMessageIds = successMessageIds != null ? successMessageIds : new ArrayList<>();
    }
    
    public List<String> getFailedMessageIds() {
        return failedMessageIds;
    }
    
    public void setFailedMessageIds(List<String> failedMessageIds) {
        this.failedMessageIds = failedMessageIds != null ? failedMessageIds : new ArrayList<>();
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
    
    public long getAckTime() {
        return ackTime;
    }
    
    public void setAckTime(long ackTime) {
        this.ackTime = ackTime;
    }
    
    /**
     * 检查确认是否成功
     */
    public boolean isSuccess() {
        return ackStatus == AckStatus.ACK_OK;
    }
    
    /**
     * 检查是否有成功确认的消息
     */
    public boolean hasSuccessMessage() {
        return successMessageIds != null && !successMessageIds.isEmpty();
    }
    
    /**
     * 检查是否有失败确认的消息
     */
    public boolean hasFailedMessage() {
        return failedMessageIds != null && !failedMessageIds.isEmpty();
    }
    
    /**
     * 获取成功确认的消息数量
     */
    public int getSuccessCount() {
        return successMessageIds != null ? successMessageIds.size() : 0;
    }
    
    /**
     * 获取失败确认的消息数量
     */
    public int getFailedCount() {
        return failedMessageIds != null ? failedMessageIds.size() : 0;
    }
    
    /**
     * 获取总确认数量
     */
    public int getTotalCount() {
        return getSuccessCount() + getFailedCount();
    }
    
    /**
     * 添加成功确认的消息ID
     */
    public void addSuccessMessageId(String messageId) {
        if (successMessageIds == null) {
            successMessageIds = new ArrayList<>();
        }
        successMessageIds.add(messageId);
    }
    
    /**
     * 添加失败确认的消息ID
     */
    public void addFailedMessageId(String messageId) {
        if (failedMessageIds == null) {
            failedMessageIds = new ArrayList<>();
        }
        failedMessageIds.add(messageId);
    }
    
    /**
     * 创建成功结果
     */
    public static AckResult success(String messageId) {
        List<String> successIds = new ArrayList<>();
        successIds.add(messageId);
        return new AckResult(AckStatus.ACK_OK, successIds);
    }
    
    /**
     * 创建批量成功结果
     */
    public static AckResult success(List<String> messageIds) {
        return new AckResult(AckStatus.ACK_OK, messageIds);
    }
    
    /**
     * 创建失败结果
     */
    public static AckResult failure(String errorMessage) {
        return new AckResult(AckStatus.ACK_FAILED, errorMessage, -1);
    }
    
    /**
     * 创建失败结果（带错误码）
     */
    public static AckResult failure(String errorMessage, int errorCode) {
        return new AckResult(AckStatus.ACK_FAILED, errorMessage, errorCode);
    }
    
    /**
     * 创建超时结果
     */
    public static AckResult timeout() {
        return new AckResult(AckStatus.ACK_TIMEOUT, "确认超时", -2);
    }
    
    /**
     * 创建消息不存在结果
     */
    public static AckResult messageNotExist() {
        return new AckResult(AckStatus.MESSAGE_NOT_EXIST, "消息不存在", -3);
    }
    
    @Override
    public String toString() {
        return "AckResult{" +
                "ackStatus=" + ackStatus +
                ", successCount=" + getSuccessCount() +
                ", failedCount=" + getFailedCount() +
                ", costTime=" + costTime +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorCode=" + errorCode +
                ", ackTime=" + ackTime +
                '}';
    }
}
