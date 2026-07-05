package com.flare.mq.broker.ack;

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
    private AckResultStatus status;
    
    /**
     * 成功确认的消息ID列表
     */
    private List<String> successMessageIds;
    
    /**
     * 失败确认的消息ID列表
     */
    private List<String> failedMessageIds;
    
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
     * 处理耗时（毫秒）
     */
    private long costTime;
    
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
    public AckResult(AckResultStatus status, List<String> successMessageIds) {
        this();
        this.status = status;
        this.successMessageIds = successMessageIds != null ? successMessageIds : new ArrayList<>();
    }
    
    /**
     * 失败结果构造函数
     */
    public AckResult(AckResultStatus status, String errorMessage, String messageId) {
        this();
        this.status = status;
        this.errorMessage = errorMessage;
        if (messageId != null) {
            this.failedMessageIds.add(messageId);
        }
    }
    
    // Getter和Setter方法
    
    public AckResultStatus getStatus() {
        return status;
    }
    
    public void setStatus(AckResultStatus status) {
        this.status = status;
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
    
    public long getCostTime() {
        return costTime;
    }
    
    public void setCostTime(long costTime) {
        this.costTime = costTime;
    }
    
    /**
     * 检查是否成功
     */
    public boolean isSuccess() {
        return status == AckResultStatus.SUCCESS;
    }
    
    /**
     * 检查是否部分成功
     */
    public boolean isPartialSuccess() {
        return status == AckResultStatus.PARTIAL_SUCCESS;
    }
    
    /**
     * 检查是否失败
     */
    public boolean isFailure() {
        return status == AckResultStatus.FAILURE;
    }
    
    /**
     * 获取成功数量
     */
    public int getSuccessCount() {
        return successMessageIds.size();
    }
    
    /**
     * 获取失败数量
     */
    public int getFailedCount() {
        return failedMessageIds.size();
    }
    
    /**
     * 获取总数量
     */
    public int getTotalCount() {
        return getSuccessCount() + getFailedCount();
    }
    
    /**
     * 添加成功消息ID
     */
    public void addSuccessMessageId(String messageId) {
        if (messageId != null) {
            successMessageIds.add(messageId);
        }
    }
    
    /**
     * 添加失败消息ID
     */
    public void addFailedMessageId(String messageId) {
        if (messageId != null) {
            failedMessageIds.add(messageId);
        }
    }
    
    /**
     * 创建成功结果
     */
    public static AckResult success(String messageId) {
        List<String> successIds = new ArrayList<>();
        successIds.add(messageId);
        return new AckResult(AckResultStatus.SUCCESS, successIds);
    }
    
    /**
     * 创建批量成功结果
     */
    public static AckResult success(List<String> messageIds) {
        return new AckResult(AckResultStatus.SUCCESS, messageIds);
    }
    
    /**
     * 创建部分成功结果
     */
    public static AckResult partialSuccess(List<String> successIds, List<String> failedIds) {
        AckResult result = new AckResult(AckResultStatus.PARTIAL_SUCCESS, successIds);
        result.setFailedMessageIds(failedIds);
        return result;
    }
    
    /**
     * 创建失败结果
     */
    public static AckResult failure(String errorMessage, String messageId) {
        return new AckResult(AckResultStatus.FAILURE, errorMessage, messageId);
    }
    
    /**
     * 创建消息不存在结果
     */
    public static AckResult messageNotExist(String messageId) {
        AckResult result = new AckResult(AckResultStatus.MESSAGE_NOT_EXIST, "消息不存在", messageId);
        result.setErrorCode(-1);
        return result;
    }
    
    /**
     * 创建消息已确认结果
     */
    public static AckResult alreadyAcked(String messageId) {
        AckResult result = new AckResult(AckResultStatus.ALREADY_ACKED, "消息已确认", messageId);
        result.setErrorCode(-2);
        return result;
    }
    
    /**
     * 创建超时结果
     */
    public static AckResult timeout(String messageId) {
        AckResult result = new AckResult(AckResultStatus.TIMEOUT, "确认超时", messageId);
        result.setErrorCode(-3);
        return result;
    }
    
    @Override
    public String toString() {
        return "AckResult{" +
                "status=" + status +
                ", successCount=" + getSuccessCount() +
                ", failedCount=" + getFailedCount() +
                ", totalCount=" + getTotalCount() +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorCode=" + errorCode +
                ", ackTime=" + ackTime +
                ", costTime=" + costTime +
                '}';
    }
}
