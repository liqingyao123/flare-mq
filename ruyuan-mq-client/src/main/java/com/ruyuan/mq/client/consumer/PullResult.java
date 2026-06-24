package com.ruyuan.mq.client.consumer;

import com.ruyuan.mq.client.producer.Message;
import java.util.List;
import java.util.ArrayList;

/**
 * 拉取结果
 * 
 * @author RuYuan MQ Team
 */
public class PullResult {
    
    /**
     * 拉取状态
     */
    private PullStatus pullStatus;
    
    /**
     * 下一次拉取的偏移量
     */
    private long nextBeginOffset;
    
    /**
     * 最小偏移量
     */
    private long minOffset;
    
    /**
     * 最大偏移量
     */
    private long maxOffset;
    
    /**
     * 拉取到的消息列表
     */
    private List<Message> messages;
    
    /**
     * 拉取耗时（毫秒）
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
     * 拉取时间
     */
    private long pullTime;
    
    /**
     * 默认构造函数
     */
    public PullResult() {
        this.messages = new ArrayList<>();
        this.pullTime = System.currentTimeMillis();
    }
    
    /**
     * 成功结果构造函数
     */
    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<Message> messages) {
        this();
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messages = messages != null ? messages : new ArrayList<>();
    }
    
    /**
     * 失败结果构造函数
     */
    public PullResult(PullStatus pullStatus, String errorMessage, int errorCode) {
        this();
        this.pullStatus = pullStatus;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }
    
    // Getter和Setter方法
    
    public PullStatus getPullStatus() {
        return pullStatus;
    }
    
    public void setPullStatus(PullStatus pullStatus) {
        this.pullStatus = pullStatus;
    }
    
    public long getNextBeginOffset() {
        return nextBeginOffset;
    }
    
    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }
    
    public long getMinOffset() {
        return minOffset;
    }
    
    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }
    
    public long getMaxOffset() {
        return maxOffset;
    }
    
    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }
    
    public List<Message> getMessages() {
        return messages;
    }
    
    public void setMessages(List<Message> messages) {
        this.messages = messages != null ? messages : new ArrayList<>();
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
    
    public long getPullTime() {
        return pullTime;
    }
    
    public void setPullTime(long pullTime) {
        this.pullTime = pullTime;
    }
    
    /**
     * 检查拉取是否成功
     */
    public boolean isSuccess() {
        return pullStatus == PullStatus.FOUND || pullStatus == PullStatus.NO_NEW_MSG;
    }
    
    /**
     * 检查是否有消息
     */
    public boolean hasMessage() {
        return messages != null && !messages.isEmpty();
    }
    
    /**
     * 获取消息数量
     */
    public int getMessageCount() {
        return messages != null ? messages.size() : 0;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return pullStatus == PullStatus.OFFSET_ILLEGAL || pullStatus == PullStatus.BROKER_TIMEOUT;
    }
    
    /**
     * 创建成功结果（有消息）
     */
    public static PullResult found(long nextBeginOffset, long minOffset, long maxOffset, List<Message> messages) {
        return new PullResult(PullStatus.FOUND, nextBeginOffset, minOffset, maxOffset, messages);
    }
    
    /**
     * 创建成功结果（无新消息）
     */
    public static PullResult noNewMessage(long nextBeginOffset, long minOffset, long maxOffset) {
        return new PullResult(PullStatus.NO_NEW_MSG, nextBeginOffset, minOffset, maxOffset, null);
    }
    
    /**
     * 创建失败结果
     */
    public static PullResult failure(String errorMessage) {
        return new PullResult(PullStatus.PULL_FAILED, errorMessage, -1);
    }
    
    /**
     * 创建失败结果（带错误码）
     */
    public static PullResult failure(String errorMessage, int errorCode) {
        return new PullResult(PullStatus.PULL_FAILED, errorMessage, errorCode);
    }
    
    /**
     * 创建偏移量非法结果
     */
    public static PullResult offsetIllegal(long nextBeginOffset, long minOffset, long maxOffset) {
        PullResult result = new PullResult(PullStatus.OFFSET_ILLEGAL, "偏移量非法", -2);
        result.nextBeginOffset = nextBeginOffset;
        result.minOffset = minOffset;
        result.maxOffset = maxOffset;
        return result;
    }
    
    /**
     * 创建超时结果
     */
    public static PullResult timeout() {
        return new PullResult(PullStatus.BROKER_TIMEOUT, "拉取超时", -3);
    }
    
    /**
     * 创建Topic不存在结果
     */
    public static PullResult topicNotExist() {
        return new PullResult(PullStatus.TOPIC_NOT_EXIST, "Topic不存在", -4);
    }
    
    @Override
    public String toString() {
        return "PullResult{" +
                "pullStatus=" + pullStatus +
                ", nextBeginOffset=" + nextBeginOffset +
                ", minOffset=" + minOffset +
                ", maxOffset=" + maxOffset +
                ", messageCount=" + getMessageCount() +
                ", costTime=" + costTime +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorCode=" + errorCode +
                ", pullTime=" + pullTime +
                '}';
    }
}
