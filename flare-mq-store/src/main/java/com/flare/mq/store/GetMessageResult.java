package com.flare.mq.store;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取消息结果
 * 
 * @author FlareMQ Team
 */
public class GetMessageResult {
    
    /**
     * 获取状态
     */
    private GetMessageStatus status;
    
    /**
     * 消息列表
     */
    private List<Message> messageList;
    
    /**
     * 下一个开始偏移量
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
     * 构造函数
     */
    public GetMessageResult(GetMessageStatus status) {
        this.status = status;
        this.messageList = new ArrayList<>();
    }
    
    /**
     * 添加消息
     */
    public void addMessage(Message message) {
        if (messageList == null) {
            messageList = new ArrayList<>();
        }
        messageList.add(message);
    }
    
    /**
     * 获取消息数量
     */
    public int getMessageCount() {
        return messageList != null ? messageList.size() : 0;
    }
    
    /**
     * 是否找到消息
     */
    public boolean isFound() {
        return status == GetMessageStatus.FOUND && getMessageCount() > 0;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public GetMessageStatus getStatus() {
        return status;
    }
    
    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }
    
    public List<Message> getMessageList() {
        return messageList;
    }
    
    public void setMessageList(List<Message> messageList) {
        this.messageList = messageList;
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
    
    @Override
    public String toString() {
        return "GetMessageResult{" +
                "status=" + status +
                ", messageCount=" + getMessageCount() +
                ", nextBeginOffset=" + nextBeginOffset +
                ", minOffset=" + minOffset +
                ", maxOffset=" + maxOffset +
                '}';
    }
}

/**
 * 获取消息状态枚举
 */
enum GetMessageStatus {
    /**
     * 找到消息
     */
    FOUND,
    
    /**
     * 队列中没有消息
     */
    NO_MESSAGE_IN_QUEUE,
    
    /**
     * 没有匹配的消息
     */
    NO_MATCHED_MESSAGE,
    
    /**
     * 偏移量过小
     */
    OFFSET_TOO_SMALL,
    
    /**
     * 偏移量过大
     */
    OFFSET_OVERFLOW,
    
    /**
     * 偏移量非法
     */
    OFFSET_ILLEGAL,
    
    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,
    
    /**
     * 未知错误
     */
    UNKNOWN_ERROR
}
