package com.ruyuan.mq.store;

/**
 * 追加消息结果
 * 
 * @author RuYuan MQ Team
 */
public class AppendMessageResult {
    
    /**
     * 追加状态
     */
    private AppendMessageStatus status;
    
    /**
     * 消息在CommitLog中的偏移量
     */
    private long wroteOffset;
    
    /**
     * 消息大小
     */
    private int wroteBytes;
    
    /**
     * 构造函数
     */
    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
    }
    
    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return status == AppendMessageStatus.SUCCESS;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public AppendMessageStatus getStatus() {
        return status;
    }
    
    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }
    
    public long getWroteOffset() {
        return wroteOffset;
    }
    
    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }
    
    public int getWroteBytes() {
        return wroteBytes;
    }
    
    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }
    
    @Override
    public String toString() {
        return "AppendMessageResult{" +
                "status=" + status +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                '}';
    }
}

/**
 * 追加消息状态枚举
 */
enum AppendMessageStatus {
    /**
     * 成功
     */
    SUCCESS,
    
    /**
     * 序列化错误
     */
    SERIALIZE_ERROR,
    
    /**
     * 创建文件错误
     */
    CREATE_FILE_ERROR,
    
    /**
     * 追加错误
     */
    APPEND_ERROR,
    
    /**
     * 文件已满
     */
    FILE_FULL,
    
    /**
     * 未知错误
     */
    UNKNOWN_ERROR
}
