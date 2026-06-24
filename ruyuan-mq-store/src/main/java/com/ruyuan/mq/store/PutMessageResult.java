package com.ruyuan.mq.store;

/**
 * 存储消息结果
 * 
 * @author RuYuan MQ Team
 */
public class PutMessageResult {
    
    /**
     * 存储状态
     */
    private PutMessageStatus putMessageStatus;
    
    /**
     * 追加消息结果
     */
    private AppendMessageResult appendMessageResult;
    
    /**
     * 构造函数
     */
    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }
    
    /**
     * 是否成功
     */
    public boolean isOk() {
        return putMessageStatus == PutMessageStatus.PUT_OK;
    }
    
    // ========== Getter和Setter方法 ==========
    
    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }
    
    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }
    
    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }
    
    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }
    
    @Override
    public String toString() {
        return "PutMessageResult{" +
                "putMessageStatus=" + putMessageStatus +
                ", appendMessageResult=" + appendMessageResult +
                '}';
    }
}

/**
 * 存储消息状态枚举
 */
enum PutMessageStatus {
    /**
     * 存储成功
     */
    PUT_OK,
    
    /**
     * 刷盘超时
     */
    FLUSH_DISK_TIMEOUT,
    
    /**
     * 同步双写超时
     */
    FLUSH_SLAVE_TIMEOUT,
    
    /**
     * 从服务器不可用
     */
    SLAVE_NOT_AVAILABLE,
    
    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,
    
    /**
     * 创建文件失败
     */
    CREATE_MAPPED_FILE_FAILED,
    
    /**
     * 消息非法
     */
    MESSAGE_ILLEGAL,
    
    /**
     * 存储消息失败
     */
    PUT_MESSAGE_FAILED,
    
    /**
     * 未知错误
     */
    UNKNOWN_ERROR
}
