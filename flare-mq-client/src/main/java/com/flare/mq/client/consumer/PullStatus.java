package com.flare.mq.client.consumer;

/**
 * 拉取状态枚举
 * 
 * @author FlareMQ Team
 */
public enum PullStatus {
    
    /**
     * 拉取到消息
     */
    FOUND("FOUND", "拉取到消息"),
    
    /**
     * 没有新消息
     */
    NO_NEW_MSG("NO_NEW_MSG", "没有新消息"),
    
    /**
     * 偏移量非法
     */
    OFFSET_ILLEGAL("OFFSET_ILLEGAL", "偏移量非法"),
    
    /**
     * Broker超时
     */
    BROKER_TIMEOUT("BROKER_TIMEOUT", "Broker超时"),
    
    /**
     * Topic不存在
     */
    TOPIC_NOT_EXIST("TOPIC_NOT_EXIST", "Topic不存在"),
    
    /**
     * 拉取失败
     */
    PULL_FAILED("PULL_FAILED", "拉取失败");
    
    private final String code;
    private final String description;
    
    PullStatus(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * 根据代码获取状态
     */
    public static PullStatus fromCode(String code) {
        if (code == null) {
            return PULL_FAILED;
        }
        
        for (PullStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return PULL_FAILED;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == FOUND || this == NO_NEW_MSG;
    }
    
    /**
     * 检查是否有消息
     */
    public boolean hasMessage() {
        return this == FOUND;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return this == OFFSET_ILLEGAL || this == BROKER_TIMEOUT;
    }
    
    /**
     * 检查是否为超时状态
     */
    public boolean isTimeout() {
        return this == BROKER_TIMEOUT;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
