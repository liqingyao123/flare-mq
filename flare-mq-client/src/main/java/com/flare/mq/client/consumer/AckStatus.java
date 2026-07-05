package com.flare.mq.client.consumer;

/**
 * 确认状态枚举
 * 
 * @author FlareMQ Team
 */
public enum AckStatus {
    
    /**
     * 确认成功
     */
    ACK_OK("ACK_OK", "确认成功"),
    
    /**
     * 确认失败
     */
    ACK_FAILED("ACK_FAILED", "确认失败"),
    
    /**
     * 确认超时
     */
    ACK_TIMEOUT("ACK_TIMEOUT", "确认超时"),
    
    /**
     * 消息不存在
     */
    MESSAGE_NOT_EXIST("MESSAGE_NOT_EXIST", "消息不存在"),
    
    /**
     * 消息已确认
     */
    MESSAGE_ALREADY_ACKED("MESSAGE_ALREADY_ACKED", "消息已确认");
    
    private final String code;
    private final String description;
    
    AckStatus(String code, String description) {
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
    public static AckStatus fromCode(String code) {
        if (code == null) {
            return ACK_FAILED;
        }
        
        for (AckStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return ACK_FAILED;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == ACK_OK;
    }
    
    /**
     * 检查是否为失败状态
     */
    public boolean isFailure() {
        return this == ACK_FAILED;
    }
    
    /**
     * 检查是否为超时状态
     */
    public boolean isTimeout() {
        return this == ACK_TIMEOUT;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return this == ACK_FAILED || this == ACK_TIMEOUT;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
