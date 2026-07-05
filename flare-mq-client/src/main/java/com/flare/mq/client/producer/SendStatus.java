package com.flare.mq.client.producer;

/**
 * 发送状态枚举
 * 
 * @author FlareMQ Team
 */
public enum SendStatus {
    
    /**
     * Send successful
     */
    SEND_OK("SEND_OK", "Send successful"),

    /**
     * Send failed
     */
    SEND_FAILED("SEND_FAILED", "Send failed"),

    /**
     * Send timeout
     */
    SEND_TIMEOUT("SEND_TIMEOUT", "Send timeout"),

    /**
     * Flush disk timeout
     */
    FLUSH_DISK_TIMEOUT("FLUSH_DISK_TIMEOUT", "Flush disk timeout"),

    /**
     * Slave sync timeout
     */
    FLUSH_SLAVE_TIMEOUT("FLUSH_SLAVE_TIMEOUT", "Slave sync timeout"),

    /**
     * Slave not available
     */
    SLAVE_NOT_AVAILABLE("SLAVE_NOT_AVAILABLE", "Slave not available");
    
    private final String code;
    private final String description;
    
    SendStatus(String code, String description) {
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
    public static SendStatus fromCode(String code) {
        if (code == null) {
            return SEND_FAILED;
        }
        
        for (SendStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return SEND_FAILED;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == SEND_OK;
    }
    
    /**
     * 检查是否为失败状态
     */
    public boolean isFailure() {
        return this == SEND_FAILED;
    }
    
    /**
     * 检查是否为超时状态
     */
    public boolean isTimeout() {
        return this == SEND_TIMEOUT || 
               this == FLUSH_DISK_TIMEOUT || 
               this == FLUSH_SLAVE_TIMEOUT;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return this == FLUSH_DISK_TIMEOUT ||
               this == FLUSH_SLAVE_TIMEOUT ||
               this == SLAVE_NOT_AVAILABLE;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
