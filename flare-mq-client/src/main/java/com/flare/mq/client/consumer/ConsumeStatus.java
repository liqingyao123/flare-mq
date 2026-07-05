package com.flare.mq.client.consumer;

/**
 * 消费状态枚举
 * 
 * @author FlareMQ Team
 */
public enum ConsumeStatus {
    
    /**
     * 消费成功
     */
    CONSUME_SUCCESS("CONSUME_SUCCESS", "消费成功"),
    
    /**
     * 消费失败，稍后重试
     */
    RECONSUME_LATER("RECONSUME_LATER", "消费失败，稍后重试"),
    
    /**
     * 消费异常，需要重试
     */
    CONSUME_EXCEPTION("CONSUME_EXCEPTION", "消费异常"),
    
    /**
     * 消费超时
     */
    CONSUME_TIMEOUT("CONSUME_TIMEOUT", "消费超时");
    
    private final String code;
    private final String description;
    
    ConsumeStatus(String code, String description) {
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
    public static ConsumeStatus fromCode(String code) {
        if (code == null) {
            return CONSUME_EXCEPTION;
        }
        
        for (ConsumeStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return CONSUME_EXCEPTION;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == CONSUME_SUCCESS;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return this == RECONSUME_LATER || this == CONSUME_EXCEPTION;
    }
    
    /**
     * 检查是否为超时状态
     */
    public boolean isTimeout() {
        return this == CONSUME_TIMEOUT;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
