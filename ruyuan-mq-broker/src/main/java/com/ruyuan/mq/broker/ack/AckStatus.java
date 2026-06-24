package com.ruyuan.mq.broker.ack;

/**
 * 消息确认状态枚举
 * 
 * @author RuYuan MQ Team
 */
public enum AckStatus {
    
    /**
     * 待确认状态 - 消息已发送给消费者，等待确认
     */
    PENDING("PENDING", "待确认"),
    
    /**
     * 已确认状态 - 消费者已确认消息处理完成
     */
    ACKED("ACKED", "已确认"),
    
    /**
     * 重试中状态 - 消息正在重试
     */
    RETRYING("RETRYING", "重试中"),
    
    /**
     * 死信状态 - 消息超过最大重试次数，进入死信队列
     */
    DEAD_LETTER("DEAD_LETTER", "死信"),
    
    /**
     * 超时状态 - 消息确认超时
     */
    TIMEOUT("TIMEOUT", "超时");
    
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
            return PENDING;
        }
        
        for (AckStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return PENDING;
    }
    
    /**
     * 检查是否为最终状态
     */
    public boolean isFinalStatus() {
        return this == ACKED || this == DEAD_LETTER;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == ACKED;
    }
    
    /**
     * 检查是否为失败状态
     */
    public boolean isFailure() {
        return this == DEAD_LETTER || this == TIMEOUT;
    }
    
    /**
     * 检查是否需要重试
     */
    public boolean needRetry() {
        return this == PENDING || this == RETRYING || this == TIMEOUT;
    }
    
    /**
     * 检查是否可以确认
     */
    public boolean canAck() {
        return this == PENDING || this == RETRYING || this == TIMEOUT;
    }
    
    /**
     * 检查是否可以重试
     */
    public boolean canRetry() {
        return this == PENDING || this == TIMEOUT;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
