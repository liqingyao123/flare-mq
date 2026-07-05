package com.flare.mq.broker.ack;

/**
 * 死信类型枚举
 * 
 * @author FlareMQ Team
 */
public enum DeadLetterType {
    
    /**
     * 重试耗尽 - 消息重试次数超过最大限制
     */
    RETRY_EXHAUSTED("RETRY_EXHAUSTED", "重试耗尽"),
    
    /**
     * 消费超时 - 消息消费超时
     */
    CONSUME_TIMEOUT("CONSUME_TIMEOUT", "消费超时"),
    
    /**
     * 消费异常 - 消费过程中发生异常
     */
    CONSUME_EXCEPTION("CONSUME_EXCEPTION", "消费异常"),
    
    /**
     * 消息过期 - 消息超过TTL时间
     */
    MESSAGE_EXPIRED("MESSAGE_EXPIRED", "消息过期"),
    
    /**
     * 队列满 - 目标队列已满
     */
    QUEUE_FULL("QUEUE_FULL", "队列满"),
    
    /**
     * 消费者不可用 - 没有可用的消费者
     */
    NO_CONSUMER("NO_CONSUMER", "消费者不可用"),
    
    /**
     * 手动移入 - 手动将消息移入死信队列
     */
    MANUAL_MOVE("MANUAL_MOVE", "手动移入");
    
    private final String code;
    private final String description;
    
    DeadLetterType(String code, String description) {
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
     * 根据代码获取死信类型
     */
    public static DeadLetterType fromCode(String code) {
        if (code == null) {
            return RETRY_EXHAUSTED;
        }
        
        for (DeadLetterType type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        
        return RETRY_EXHAUSTED;
    }
    
    /**
     * 检查是否为系统自动产生的死信
     */
    public boolean isSystemGenerated() {
        return this != MANUAL_MOVE;
    }
    
    /**
     * 检查是否为手动产生的死信
     */
    public boolean isManualGenerated() {
        return this == MANUAL_MOVE;
    }
    
    /**
     * 检查是否可以重新投递
     */
    public boolean canRedeliver() {
        return this == RETRY_EXHAUSTED || this == CONSUME_TIMEOUT || this == CONSUME_EXCEPTION;
    }
    
    /**
     * 获取严重程度（数字越大越严重）
     */
    public int getSeverity() {
        switch (this) {
            case MESSAGE_EXPIRED:
                return 1;
            case CONSUME_TIMEOUT:
                return 2;
            case QUEUE_FULL:
                return 3;
            case NO_CONSUMER:
                return 4;
            case CONSUME_EXCEPTION:
                return 5;
            case RETRY_EXHAUSTED:
                return 6;
            case MANUAL_MOVE:
                return 7;
            default:
                return 0;
        }
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
