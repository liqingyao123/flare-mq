package com.flare.mq.broker.ack;

/**
 * 死信状态枚举
 * 
 * @author FlareMQ Team
 */
public enum DeadLetterStatus {
    
    /**
     * 待处理 - 刚进入死信队列，等待处理
     */
    PENDING("PENDING", "待处理"),
    
    /**
     * 重新投递中 - 正在尝试重新投递
     */
    REDELIVERING("REDELIVERING", "重新投递中"),
    
    /**
     * 重新投递成功 - 已成功重新投递
     */
    REDELIVERED("REDELIVERED", "重新投递成功"),
    
    /**
     * 重新投递失败 - 重新投递失败
     */
    REDELIVERY_FAILED("REDELIVERY_FAILED", "重新投递失败"),
    
    /**
     * 已丢弃 - 消息已被丢弃
     */
    DISCARDED("DISCARDED", "已丢弃"),
    
    /**
     * 已归档 - 消息已归档保存
     */
    ARCHIVED("ARCHIVED", "已归档");
    
    private final String code;
    private final String description;
    
    DeadLetterStatus(String code, String description) {
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
    public static DeadLetterStatus fromCode(String code) {
        if (code == null) {
            return PENDING;
        }
        
        for (DeadLetterStatus status : values()) {
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
        return this == REDELIVERED || this == DISCARDED || this == ARCHIVED;
    }
    
    /**
     * 检查是否为处理中状态
     */
    public boolean isProcessing() {
        return this == REDELIVERING;
    }
    
    /**
     * 检查是否可以重新投递
     */
    public boolean canRedeliver() {
        return this == PENDING || this == REDELIVERY_FAILED;
    }
    
    /**
     * 检查是否可以丢弃
     */
    public boolean canDiscard() {
        return this == PENDING || this == REDELIVERY_FAILED;
    }
    
    /**
     * 检查是否可以归档
     */
    public boolean canArchive() {
        return this == PENDING || this == REDELIVERY_FAILED;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == REDELIVERED;
    }
    
    /**
     * 检查是否为失败状态
     */
    public boolean isFailure() {
        return this == REDELIVERY_FAILED || this == DISCARDED;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
