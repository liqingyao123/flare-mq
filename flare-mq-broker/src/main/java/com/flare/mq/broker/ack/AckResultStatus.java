package com.flare.mq.broker.ack;

/**
 * 确认结果状态枚举
 * 
 * @author FlareMQ Team
 */
public enum AckResultStatus {
    
    /**
     * 成功
     */
    SUCCESS("SUCCESS", "成功"),
    
    /**
     * 部分成功
     */
    PARTIAL_SUCCESS("PARTIAL_SUCCESS", "部分成功"),
    
    /**
     * 失败
     */
    FAILURE("FAILURE", "失败"),
    
    /**
     * 消息不存在
     */
    MESSAGE_NOT_EXIST("MESSAGE_NOT_EXIST", "消息不存在"),
    
    /**
     * 消息已确认
     */
    ALREADY_ACKED("ALREADY_ACKED", "消息已确认"),
    
    /**
     * 超时
     */
    TIMEOUT("TIMEOUT", "超时");
    
    private final String code;
    private final String description;
    
    AckResultStatus(String code, String description) {
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
    public static AckResultStatus fromCode(String code) {
        if (code == null) {
            return FAILURE;
        }
        
        for (AckResultStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return FAILURE;
    }
    
    /**
     * 检查是否为成功状态
     */
    public boolean isSuccess() {
        return this == SUCCESS;
    }
    
    /**
     * 检查是否为部分成功状态
     */
    public boolean isPartialSuccess() {
        return this == PARTIAL_SUCCESS;
    }
    
    /**
     * 检查是否为失败状态
     */
    public boolean isFailure() {
        return this == FAILURE;
    }
    
    /**
     * 检查是否有成功的部分
     */
    public boolean hasSuccess() {
        return this == SUCCESS || this == PARTIAL_SUCCESS;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
