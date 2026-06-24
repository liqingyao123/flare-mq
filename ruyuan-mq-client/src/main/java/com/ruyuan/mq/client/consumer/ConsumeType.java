package com.ruyuan.mq.client.consumer;

/**
 * 消费类型枚举
 * 
 * @author RuYuan MQ Team
 */
public enum ConsumeType {
    
    /**
     * 主动拉取消费 - Consumer主动从Broker拉取消息
     */
    CONSUME_ACTIVELY("CONSUME_ACTIVELY", "主动拉取消费"),
    
    /**
     * 被动推送消费 - Broker主动推送消息给Consumer
     */
    CONSUME_PASSIVELY("CONSUME_PASSIVELY", "被动推送消费");
    
    private final String code;
    private final String description;
    
    ConsumeType(String code, String description) {
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
     * 根据代码获取消费类型
     */
    public static ConsumeType fromCode(String code) {
        if (code == null) {
            return CONSUME_ACTIVELY;
        }
        
        for (ConsumeType type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        
        return CONSUME_ACTIVELY;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
