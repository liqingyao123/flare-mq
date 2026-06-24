package com.ruyuan.mq.client.consumer;

/**
 * 消费起始位置枚举
 * 
 * @author RuYuan MQ Team
 */
public enum ConsumeFromWhere {
    
    /**
     * 从队列的最后位置开始消费，即跳过历史消息
     */
    CONSUME_FROM_LAST_OFFSET("CONSUME_FROM_LAST_OFFSET", "从最后位置开始消费"),
    
    /**
     * 从队列的第一个消息开始消费，即消费所有历史消息
     */
    CONSUME_FROM_FIRST_OFFSET("CONSUME_FROM_FIRST_OFFSET", "从第一个消息开始消费"),
    
    /**
     * 从指定时间戳开始消费
     */
    CONSUME_FROM_TIMESTAMP("CONSUME_FROM_TIMESTAMP", "从指定时间戳开始消费"),
    
    /**
     * 从存储的消费进度开始消费
     */
    CONSUME_FROM_STORED_OFFSET("CONSUME_FROM_STORED_OFFSET", "从存储的消费进度开始消费");
    
    private final String code;
    private final String description;
    
    ConsumeFromWhere(String code, String description) {
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
     * 根据代码获取消费起始位置
     */
    public static ConsumeFromWhere fromCode(String code) {
        if (code == null) {
            return CONSUME_FROM_LAST_OFFSET;
        }
        
        for (ConsumeFromWhere where : values()) {
            if (where.code.equalsIgnoreCase(code)) {
                return where;
            }
        }
        
        return CONSUME_FROM_LAST_OFFSET;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
