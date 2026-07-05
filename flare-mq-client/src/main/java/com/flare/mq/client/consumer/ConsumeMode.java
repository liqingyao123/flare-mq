package com.flare.mq.client.consumer;

/**
 * 消费模式枚举
 * 
 * @author FlareMQ Team
 */
public enum ConsumeMode {
    
    /**
     * 集群消费模式 - 同一个Consumer Group内的Consumer平均分摊消费
     */
    CLUSTERING("CLUSTERING", "集群消费"),
    
    /**
     * 广播消费模式 - 同一个Consumer Group内的每个Consumer都消费全量消息
     */
    BROADCASTING("BROADCASTING", "广播消费");
    
    private final String code;
    private final String description;
    
    ConsumeMode(String code, String description) {
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
     * 根据代码获取消费模式
     */
    public static ConsumeMode fromCode(String code) {
        if (code == null) {
            return CLUSTERING;
        }
        
        for (ConsumeMode mode : values()) {
            if (mode.code.equalsIgnoreCase(code)) {
                return mode;
            }
        }
        
        return CLUSTERING;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
