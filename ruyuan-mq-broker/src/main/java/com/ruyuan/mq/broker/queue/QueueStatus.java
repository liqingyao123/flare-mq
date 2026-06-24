package com.ruyuan.mq.broker.queue;

/**
 * Queue状态枚举
 * 
 * @author RuYuan MQ Team
 */
public enum QueueStatus {
    
    /**
     * 活跃状态 - 正常接收和发送消息
     */
    ACTIVE("ACTIVE", "活跃"),
    
    /**
     * 暂停状态 - 暂停接收新消息，但可以消费已有消息
     */
    PAUSED("PAUSED", "暂停"),
    
    /**
     * 只读状态 - 只能消费消息，不能接收新消息
     */
    READ_ONLY("READ_ONLY", "只读"),
    
    /**
     * 维护状态 - 正在进行维护操作
     */
    MAINTENANCE("MAINTENANCE", "维护中"),
    
    /**
     * 已删除状态 - 标记为删除，等待清理
     */
    DELETED("DELETED", "已删除");
    
    private final String code;
    private final String description;
    
    QueueStatus(String code, String description) {
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
    public static QueueStatus fromCode(String code) {
        if (code == null) {
            return ACTIVE;
        }
        
        for (QueueStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return ACTIVE;
    }
    
    /**
     * 检查是否可以接收消息
     */
    public boolean canReceiveMessage() {
        return this == ACTIVE;
    }
    
    /**
     * 检查是否可以消费消息
     */
    public boolean canConsumeMessage() {
        return this == ACTIVE || this == PAUSED || this == READ_ONLY;
    }
    
    /**
     * 检查是否可以进行管理操作
     */
    public boolean canManage() {
        return this != DELETED;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
