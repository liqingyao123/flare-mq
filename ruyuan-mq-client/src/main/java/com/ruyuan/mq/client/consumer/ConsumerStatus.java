package com.ruyuan.mq.client.consumer;

/**
 * Consumer状态枚举
 * 
 * @author RuYuan MQ Team
 */
public enum ConsumerStatus {
    
    /**
     * 创建状态 - 刚创建，未启动
     */
    CREATE_JUST("CREATE_JUST", "已创建"),
    
    /**
     * 启动失败状态
     */
    START_FAILED("START_FAILED", "启动失败"),
    
    /**
     * 运行状态 - 正常运行
     */
    RUNNING("RUNNING", "运行中"),
    
    /**
     * 暂停状态 - 暂停消费
     */
    PAUSED("PAUSED", "已暂停"),
    
    /**
     * 关闭状态 - 已关闭
     */
    SHUTDOWN_ALREADY("SHUTDOWN_ALREADY", "已关闭");
    
    private final String code;
    private final String description;
    
    ConsumerStatus(String code, String description) {
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
    public static ConsumerStatus fromCode(String code) {
        if (code == null) {
            return CREATE_JUST;
        }
        
        for (ConsumerStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return CREATE_JUST;
    }
    
    /**
     * 检查是否可以消费消息
     */
    public boolean canConsumeMessage() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否已启动
     */
    public boolean isStarted() {
        return this == RUNNING || this == PAUSED;
    }
    
    /**
     * 检查是否已关闭
     */
    public boolean isShutdown() {
        return this == SHUTDOWN_ALREADY;
    }
    
    /**
     * 检查是否可以暂停
     */
    public boolean canPause() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否可以恢复
     */
    public boolean canResume() {
        return this == PAUSED;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
