package com.ruyuan.mq.client.producer;

/**
 * Producer状态枚举
 * 
 * @author RuYuan MQ Team
 */
public enum ProducerStatus {
    
    /**
     * 创建状态 - 刚创建，未启动
     */
    CREATE_JUST("CREATE_JUST", "已创建"),
    
    /**
     * 启动中状态 - 正在启动
     */
    START_FAILED("START_FAILED", "启动失败"),
    
    /**
     * 运行状态 - 正常运行
     */
    RUNNING("RUNNING", "运行中"),
    
    /**
     * 关闭中状态 - 正在关闭
     */
    SHUTDOWN_ALREADY("SHUTDOWN_ALREADY", "已关闭");
    
    private final String code;
    private final String description;
    
    ProducerStatus(String code, String description) {
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
    public static ProducerStatus fromCode(String code) {
        if (code == null) {
            return CREATE_JUST;
        }
        
        for (ProducerStatus status : values()) {
            if (status.code.equalsIgnoreCase(code)) {
                return status;
            }
        }
        
        return CREATE_JUST;
    }
    
    /**
     * 检查是否可以发送消息
     */
    public boolean canSendMessage() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否已启动
     */
    public boolean isStarted() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否已关闭
     */
    public boolean isShutdown() {
        return this == SHUTDOWN_ALREADY;
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
