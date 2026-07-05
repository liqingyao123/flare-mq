package com.flare.mq.broker.stream;

/**
 * 流处理状态枚举
 * 
 * @author FlareMQ Team
 */
public enum StreamStatus {
    
    /**
     * 初始化状态
     */
    INITIALIZED("初始化", "流已创建但尚未启动"),
    
    /**
     * 运行中状态
     */
    RUNNING("运行中", "流正在处理消息"),
    
    /**
     * 暂停状态
     */
    PAUSED("暂停", "流已暂停，不处理新消息"),
    
    /**
     * 停止状态
     */
    STOPPED("停止", "流已停止处理"),
    
    /**
     * 错误状态
     */
    ERROR("错误", "流处理出现错误"),
    
    /**
     * 关闭状态
     */
    CLOSED("关闭", "流已关闭并释放资源");
    
    private final String displayName;
    private final String description;
    
    StreamStatus(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }
    
    /**
     * 获取显示名称
     */
    public String getDisplayName() {
        return displayName;
    }
    
    /**
     * 获取描述
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * 检查是否为活跃状态
     */
    public boolean isActive() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否可以启动
     */
    public boolean canStart() {
        return this == INITIALIZED || this == STOPPED || this == PAUSED;
    }
    
    /**
     * 检查是否可以停止
     */
    public boolean canStop() {
        return this == RUNNING || this == PAUSED || this == ERROR;
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
    
    /**
     * 检查是否为终止状态
     */
    public boolean isTerminal() {
        return this == CLOSED;
    }
    
    @Override
    public String toString() {
        return String.format("%s(%s)", displayName, description);
    }
}
