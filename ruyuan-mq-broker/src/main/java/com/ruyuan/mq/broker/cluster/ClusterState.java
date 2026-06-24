package com.ruyuan.mq.broker.cluster;

/**
 * 集群状态枚举
 * 
 * @author RuYuan MQ Team
 */
public enum ClusterState {
    
    /**
     * 初始化中
     */
    INITIALIZING("初始化中", "集群正在初始化"),
    
    /**
     * 运行中
     */
    RUNNING("运行中", "集群正常运行"),
    
    /**
     * 降级运行
     */
    DEGRADED("降级运行", "集群部分节点故障，降级运行"),
    
    /**
     * 故障转移中
     */
    FAILOVER("故障转移中", "集群正在进行故障转移"),
    
    /**
     * 重新平衡中
     */
    REBALANCING("重新平衡中", "集群正在进行负载重新平衡"),
    
    /**
     * 关闭中
     */
    SHUTTING_DOWN("关闭中", "集群正在关闭"),
    
    /**
     * 已停止
     */
    STOPPED("已停止", "集群已停止运行"),
    
    /**
     * 错误状态
     */
    ERROR("错误状态", "集群出现严重错误");
    
    private final String displayName;
    private final String description;
    
    ClusterState(String displayName, String description) {
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
     * 检查是否为正常运行状态
     */
    public boolean isHealthy() {
        return this == RUNNING;
    }
    
    /**
     * 检查是否为活跃状态
     */
    public boolean isActive() {
        return this == RUNNING || this == DEGRADED || this == REBALANCING;
    }
    
    /**
     * 检查是否为过渡状态
     */
    public boolean isTransitional() {
        return this == INITIALIZING || this == FAILOVER || 
               this == REBALANCING || this == SHUTTING_DOWN;
    }
    
    /**
     * 检查是否为终止状态
     */
    public boolean isTerminal() {
        return this == STOPPED || this == ERROR;
    }
    
    @Override
    public String toString() {
        return String.format("%s(%s)", displayName, description);
    }
}
