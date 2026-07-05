package com.flare.mq.broker.cluster;

/**
 * 集群配置类
 * 
 * @author FlareMQ Team
 */
public class ClusterConfig {
    
    // 基本配置
    private String brokerAddr = "127.0.0.1:10911";
    private long brokerId = 0L;
    private boolean masterCandidate = true;
    
    // 超时配置
    private long nodeTimeoutMs = 30000L;        // 30秒节点超时
    private long nodeExpireMs = 120000L;        // 2分钟节点过期
    private long heartbeatIntervalMs = 10000L;  // 10秒心跳间隔
    
    // 健康检查配置
    private double minHealthRatio = 0.5;        // 最小健康比例50%
    private int maxRetryTimes = 3;              // 最大重试次数
    private long retryIntervalMs = 5000L;       // 重试间隔5秒
    
    // 复制配置
    private boolean enableReplication = true;   // 启用数据复制
    private int replicationFactor = 2;          // 复制因子
    private long replicationTimeoutMs = 10000L; // 复制超时10秒
    private int replicationBatchSize = 100;     // 复制批次大小
    
    // 故障转移配置
    private boolean enableFailover = true;      // 启用故障转移
    private long failoverTimeoutMs = 30000L;    // 故障转移超时30秒
    private int failoverRetryTimes = 3;         // 故障转移重试次数
    
    // 负载均衡配置
    private String loadBalanceStrategy = "ROUND_ROBIN"; // 负载均衡策略
    private boolean enableDynamicBalance = true;        // 启用动态负载均衡
    private long balanceIntervalMs = 60000L;           // 负载均衡间隔1分钟
    
    // 网络配置
    private int connectTimeoutMs = 5000;        // 连接超时5秒
    private int socketTimeoutMs = 10000;        // Socket超时10秒
    private int maxConnections = 100;           // 最大连接数
    
    // 构造函数
    public ClusterConfig() {
    }
    
    public ClusterConfig(String brokerAddr, long brokerId) {
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
    }
    
    // Getters and Setters
    public String getBrokerAddr() {
        return brokerAddr;
    }
    
    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }
    
    public long getBrokerId() {
        return brokerId;
    }
    
    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }
    
    public boolean isMasterCandidate() {
        return masterCandidate;
    }
    
    public void setMasterCandidate(boolean masterCandidate) {
        this.masterCandidate = masterCandidate;
    }
    
    public long getNodeTimeoutMs() {
        return nodeTimeoutMs;
    }
    
    public void setNodeTimeoutMs(long nodeTimeoutMs) {
        this.nodeTimeoutMs = nodeTimeoutMs;
    }
    
    public long getNodeExpireMs() {
        return nodeExpireMs;
    }
    
    public void setNodeExpireMs(long nodeExpireMs) {
        this.nodeExpireMs = nodeExpireMs;
    }
    
    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }
    
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }
    
    public double getMinHealthRatio() {
        return minHealthRatio;
    }
    
    public void setMinHealthRatio(double minHealthRatio) {
        this.minHealthRatio = minHealthRatio;
    }
    
    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }
    
    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }
    
    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }
    
    public void setRetryIntervalMs(long retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }
    
    public boolean isEnableReplication() {
        return enableReplication;
    }
    
    public void setEnableReplication(boolean enableReplication) {
        this.enableReplication = enableReplication;
    }
    
    public int getReplicationFactor() {
        return replicationFactor;
    }
    
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
    
    public long getReplicationTimeoutMs() {
        return replicationTimeoutMs;
    }
    
    public void setReplicationTimeoutMs(long replicationTimeoutMs) {
        this.replicationTimeoutMs = replicationTimeoutMs;
    }
    
    public int getReplicationBatchSize() {
        return replicationBatchSize;
    }
    
    public void setReplicationBatchSize(int replicationBatchSize) {
        this.replicationBatchSize = replicationBatchSize;
    }
    
    public boolean isEnableFailover() {
        return enableFailover;
    }
    
    public void setEnableFailover(boolean enableFailover) {
        this.enableFailover = enableFailover;
    }
    
    public long getFailoverTimeoutMs() {
        return failoverTimeoutMs;
    }
    
    public void setFailoverTimeoutMs(long failoverTimeoutMs) {
        this.failoverTimeoutMs = failoverTimeoutMs;
    }
    
    public int getFailoverRetryTimes() {
        return failoverRetryTimes;
    }
    
    public void setFailoverRetryTimes(int failoverRetryTimes) {
        this.failoverRetryTimes = failoverRetryTimes;
    }
    
    public String getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }
    
    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }
    
    public boolean isEnableDynamicBalance() {
        return enableDynamicBalance;
    }
    
    public void setEnableDynamicBalance(boolean enableDynamicBalance) {
        this.enableDynamicBalance = enableDynamicBalance;
    }
    
    public long getBalanceIntervalMs() {
        return balanceIntervalMs;
    }
    
    public void setBalanceIntervalMs(long balanceIntervalMs) {
        this.balanceIntervalMs = balanceIntervalMs;
    }
    
    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }
    
    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }
    
    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }
    
    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }
    
    public int getMaxConnections() {
        return maxConnections;
    }
    
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    
    /**
     * 验证配置有效性
     */
    public void validate() {
        if (brokerAddr == null || brokerAddr.trim().isEmpty()) {
            throw new IllegalArgumentException("brokerAddr cannot be null or empty");
        }
        
        if (brokerId < 0) {
            throw new IllegalArgumentException("brokerId must be non-negative");
        }
        
        if (nodeTimeoutMs <= 0) {
            throw new IllegalArgumentException("nodeTimeoutMs must be positive");
        }
        
        if (nodeExpireMs <= nodeTimeoutMs) {
            throw new IllegalArgumentException("nodeExpireMs must be greater than nodeTimeoutMs");
        }
        
        if (minHealthRatio < 0.0 || minHealthRatio > 1.0) {
            throw new IllegalArgumentException("minHealthRatio must be between 0.0 and 1.0");
        }
        
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be at least 1");
        }
    }
    
    @Override
    public String toString() {
        return "ClusterConfig{" +
                "brokerAddr='" + brokerAddr + '\'' +
                ", brokerId=" + brokerId +
                ", masterCandidate=" + masterCandidate +
                ", nodeTimeoutMs=" + nodeTimeoutMs +
                ", enableReplication=" + enableReplication +
                ", replicationFactor=" + replicationFactor +
                ", enableFailover=" + enableFailover +
                ", loadBalanceStrategy='" + loadBalanceStrategy + '\'' +
                '}';
    }
}
