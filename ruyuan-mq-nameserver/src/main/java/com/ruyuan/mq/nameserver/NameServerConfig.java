package com.ruyuan.mq.nameserver;

/**
 * NameServer配置类
 * 
 * @author RuYuan MQ Team
 */
public class NameServerConfig {
    
    // 服务器配置
    private String nameServerAddress = "127.0.0.1:9876";
    private int listenPort = 9876;
    private String clusterName = "DefaultCluster";
    
    // 健康检查配置
    private long brokerChannelExpiredTime = 1000 * 60 * 2; // 2分钟
    private long scanNotActiveBrokerInterval = 1000 * 10;  // 10秒
    
    // 路由信息配置
    private long routeInfoExpiredTime = 1000 * 60 * 5;     // 5分钟
    private long cleanupExpiredRouteInterval = 1000 * 30;  // 30秒
    
    // 服务发现配置
    private boolean enableServiceDiscovery = true;
    private long serviceDiscoveryInterval = 1000 * 30;     // 30秒
    
    // 集群配置
    private boolean enableCluster = false;
    private String[] clusterNodes = new String[0];
    
    // 持久化配置
    private boolean enablePersistence = true;
    private String persistenceFile = "nameserver.dat";
    private long persistenceInterval = 1000 * 60;          // 1分钟
    
    // 性能配置
    private int workerThreads = 8;
    private int maxConnections = 1000;
    private int sendMessageThreadPoolNums = 4;
    private int pullMessageThreadPoolNums = 4;
    
    // 日志配置
    private boolean enableRequestLog = true;
    private String logLevel = "INFO";
    
    // 构造函数
    public NameServerConfig() {
    }
    
    // Getters and Setters
    public String getNameServerAddress() {
        return nameServerAddress;
    }
    
    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }
    
    public int getListenPort() {
        return listenPort;
    }
    
    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }
    
    public String getClusterName() {
        return clusterName;
    }
    
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public long getBrokerChannelExpiredTime() {
        return brokerChannelExpiredTime;
    }
    
    public void setBrokerChannelExpiredTime(long brokerChannelExpiredTime) {
        this.brokerChannelExpiredTime = brokerChannelExpiredTime;
    }
    
    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }
    
    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }
    
    public long getRouteInfoExpiredTime() {
        return routeInfoExpiredTime;
    }
    
    public void setRouteInfoExpiredTime(long routeInfoExpiredTime) {
        this.routeInfoExpiredTime = routeInfoExpiredTime;
    }
    
    public long getCleanupExpiredRouteInterval() {
        return cleanupExpiredRouteInterval;
    }
    
    public void setCleanupExpiredRouteInterval(long cleanupExpiredRouteInterval) {
        this.cleanupExpiredRouteInterval = cleanupExpiredRouteInterval;
    }
    
    public boolean isEnableServiceDiscovery() {
        return enableServiceDiscovery;
    }
    
    public void setEnableServiceDiscovery(boolean enableServiceDiscovery) {
        this.enableServiceDiscovery = enableServiceDiscovery;
    }
    
    public long getServiceDiscoveryInterval() {
        return serviceDiscoveryInterval;
    }
    
    public void setServiceDiscoveryInterval(long serviceDiscoveryInterval) {
        this.serviceDiscoveryInterval = serviceDiscoveryInterval;
    }
    
    public boolean isEnableCluster() {
        return enableCluster;
    }
    
    public void setEnableCluster(boolean enableCluster) {
        this.enableCluster = enableCluster;
    }
    
    public String[] getClusterNodes() {
        return clusterNodes;
    }
    
    public void setClusterNodes(String[] clusterNodes) {
        this.clusterNodes = clusterNodes;
    }
    
    public boolean isEnablePersistence() {
        return enablePersistence;
    }
    
    public void setEnablePersistence(boolean enablePersistence) {
        this.enablePersistence = enablePersistence;
    }
    
    public String getPersistenceFile() {
        return persistenceFile;
    }
    
    public void setPersistenceFile(String persistenceFile) {
        this.persistenceFile = persistenceFile;
    }
    
    public long getPersistenceInterval() {
        return persistenceInterval;
    }
    
    public void setPersistenceInterval(long persistenceInterval) {
        this.persistenceInterval = persistenceInterval;
    }
    
    public int getWorkerThreads() {
        return workerThreads;
    }
    
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }
    
    public int getMaxConnections() {
        return maxConnections;
    }
    
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    
    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }
    
    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }
    
    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }
    
    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }
    
    public boolean isEnableRequestLog() {
        return enableRequestLog;
    }
    
    public void setEnableRequestLog(boolean enableRequestLog) {
        this.enableRequestLog = enableRequestLog;
    }
    
    public String getLogLevel() {
        return logLevel;
    }
    
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }
    
    @Override
    public String toString() {
        return "NameServerConfig{" +
                "nameServerAddress='" + nameServerAddress + '\'' +
                ", listenPort=" + listenPort +
                ", clusterName='" + clusterName + '\'' +
                ", brokerChannelExpiredTime=" + brokerChannelExpiredTime +
                ", enableServiceDiscovery=" + enableServiceDiscovery +
                ", enableCluster=" + enableCluster +
                ", enablePersistence=" + enablePersistence +
                ", workerThreads=" + workerThreads +
                ", maxConnections=" + maxConnections +
                '}';
    }
}
