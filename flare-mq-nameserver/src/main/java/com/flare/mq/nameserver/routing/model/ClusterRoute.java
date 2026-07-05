package com.flare.mq.nameserver.routing.model;

/**
 * 集群路由结果
 * 
 * @author FlareMQ Team
 */
public class ClusterRoute {
    
    private BrokerInfo broker;
    private int queueCount;
    private String loadBalanceStrategy;
    private boolean isFailover;
    private String routeReason;
    private long routeTime;
    private GlobalRoute globalRoute;
    
    public ClusterRoute() {
    }
    
    public ClusterRoute(BrokerInfo broker, int queueCount) {
        this.broker = broker;
        this.queueCount = queueCount;
        this.isFailover = false;
        this.routeTime = System.currentTimeMillis();
    }
    
    public ClusterRoute(BrokerInfo broker, int queueCount, String loadBalanceStrategy) {
        this.broker = broker;
        this.queueCount = queueCount;
        this.loadBalanceStrategy = loadBalanceStrategy;
        this.isFailover = false;
        this.routeTime = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public BrokerInfo getBroker() {
        return broker;
    }
    
    public void setBroker(BrokerInfo broker) {
        this.broker = broker;
    }
    
    public int getQueueCount() {
        return queueCount;
    }
    
    public void setQueueCount(int queueCount) {
        this.queueCount = queueCount;
    }
    
    public String getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }
    
    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }
    
    public boolean isFailover() {
        return isFailover;
    }
    
    public void setFailover(boolean failover) {
        isFailover = failover;
    }
    
    public String getRouteReason() {
        return routeReason;
    }
    
    public void setRouteReason(String routeReason) {
        this.routeReason = routeReason;
    }
    
    public long getRouteTime() {
        return routeTime;
    }
    
    public void setRouteTime(long routeTime) {
        this.routeTime = routeTime;
    }

    public GlobalRoute getGlobalRoute() {
        return globalRoute;
    }

    public void setGlobalRoute(GlobalRoute globalRoute) {
        this.globalRoute = globalRoute;
    }
    
    @Override
    public String toString() {
        return "ClusterRoute{" +
                "broker=" + broker +
                ", queueCount=" + queueCount +
                ", loadBalanceStrategy='" + loadBalanceStrategy + '\'' +
                ", isFailover=" + isFailover +
                ", routeReason='" + routeReason + '\'' +
                ", routeTime=" + routeTime +
                '}';
    }
}
