package com.ruyuan.mq.nameserver.routing.model;

/**
 * 全局路由结果
 * 
 * @author RuYuan MQ Team
 */
public class GlobalRoute {
    
    private ClusterInfo cluster;
    private int priority;
    private String routeReason;
    private long routeTime;
    
    public GlobalRoute() {
    }
    
    public GlobalRoute(ClusterInfo cluster, int priority) {
        this.cluster = cluster;
        this.priority = priority;
        this.routeTime = System.currentTimeMillis();
    }
    
    public GlobalRoute(ClusterInfo cluster, int priority, String routeReason) {
        this.cluster = cluster;
        this.priority = priority;
        this.routeReason = routeReason;
        this.routeTime = System.currentTimeMillis();
    }
    
    // Getters and Setters
    public ClusterInfo getCluster() {
        return cluster;
    }
    
    public void setCluster(ClusterInfo cluster) {
        this.cluster = cluster;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public void setPriority(int priority) {
        this.priority = priority;
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
    
    @Override
    public String toString() {
        return "GlobalRoute{" +
                "cluster=" + cluster +
                ", priority=" + priority +
                ", routeReason='" + routeReason + '\'' +
                ", routeTime=" + routeTime +
                '}';
    }
}
