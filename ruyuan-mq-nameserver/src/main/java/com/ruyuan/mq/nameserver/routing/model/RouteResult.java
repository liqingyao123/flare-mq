package com.ruyuan.mq.nameserver.routing.model;

/**
 * 最终路由结果
 * 
 * @author RuYuan MQ Team
 */
public class RouteResult {
    
    private BrokerInfo broker;
    private int queueId;
    private String queueSelectStrategy;
    private GlobalRoute globalRoute;
    private ClusterRoute clusterRoute;
    private String routePath; // 路由路径，用于调试
    private long totalRouteTime;
    private boolean success;
    private String errorMessage;
    
    public RouteResult() {
    }
    
    public RouteResult(BrokerInfo broker, int queueId) {
        this.broker = broker;
        this.queueId = queueId;
        this.success = true;
        this.totalRouteTime = System.currentTimeMillis();
    }
    
    public RouteResult(BrokerInfo broker, int queueId, GlobalRoute globalRoute, ClusterRoute clusterRoute) {
        this.broker = broker;
        this.queueId = queueId;
        this.globalRoute = globalRoute;
        this.clusterRoute = clusterRoute;
        this.success = true;
        this.totalRouteTime = System.currentTimeMillis();
        
        // 构建路由路径
        this.routePath = buildRoutePath();
    }
    
    /**
     * 构建路由路径字符串，用于调试和监控
     */
    private String buildRoutePath() {
        StringBuilder sb = new StringBuilder();
        if (globalRoute != null && globalRoute.getCluster() != null) {
            sb.append("Global[").append(globalRoute.getCluster().getClusterId()).append("]");
        }
        if (clusterRoute != null && clusterRoute.getBroker() != null) {
            sb.append(" -> Cluster[").append(clusterRoute.getBroker().getBrokerId()).append("]");
        }
        sb.append(" -> Local[Queue-").append(queueId).append("]");
        return sb.toString();
    }
    
    // Getters and Setters
    public BrokerInfo getBroker() {
        return broker;
    }
    
    public void setBroker(BrokerInfo broker) {
        this.broker = broker;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
    
    public String getQueueSelectStrategy() {
        return queueSelectStrategy;
    }
    
    public void setQueueSelectStrategy(String queueSelectStrategy) {
        this.queueSelectStrategy = queueSelectStrategy;
    }
    
    public GlobalRoute getGlobalRoute() {
        return globalRoute;
    }
    
    public void setGlobalRoute(GlobalRoute globalRoute) {
        this.globalRoute = globalRoute;
    }
    
    public ClusterRoute getClusterRoute() {
        return clusterRoute;
    }
    
    public void setClusterRoute(ClusterRoute clusterRoute) {
        this.clusterRoute = clusterRoute;
    }
    
    public String getRoutePath() {
        return routePath;
    }
    
    public void setRoutePath(String routePath) {
        this.routePath = routePath;
    }
    
    public long getTotalRouteTime() {
        return totalRouteTime;
    }
    
    public void setTotalRouteTime(long totalRouteTime) {
        this.totalRouteTime = totalRouteTime;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    /**
     * 创建失败的路由结果
     */
    public static RouteResult failure(String errorMessage) {
        RouteResult result = new RouteResult();
        result.setSuccess(false);
        result.setErrorMessage(errorMessage);
        result.setTotalRouteTime(System.currentTimeMillis());
        return result;
    }
    
    @Override
    public String toString() {
        return "RouteResult{" +
                "broker=" + broker +
                ", queueId=" + queueId +
                ", queueSelectStrategy='" + queueSelectStrategy + '\'' +
                ", routePath='" + routePath + '\'' +
                ", totalRouteTime=" + totalRouteTime +
                ", success=" + success +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
