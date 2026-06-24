package com.ruyuan.mq.nameserver.routing;

/**
 * 路由常量定义
 * 
 * @author RuYuan MQ Team
 */
public class RouteConstants {
    
    // 路由决策超时时间
    public static final long ROUTE_DECISION_TIMEOUT = 1000L; // 1秒
    
    // 负载均衡算法类型
    public static final String LOAD_BALANCE_ROUND_ROBIN = "ROUND_ROBIN";
    public static final String LOAD_BALANCE_RANDOM = "RANDOM";
    public static final String LOAD_BALANCE_CONSISTENT_HASH = "CONSISTENT_HASH";
    public static final String LOAD_BALANCE_LEAST_ACTIVE = "LEAST_ACTIVE";
    public static final String LOAD_BALANCE_WEIGHTED_ROUND_ROBIN = "WEIGHTED_ROUND_ROBIN";
    
    // 故障转移相关常量
    public static final long HEALTH_CHECK_INTERVAL = 5000L; // 5秒
    public static final long FAILOVER_TIMEOUT = 30000L; // 30秒
    public static final int MAX_RETRY_TIMES = 3;
    
    // 集群类型
    public static final String CLUSTER_TYPE_ORDER = "order-cluster";
    public static final String CLUSTER_TYPE_LOG = "log-cluster";
    public static final String CLUSTER_TYPE_DEFAULT = "default-cluster";
    
    // 路由优先级
    public static final int PRIORITY_HIGH = 1;
    public static final int PRIORITY_MEDIUM = 2;
    public static final int PRIORITY_LOW = 3;
    
    // 队列选择策略
    public static final String QUEUE_SELECT_HASH = "HASH";
    public static final String QUEUE_SELECT_ROUND_ROBIN = "ROUND_ROBIN";
    public static final String QUEUE_SELECT_RANDOM = "RANDOM";
    
    // 路由层级
    public static final String ROUTE_LEVEL_GLOBAL = "GLOBAL";
    public static final String ROUTE_LEVEL_CLUSTER = "CLUSTER";
    public static final String ROUTE_LEVEL_LOCAL = "LOCAL";
    
    // 默认配置
    public static final int DEFAULT_QUEUE_COUNT = 4;
    public static final int DEFAULT_BROKER_COUNT = 3;
    public static final double DEFAULT_LOAD_THRESHOLD = 0.8;
    
    // 监控相关
    public static final long METRICS_COLLECT_INTERVAL = 10000L; // 10秒
    public static final int METRICS_HISTORY_SIZE = 100;
}
