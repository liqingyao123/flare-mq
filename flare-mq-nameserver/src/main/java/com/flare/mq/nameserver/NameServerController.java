package com.flare.mq.nameserver;

import com.flare.mq.nameserver.registry.ServiceRegistry;
import com.flare.mq.nameserver.registry.ServiceDiscovery;
import com.flare.mq.nameserver.health.HealthChecker;
import com.flare.mq.nameserver.route.RouteInfoManager;
import com.flare.mq.protocol.server.NettyServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * NameServer控制器 - 服务发现和路由管理的核心组件
 * 
 * @author FlareMQ Team
 */
public class NameServerController {
    
    private static final Logger logger = LoggerFactory.getLogger(NameServerController.class);
    
    private final NameServerConfig nameServerConfig;
    private final ServiceRegistry serviceRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final HealthChecker healthChecker;
    private final RouteInfoManager routeInfoManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NettyServer nettyServer;

    private volatile boolean started = false;

    public NameServerController(NameServerConfig nameServerConfig) {
        this.nameServerConfig = nameServerConfig;
        this.serviceRegistry = new ServiceRegistry();
        this.serviceDiscovery = new ServiceDiscovery(serviceRegistry);
        this.healthChecker = new HealthChecker(serviceRegistry);
        this.routeInfoManager = new RouteInfoManager();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "NameServer-Scheduled-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        // 创建NameServer专用的请求处理器
        NameServerRequestHandler requestHandler = new NameServerRequestHandler(
                serviceDiscovery, serviceRegistry, routeInfoManager, healthChecker);
        this.nettyServer = new NettyServer(nameServerConfig.getListenPort(), requestHandler);

        logger.info("NameServerController initialized");
    }
    
    /**
     * 启动NameServer
     */
    public void start() {
        if (started) {
            logger.warn("NameServer already started");
            return;
        }

        try {
            // 注册请求处理器
            registerProcessors();

            // 启动Netty服务器
            nettyServer.start();
            logger.info("Netty server started on port: {}", nameServerConfig.getListenPort());

            // 启动定时任务
            startScheduledTasks();

            started = true;
            logger.info("NameServer started successfully on port: {}", nameServerConfig.getListenPort());

        } catch (Exception e) {
            logger.error("Failed to start NameServer", e);
            throw new RuntimeException("Failed to start NameServer", e);
        }
    }
    
    /**
     * 关闭NameServer
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        
        logger.info("Shutting down NameServer...");
        
        try {
            // 关闭Netty服务器
            nettyServer.shutdown();
            logger.info("Netty server shutdown completed");

            // 关闭定时任务
            scheduledExecutorService.shutdown();
            if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }

            // 关闭其他组件
            healthChecker.shutdown();
            serviceRegistry.shutdown();
            routeInfoManager.shutdown();

            started = false;
            logger.info("NameServer shutdown completed");

        } catch (Exception e) {
            logger.error("Error during NameServer shutdown", e);
        }
    }
    

    
    /**
     * 启动定时任务
     */
    private void startScheduledTasks() {
        // 定期扫描不活跃的Broker
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                healthChecker.scanNotActiveBroker();
            } catch (Exception e) {
                logger.error("Error scanning inactive brokers", e);
            }
        }, 5, 10, TimeUnit.SECONDS);
        
        // 定期清理过期路由信息
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                routeInfoManager.cleanupExpiredRoutes();
            } catch (Exception e) {
                logger.error("Error cleaning up expired routes", e);
            }
        }, 10, 30, TimeUnit.SECONDS);
        
        // 定期打印统计信息
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printStatistics();
            } catch (Exception e) {
                logger.error("Error printing statistics", e);
            }
        }, 60, 60, TimeUnit.SECONDS);
        
        logger.info("Scheduled tasks started");
    }
    
    /**
     * 打印统计信息
     */
    private void printStatistics() {
        int brokerCount = serviceRegistry.getBrokerCount();
        int topicCount = routeInfoManager.getTopicCount();
        int queueCount = routeInfoManager.getQueueCount();
        
        logger.info("NameServer Statistics: Brokers={}, Topics={}, Queues={}", 
                   brokerCount, topicCount, queueCount);
    }
    
    /**
     * 检查NameServer是否已启动
     */
    public boolean isStarted() {
        return started;
    }
    
    // Getters
    public ServiceRegistry getServiceRegistry() { return serviceRegistry; }
    public ServiceDiscovery getServiceDiscovery() { return serviceDiscovery; }
    public HealthChecker getHealthChecker() { return healthChecker; }
    public RouteInfoManager getRouteInfoManager() { return routeInfoManager; }

    /**
     * 注册请求处理器
     */
    private void registerProcessors() {
        // 请求处理器已在构造函数中设置
        logger.info("NameServer request handlers registered successfully");
    }
}

/**
 * 请求代码常量
 */
class RequestCode {
    public static final int REGISTER_BROKER = 103;
    public static final int UNREGISTER_BROKER = 104;
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;
    public static final int BROKER_HEARTBEAT = 106;
    public static final int GET_BROKER_CLUSTER_INFO = 107;
}
