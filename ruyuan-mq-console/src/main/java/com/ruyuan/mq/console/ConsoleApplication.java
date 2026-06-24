package com.ruyuan.mq.console;

import com.ruyuan.mq.console.controller.MonitorController;
import com.ruyuan.mq.console.service.MonitorService;
import com.ruyuan.mq.console.service.impl.MonitorServiceImpl;
import com.ruyuan.mq.console.service.impl.RealMonitorServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RuYuan MQ 管理控制台应用
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class ConsoleApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsoleApplication.class);
    
    private final MonitorService monitorService;
    private final MonitorController monitorController;
    private final ScheduledExecutorService scheduledExecutor;
    
    private volatile boolean running = false;
    
    public ConsoleApplication() {
        // 使用真实监控服务
        RealMonitorServiceImpl realMonitorService = new RealMonitorServiceImpl();
        realMonitorService.setNameServerAddr("localhost:9876"); // 设置NameServer地址
        this.monitorService = realMonitorService;
        this.monitorController = new MonitorController(monitorService);
        this.scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "Console-Scheduler");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * 启动控制台应用
     */
    public void start() {
        if (running) {
            logger.warn("Console application already running");
            return;
        }
        
        try {
            logger.info("Starting RuYuan MQ Console Application...");
            
            // 启动监控服务
            monitorService.start();
            
            // 启动定时任务
            startScheduledTasks();
            
            running = true;
            logger.info("RuYuan MQ Console Application started successfully");
            
            // 显示初始状态
            displaySystemStatus();
            
        } catch (Exception e) {
            logger.error("Failed to start console application", e);
            throw new RuntimeException("Failed to start console application", e);
        }
    }
    
    /**
     * 关闭控制台应用
     */
    public void shutdown() {
        if (!running) {
            return;
        }
        
        logger.info("Shutting down RuYuan MQ Console Application...");
        
        try {
            // 关闭定时任务
            scheduledExecutor.shutdown();
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            
            // 关闭监控服务
            monitorService.shutdown();
            
            running = false;
            logger.info("RuYuan MQ Console Application shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during console application shutdown", e);
        }
    }
    
    /**
     * 启动定时任务
     */
    private void startScheduledTasks() {
        // 定期刷新系统状态
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                monitorService.refreshSystemMetrics();
            } catch (Exception e) {
                logger.error("Error refreshing system metrics", e);
            }
        }, 10, 30, TimeUnit.SECONDS);
        
        // 定期显示系统状态
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                displaySystemStatus();
            } catch (Exception e) {
                logger.error("Error displaying system status", e);
            }
        }, 60, 60, TimeUnit.SECONDS);
        
        logger.info("Console scheduled tasks started");
    }
    
    /**
     * 显示系统状态
     */
    private void displaySystemStatus() {
        try {
            logger.info("\n" + monitorController.getSystemOverview());
        } catch (Exception e) {
            logger.error("Error displaying system status", e);
        }
    }
    
    /**
     * 获取监控控制器
     */
    public MonitorController getMonitorController() {
        return monitorController;
    }
    
    /**
     * 检查应用是否运行中
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * 主方法
     */
    public static void main(String[] args) {
        ConsoleApplication app = new ConsoleApplication();
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
        
        try {
            app.start();
            
            // 保持应用运行
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Console application error", e);
            System.exit(1);
        }
    }
}
