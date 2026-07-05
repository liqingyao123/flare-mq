package com.flare.mq.broker.stream;

import com.flare.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流处理引擎核心实现
 * 
 * @author FlareMQ Team
 */
public class StreamEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamEngine.class);
    
    private final ExecutorService processingPool;
    private final ScheduledExecutorService scheduledPool;
    private final WindowManager windowManager;
    private final StateManager stateManager;
    private final StreamStatistics statistics;
    private volatile boolean running;
    
    public StreamEngine() {
        this.processingPool = Executors.newFixedThreadPool(8, r -> {
            Thread t = new Thread(r, "StreamEngine-Processing-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        this.scheduledPool = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "StreamEngine-Scheduled-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        this.windowManager = new WindowManager();
        this.stateManager = new StateManager();
        this.statistics = new StreamStatistics();
        this.running = true;
        
        // 启动定期清理任务
        scheduledPool.scheduleAtFixedRate(this::cleanupExpiredWindows, 30, 30, TimeUnit.SECONDS);
        
        logger.info("StreamEngine initialized with {} processing threads", 8);
    }
    
    /**
     * 处理消息
     */
    public void processMessage(Message message, StreamProcessor processor) {
        if (!running) {
            logger.warn("StreamEngine is not running, ignoring message");
            return;
        }
        
        statistics.incrementProcessedMessages();
        
        processingPool.submit(() -> {
            try {
                long startTime = System.currentTimeMillis();
                processor.process(message);
                long processingTime = System.currentTimeMillis() - startTime;
                statistics.recordProcessingTime(processingTime);
                
                logger.debug("Message processed in {}ms: {}", processingTime, message.getKeys());
                
            } catch (Exception e) {
                statistics.incrementFailedMessages();
                logger.error("Error processing message: {}", message.getKeys(), e);
            }
        });
    }
    
    /**
     * 处理窗口消息
     */
    public void processWindowMessage(Message message, WindowSpec windowSpec, WindowProcessor processor) {
        if (!running) {
            return;
        }
        
        windowManager.processMessage(message, windowSpec, processor);
    }
    
    /**
     * 获取状态管理器
     */
    public StateManager getStateManager() {
        return stateManager;
    }
    
    /**
     * 获取窗口管理器
     */
    public WindowManager getWindowManager() {
        return windowManager;
    }
    
    /**
     * 获取统计信息
     */
    public StreamStatistics getStatistics() {
        return statistics;
    }
    
    /**
     * 清理过期窗口
     */
    private void cleanupExpiredWindows() {
        try {
            int cleanedCount = windowManager.cleanupExpiredWindows();
            if (cleanedCount > 0) {
                logger.debug("Cleaned up {} expired windows", cleanedCount);
            }
        } catch (Exception e) {
            logger.error("Error cleaning up expired windows", e);
        }
    }
    
    /**
     * 关闭流处理引擎
     */
    public void shutdown() {
        logger.info("Shutting down StreamEngine...");
        running = false;
        
        // 关闭线程池
        processingPool.shutdown();
        scheduledPool.shutdown();
        
        try {
            if (!processingPool.awaitTermination(30, TimeUnit.SECONDS)) {
                processingPool.shutdownNow();
            }
            if (!scheduledPool.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            processingPool.shutdownNow();
            scheduledPool.shutdownNow();
        }
        
        // 关闭其他组件
        windowManager.shutdown();
        stateManager.shutdown();
        
        logger.info("StreamEngine shutdown completed");
    }
    
    /**
     * 检查引擎是否运行中
     */
    public boolean isRunning() {
        return running;
    }
}

/**
 * 流处理器接口
 */
interface StreamProcessor {
    void process(Message message) throws Exception;
}

/**
 * 窗口处理器接口
 */
interface WindowProcessor {
    void processWindow(Window window) throws Exception;
}
