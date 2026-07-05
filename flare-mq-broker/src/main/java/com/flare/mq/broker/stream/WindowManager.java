package com.flare.mq.broker.stream;

import com.flare.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 窗口管理器
 * 
 * @author FlareMQ Team
 */
public class WindowManager {
    
    private static final Logger logger = LoggerFactory.getLogger(WindowManager.class);
    
    private final ConcurrentHashMap<String, Window> activeWindows;
    private final AtomicLong windowIdGenerator;
    private volatile boolean running;
    
    public WindowManager() {
        this.activeWindows = new ConcurrentHashMap<>();
        this.windowIdGenerator = new AtomicLong(0);
        this.running = true;
        logger.info("WindowManager initialized");
    }
    
    /**
     * 处理消息到窗口
     */
    public void processMessage(Message message, WindowSpec spec, WindowProcessor processor) {
        if (!running) {
            return;
        }
        
        try {
            String windowKey = calculateWindowKey(message, spec);
            Window window = activeWindows.computeIfAbsent(windowKey, k -> createWindow(spec));
            
            synchronized (window) {
                window.addMessage(message);
                
                if (window.isComplete()) {
                    // 窗口完成，触发处理
                    triggerWindowComputation(window, processor);
                    activeWindows.remove(windowKey);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error processing message to window", e);
        }
    }
    
    /**
     * 计算窗口键
     */
    private String calculateWindowKey(Message message, WindowSpec spec) {
        long timestamp = message.getBornTimestamp();
        
        switch (spec.getType()) {
            case TIME_WINDOW:
                long windowStart = (timestamp / spec.getSize()) * spec.getSize();
                return String.format("time_%s_%d_%d", message.getTopic(), windowStart, spec.getSize());
                
            case COUNT_WINDOW:
                // 对于计数窗口，使用Topic作为键
                return String.format("count_%s_%d", message.getTopic(), spec.getSize());
                
            case SESSION_WINDOW:
                // 对于会话窗口，使用消息Key分组
                String sessionKey = message.getKeys() != null ? message.getKeys() : "default";
                return String.format("session_%s_%s_%d", message.getTopic(), sessionKey, spec.getSize());
                
            default:
                throw new IllegalArgumentException("Unsupported window type: " + spec.getType());
        }
    }
    
    /**
     * 创建窗口
     */
    private Window createWindow(WindowSpec spec) {
        String windowId = "window_" + windowIdGenerator.incrementAndGet();
        Window window = new Window(windowId, spec);
        
        logger.debug("Created new window: {} with spec: {}", windowId, spec);
        return window;
    }
    
    /**
     * 触发窗口计算
     */
    private void triggerWindowComputation(Window window, WindowProcessor processor) {
        try {
            logger.debug("Triggering computation for window: {} with {} messages", 
                        window.getId(), window.getMessageCount());
            
            processor.processWindow(window);
            
        } catch (Exception e) {
            logger.error("Error processing window: {}", window.getId(), e);
        }
    }
    
    /**
     * 清理过期窗口
     */
    public int cleanupExpiredWindows() {
        if (!running) {
            return 0;
        }
        
        long currentTime = System.currentTimeMillis();
        int cleanedCount = 0;
        
        for (String key : activeWindows.keySet()) {
            Window window = activeWindows.get(key);
            if (window != null && window.isExpired(currentTime)) {
                if (activeWindows.remove(key) != null) {
                    cleanedCount++;
                    logger.debug("Cleaned up expired window: {}", window.getId());
                }
            }
        }
        
        return cleanedCount;
    }
    
    /**
     * 获取活跃窗口数量
     */
    public int getActiveWindowCount() {
        return activeWindows.size();
    }
    
    /**
     * 关闭窗口管理器
     */
    public void shutdown() {
        logger.info("Shutting down WindowManager...");
        running = false;
        
        // 清理所有活跃窗口
        int windowCount = activeWindows.size();
        activeWindows.clear();
        
        logger.info("WindowManager shutdown completed, cleaned {} windows", windowCount);
    }
}

/**
 * 窗口规格
 */
class WindowSpec {
    
    public enum WindowType {
        TIME_WINDOW,    // 时间窗口
        COUNT_WINDOW,   // 计数窗口
        SESSION_WINDOW  // 会话窗口
    }
    
    private final WindowType type;
    private final long size;        // 窗口大小（时间窗口为毫秒，计数窗口为消息数量）
    private final long slide;       // 滑动间隔（仅用于滑动窗口）
    private final Duration timeout; // 窗口超时时间
    
    public WindowSpec(WindowType type, long size) {
        this(type, size, size, Duration.ofMinutes(5));
    }
    
    public WindowSpec(WindowType type, long size, long slide, Duration timeout) {
        this.type = type;
        this.size = size;
        this.slide = slide;
        this.timeout = timeout;
    }
    
    // 静态工厂方法
    public static WindowSpec timeWindow(Duration duration) {
        return new WindowSpec(WindowType.TIME_WINDOW, duration.toMillis());
    }
    
    public static WindowSpec countWindow(int count) {
        return new WindowSpec(WindowType.COUNT_WINDOW, count);
    }
    
    public static WindowSpec sessionWindow(Duration timeout) {
        return new WindowSpec(WindowType.SESSION_WINDOW, timeout.toMillis());
    }
    
    // Getters
    public WindowType getType() { return type; }
    public long getSize() { return size; }
    public long getSlide() { return slide; }
    public Duration getTimeout() { return timeout; }
    
    @Override
    public String toString() {
        return String.format("WindowSpec{type=%s, size=%d, slide=%d, timeout=%s}", 
                           type, size, slide, timeout);
    }
}
