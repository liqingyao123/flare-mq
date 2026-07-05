package com.flare.mq.broker.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流处理状态管理器
 * 
 * @author FlareMQ Team
 */
public class StateManager {
    
    private static final Logger logger = LoggerFactory.getLogger(StateManager.class);
    
    private final ConcurrentHashMap<String, Object> stateStore;
    private final ConcurrentHashMap<String, Long> stateTimestamps;
    private final ScheduledExecutorService cleanupExecutor;
    private final AtomicLong stateVersion;
    private volatile boolean running;
    
    // 状态过期时间（默认1小时）
    private static final long DEFAULT_STATE_TTL = TimeUnit.HOURS.toMillis(1);
    
    public StateManager() {
        this.stateStore = new ConcurrentHashMap<>();
        this.stateTimestamps = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StateManager-Cleanup");
            t.setDaemon(true);
            return t;
        });
        this.stateVersion = new AtomicLong(0);
        this.running = true;
        
        // 启动定期清理任务
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredStates, 
                                          10, 10, TimeUnit.MINUTES);
        
        logger.info("StateManager initialized");
    }
    
    /**
     * 存储状态
     */
    public void putState(String key, Object value) {
        if (!running) {
            logger.warn("StateManager is not running, ignoring put operation for key: {}", key);
            return;
        }
        
        stateStore.put(key, value);
        stateTimestamps.put(key, System.currentTimeMillis());
        stateVersion.incrementAndGet();
        
        logger.debug("State stored: key={}, type={}", key, value.getClass().getSimpleName());
    }
    
    /**
     * 获取状态
     */
    @SuppressWarnings("unchecked")
    public <T> T getState(String key, Class<T> type) {
        if (!running) {
            return null;
        }
        
        Object value = stateStore.get(key);
        if (value == null) {
            return null;
        }
        
        // 更新访问时间
        stateTimestamps.put(key, System.currentTimeMillis());
        
        if (type.isInstance(value)) {
            logger.debug("State retrieved: key={}, type={}", key, type.getSimpleName());
            return type.cast(value);
        } else {
            logger.warn("State type mismatch: key={}, expected={}, actual={}", 
                       key, type.getSimpleName(), value.getClass().getSimpleName());
            return null;
        }
    }
    
    /**
     * 获取状态（泛型版本）
     */
    @SuppressWarnings("unchecked")
    public <T> T getState(String key) {
        if (!running) {
            return null;
        }
        
        Object value = stateStore.get(key);
        if (value != null) {
            stateTimestamps.put(key, System.currentTimeMillis());
            logger.debug("State retrieved: key={}", key);
        }
        
        return (T) value;
    }
    
    /**
     * 删除状态
     */
    public void removeState(String key) {
        if (!running) {
            return;
        }
        
        Object removed = stateStore.remove(key);
        stateTimestamps.remove(key);
        
        if (removed != null) {
            stateVersion.incrementAndGet();
            logger.debug("State removed: key={}", key);
        }
    }
    
    /**
     * 检查状态是否存在
     */
    public boolean containsState(String key) {
        return running && stateStore.containsKey(key);
    }
    
    /**
     * 获取状态数量
     */
    public int getStateCount() {
        return stateStore.size();
    }
    
    /**
     * 获取状态版本
     */
    public long getStateVersion() {
        return stateVersion.get();
    }
    
    /**
     * 清理过期状态
     */
    private void cleanupExpiredStates() {
        if (!running) {
            return;
        }
        
        long currentTime = System.currentTimeMillis();
        int cleanedCount = 0;
        
        for (String key : stateTimestamps.keySet()) {
            Long timestamp = stateTimestamps.get(key);
            if (timestamp != null && (currentTime - timestamp) > DEFAULT_STATE_TTL) {
                if (stateStore.remove(key) != null) {
                    stateTimestamps.remove(key);
                    cleanedCount++;
                }
            }
        }
        
        if (cleanedCount > 0) {
            stateVersion.incrementAndGet();
            logger.debug("Cleaned up {} expired states", cleanedCount);
        }
    }
    
    /**
     * 清空所有状态
     */
    public void clearAllStates() {
        if (!running) {
            return;
        }
        
        int count = stateStore.size();
        stateStore.clear();
        stateTimestamps.clear();
        stateVersion.incrementAndGet();
        
        logger.info("Cleared all states: {} items removed", count);
    }
    
    /**
     * 获取状态统计信息
     */
    public StateStatistics getStatistics() {
        return new StateStatistics(
            stateStore.size(),
            stateVersion.get(),
            System.currentTimeMillis()
        );
    }
    
    /**
     * 关闭状态管理器
     */
    public void shutdown() {
        logger.info("Shutting down StateManager...");
        running = false;
        
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupExecutor.shutdownNow();
        }
        
        int stateCount = stateStore.size();
        stateStore.clear();
        stateTimestamps.clear();
        
        logger.info("StateManager shutdown completed, cleared {} states", stateCount);
    }
}

/**
 * 状态统计信息
 */
class StateStatistics {
    private final int stateCount;
    private final long stateVersion;
    private final long timestamp;
    
    public StateStatistics(int stateCount, long stateVersion, long timestamp) {
        this.stateCount = stateCount;
        this.stateVersion = stateVersion;
        this.timestamp = timestamp;
    }
    
    public int getStateCount() { return stateCount; }
    public long getStateVersion() { return stateVersion; }
    public long getTimestamp() { return timestamp; }
    
    @Override
    public String toString() {
        return String.format("StateStatistics{count=%d, version=%d, timestamp=%d}", 
                           stateCount, stateVersion, timestamp);
    }
}
