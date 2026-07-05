package com.flare.mq.broker.stream;

import com.flare.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 窗口实体类
 * 
 * @author FlareMQ Team
 */
public class Window {
    
    private static final Logger logger = LoggerFactory.getLogger(Window.class);
    
    private final String id;
    private final WindowSpec spec;
    private final List<Message> messages;
    private final AtomicInteger messageCount;
    private final long createTime;
    private volatile long lastUpdateTime;
    private volatile boolean completed;
    
    public Window(String id, WindowSpec spec) {
        this.id = id;
        this.spec = spec;
        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.messageCount = new AtomicInteger(0);
        this.createTime = System.currentTimeMillis();
        this.lastUpdateTime = createTime;
        this.completed = false;
        
        logger.debug("Window created: {} with spec: {}", id, spec);
    }
    
    /**
     * 添加消息到窗口
     */
    public void addMessage(Message message) {
        if (completed) {
            logger.warn("Attempting to add message to completed window: {}", id);
            return;
        }
        
        messages.add(message);
        int count = messageCount.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
        
        logger.debug("Added message to window {}: count={}, key={}", id, count, message.getKeys());
    }
    
    /**
     * 检查窗口是否完成
     */
    public boolean isComplete() {
        if (completed) {
            return true;
        }
        
        switch (spec.getType()) {
            case TIME_WINDOW:
                // 时间窗口：检查是否超过窗口大小
                long elapsed = System.currentTimeMillis() - createTime;
                if (elapsed >= spec.getSize()) {
                    completed = true;
                    logger.debug("Time window {} completed after {}ms", id, elapsed);
                }
                break;
                
            case COUNT_WINDOW:
                // 计数窗口：检查消息数量
                if (messageCount.get() >= spec.getSize()) {
                    completed = true;
                    logger.debug("Count window {} completed with {} messages", id, messageCount.get());
                }
                break;
                
            case SESSION_WINDOW:
                // 会话窗口：检查是否超过会话超时
                long idleTime = System.currentTimeMillis() - lastUpdateTime;
                if (idleTime >= spec.getSize()) {
                    completed = true;
                    logger.debug("Session window {} completed after {}ms idle time", id, idleTime);
                }
                break;
        }
        
        return completed;
    }
    
    /**
     * 检查窗口是否过期
     */
    public boolean isExpired(long currentTime) {
        // 窗口创建后超过超时时间则认为过期
        long age = currentTime - createTime;
        boolean expired = age > spec.getTimeout().toMillis();
        
        if (expired) {
            logger.debug("Window {} expired after {}ms", id, age);
        }
        
        return expired;
    }
    
    /**
     * 获取窗口中的所有消息
     */
    public List<Message> getMessages() {
        return new ArrayList<>(messages);
    }
    
    /**
     * 获取消息数量
     */
    public int getMessageCount() {
        return messageCount.get();
    }
    
    /**
     * 获取窗口大小（字节）
     */
    public long getWindowSize() {
        return messages.stream()
                .mapToLong(msg -> msg.getBody() != null ? msg.getBody().length : 0)
                .sum();
    }
    
    /**
     * 获取窗口时间范围
     */
    public WindowTimeRange getTimeRange() {
        if (messages.isEmpty()) {
            return new WindowTimeRange(createTime, createTime);
        }
        
        long minTime = messages.stream()
                .mapToLong(Message::getBornTimestamp)
                .min()
                .orElse(createTime);
        
        long maxTime = messages.stream()
                .mapToLong(Message::getBornTimestamp)
                .max()
                .orElse(createTime);
        
        return new WindowTimeRange(minTime, maxTime);
    }
    
    /**
     * 标记窗口为完成状态
     */
    public void markCompleted() {
        this.completed = true;
        logger.debug("Window {} marked as completed", id);
    }
    
    // Getters
    public String getId() { return id; }
    public WindowSpec getSpec() { return spec; }
    public long getCreateTime() { return createTime; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    public boolean isCompleted() { return completed; }
    
    @Override
    public String toString() {
        return String.format("Window{id='%s', spec=%s, messageCount=%d, completed=%s, createTime=%d}", 
                           id, spec, messageCount.get(), completed, createTime);
    }
}

/**
 * 窗口时间范围
 */
class WindowTimeRange {
    private final long startTime;
    private final long endTime;
    
    public WindowTimeRange(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
    
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getDuration() { return endTime - startTime; }
    
    @Override
    public String toString() {
        return String.format("TimeRange{start=%d, end=%d, duration=%d}", 
                           startTime, endTime, getDuration());
    }
}
