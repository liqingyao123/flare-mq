package com.ruyuan.mq.broker.stream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 流处理统计信息
 * 
 * @author RuYuan MQ Team
 */
public class StreamStatistics {
    
    private final AtomicLong processedMessages;
    private final AtomicLong failedMessages;
    private final LongAdder totalProcessingTime;
    private final AtomicLong windowsCreated;
    private final AtomicLong windowsCompleted;
    private final AtomicLong stateOperations;
    private final long startTime;
    
    public StreamStatistics() {
        this.processedMessages = new AtomicLong(0);
        this.failedMessages = new AtomicLong(0);
        this.totalProcessingTime = new LongAdder();
        this.windowsCreated = new AtomicLong(0);
        this.windowsCompleted = new AtomicLong(0);
        this.stateOperations = new AtomicLong(0);
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * 增加已处理消息数
     */
    public void incrementProcessedMessages() {
        processedMessages.incrementAndGet();
    }
    
    /**
     * 增加失败消息数
     */
    public void incrementFailedMessages() {
        failedMessages.incrementAndGet();
    }
    
    /**
     * 记录处理时间
     */
    public void recordProcessingTime(long timeMs) {
        totalProcessingTime.add(timeMs);
    }
    
    /**
     * 增加创建的窗口数
     */
    public void incrementWindowsCreated() {
        windowsCreated.incrementAndGet();
    }
    
    /**
     * 增加完成的窗口数
     */
    public void incrementWindowsCompleted() {
        windowsCompleted.incrementAndGet();
    }
    
    /**
     * 增加状态操作数
     */
    public void incrementStateOperations() {
        stateOperations.incrementAndGet();
    }
    
    /**
     * 获取已处理消息数
     */
    public long getProcessedMessages() {
        return processedMessages.get();
    }
    
    /**
     * 获取失败消息数
     */
    public long getFailedMessages() {
        return failedMessages.get();
    }
    
    /**
     * 获取成功率
     */
    public double getSuccessRate() {
        long total = processedMessages.get();
        if (total == 0) {
            return 1.0;
        }
        long successful = total - failedMessages.get();
        return (double) successful / total;
    }
    
    /**
     * 获取平均处理时间
     */
    public double getAverageProcessingTime() {
        long total = processedMessages.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) totalProcessingTime.sum() / total;
    }
    
    /**
     * 获取处理速率（消息/秒）
     */
    public double getProcessingRate() {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed == 0) {
            return 0.0;
        }
        return (double) processedMessages.get() * 1000 / elapsed;
    }
    
    /**
     * 获取创建的窗口数
     */
    public long getWindowsCreated() {
        return windowsCreated.get();
    }
    
    /**
     * 获取完成的窗口数
     */
    public long getWindowsCompleted() {
        return windowsCompleted.get();
    }
    
    /**
     * 获取状态操作数
     */
    public long getStateOperations() {
        return stateOperations.get();
    }
    
    /**
     * 获取运行时间
     */
    public long getUptime() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        processedMessages.set(0);
        failedMessages.set(0);
        totalProcessingTime.reset();
        windowsCreated.set(0);
        windowsCompleted.set(0);
        stateOperations.set(0);
    }
    
    /**
     * 获取统计摘要
     */
    public String getSummary() {
        return String.format(
            "StreamStatistics{" +
            "processed=%d, failed=%d, successRate=%.2f%%, " +
            "avgProcessingTime=%.2fms, processingRate=%.2f msg/s, " +
            "windowsCreated=%d, windowsCompleted=%d, stateOps=%d, " +
            "uptime=%dms}",
            getProcessedMessages(),
            getFailedMessages(),
            getSuccessRate() * 100,
            getAverageProcessingTime(),
            getProcessingRate(),
            getWindowsCreated(),
            getWindowsCompleted(),
            getStateOperations(),
            getUptime()
        );
    }
    
    @Override
    public String toString() {
        return getSummary();
    }
}
