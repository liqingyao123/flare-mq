package com.ruyuan.mq.client.consumer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer统计信息
 * 
 * @author RuYuan MQ Team
 */
public class ConsumerStats {
    
    /**
     * 消费成功总数
     */
    private final AtomicLong consumeSuccessCount = new AtomicLong(0);
    
    /**
     * 消费失败总数
     */
    private final AtomicLong consumeFailureCount = new AtomicLong(0);
    
    /**
     * 消费超时总数
     */
    private final AtomicLong consumeTimeoutCount = new AtomicLong(0);
    
    /**
     * 拉取成功总数
     */
    private final AtomicLong pullSuccessCount = new AtomicLong(0);
    
    /**
     * 拉取失败总数
     */
    private final AtomicLong pullFailureCount = new AtomicLong(0);
    
    /**
     * 拉取到的消息总数
     */
    private final AtomicLong pullMessageCount = new AtomicLong(0);
    
    /**
     * 消费总字节数
     */
    private final AtomicLong consumeTotalBytes = new AtomicLong(0);
    
    /**
     * 最大消费延迟（毫秒）
     */
    private volatile long maxConsumeLatency = 0;
    
    /**
     * 最小消费延迟（毫秒）
     */
    private volatile long minConsumeLatency = Long.MAX_VALUE;
    
    /**
     * 总消费延迟（用于计算平均值）
     */
    private final AtomicLong totalConsumeLatency = new AtomicLong(0);
    
    /**
     * 最大拉取延迟（毫秒）
     */
    private volatile long maxPullLatency = 0;
    
    /**
     * 最小拉取延迟（毫秒）
     */
    private volatile long minPullLatency = Long.MAX_VALUE;
    
    /**
     * 总拉取延迟（用于计算平均值）
     */
    private final AtomicLong totalPullLatency = new AtomicLong(0);
    
    /**
     * 启动时间
     */
    private long startTime;
    
    /**
     * 最后消费时间
     */
    private volatile long lastConsumeTime;
    
    /**
     * 最后成功消费时间
     */
    private volatile long lastSuccessConsumeTime;
    
    /**
     * 最后失败消费时间
     */
    private volatile long lastFailureConsumeTime;
    
    /**
     * 最后拉取时间
     */
    private volatile long lastPullTime;
    
    public ConsumerStats() {
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * 记录消费成功
     */
    public void recordConsumeSuccess(long latency, long messageSize) {
        consumeSuccessCount.incrementAndGet();
        consumeTotalBytes.addAndGet(messageSize);
        updateConsumeLatency(latency);
        lastConsumeTime = System.currentTimeMillis();
        lastSuccessConsumeTime = lastConsumeTime;
    }
    
    /**
     * 记录消费失败
     */
    public void recordConsumeFailure(long latency) {
        consumeFailureCount.incrementAndGet();
        updateConsumeLatency(latency);
        lastConsumeTime = System.currentTimeMillis();
        lastFailureConsumeTime = lastConsumeTime;
    }
    
    /**
     * 记录消费超时
     */
    public void recordConsumeTimeout(long latency) {
        consumeTimeoutCount.incrementAndGet();
        updateConsumeLatency(latency);
        lastConsumeTime = System.currentTimeMillis();
        lastFailureConsumeTime = lastConsumeTime;
    }
    
    /**
     * 记录拉取成功
     */
    public void recordPullSuccess(long latency, int messageCount) {
        pullSuccessCount.incrementAndGet();
        pullMessageCount.addAndGet(messageCount);
        updatePullLatency(latency);
        lastPullTime = System.currentTimeMillis();
    }
    
    /**
     * 记录拉取失败
     */
    public void recordPullFailure(long latency) {
        pullFailureCount.incrementAndGet();
        updatePullLatency(latency);
        lastPullTime = System.currentTimeMillis();
    }
    
    /**
     * 更新消费延迟统计
     */
    private void updateConsumeLatency(long latency) {
        totalConsumeLatency.addAndGet(latency);
        
        // 更新最大延迟
        if (latency > maxConsumeLatency) {
            maxConsumeLatency = latency;
        }
        
        // 更新最小延迟
        if (latency < minConsumeLatency) {
            minConsumeLatency = latency;
        }
    }
    
    /**
     * 更新拉取延迟统计
     */
    private void updatePullLatency(long latency) {
        totalPullLatency.addAndGet(latency);
        
        // 更新最大延迟
        if (latency > maxPullLatency) {
            maxPullLatency = latency;
        }
        
        // 更新最小延迟
        if (latency < minPullLatency) {
            minPullLatency = latency;
        }
    }
    
    // Getter方法
    
    public long getConsumeSuccessCount() {
        return consumeSuccessCount.get();
    }
    
    public long getConsumeFailureCount() {
        return consumeFailureCount.get();
    }
    
    public long getConsumeTimeoutCount() {
        return consumeTimeoutCount.get();
    }
    
    public long getConsumeTotalCount() {
        return consumeSuccessCount.get() + consumeFailureCount.get() + consumeTimeoutCount.get();
    }
    
    public long getPullSuccessCount() {
        return pullSuccessCount.get();
    }
    
    public long getPullFailureCount() {
        return pullFailureCount.get();
    }
    
    public long getPullTotalCount() {
        return pullSuccessCount.get() + pullFailureCount.get();
    }
    
    public long getPullMessageCount() {
        return pullMessageCount.get();
    }
    
    public long getConsumeTotalBytes() {
        return consumeTotalBytes.get();
    }
    
    public long getMaxConsumeLatency() {
        return maxConsumeLatency;
    }
    
    public long getMinConsumeLatency() {
        return minConsumeLatency == Long.MAX_VALUE ? 0 : minConsumeLatency;
    }
    
    public double getAverageConsumeLatency() {
        long totalCount = getConsumeTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) totalConsumeLatency.get() / totalCount;
    }
    
    public long getMaxPullLatency() {
        return maxPullLatency;
    }
    
    public long getMinPullLatency() {
        return minPullLatency == Long.MAX_VALUE ? 0 : minPullLatency;
    }
    
    public double getAveragePullLatency() {
        long totalCount = getPullTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) totalPullLatency.get() / totalCount;
    }
    
    public double getConsumeSuccessRate() {
        long totalCount = getConsumeTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) consumeSuccessCount.get() / totalCount * 100.0;
    }
    
    public double getConsumeFailureRate() {
        long totalCount = getConsumeTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) (consumeFailureCount.get() + consumeTimeoutCount.get()) / totalCount * 100.0;
    }
    
    public double getPullSuccessRate() {
        long totalCount = getPullTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) pullSuccessCount.get() / totalCount * 100.0;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public long getLastConsumeTime() {
        return lastConsumeTime;
    }
    
    public long getLastSuccessConsumeTime() {
        return lastSuccessConsumeTime;
    }
    
    public long getLastFailureConsumeTime() {
        return lastFailureConsumeTime;
    }
    
    public long getLastPullTime() {
        return lastPullTime;
    }
    
    /**
     * 获取运行时间（毫秒）
     */
    public long getRunningTime() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * 获取平均消费TPS
     */
    public double getAverageConsumeTPS() {
        long runningTimeSeconds = getRunningTime() / 1000;
        if (runningTimeSeconds == 0) {
            return 0.0;
        }
        return (double) getConsumeTotalCount() / runningTimeSeconds;
    }
    
    /**
     * 获取平均拉取TPS
     */
    public double getAveragePullTPS() {
        long runningTimeSeconds = getRunningTime() / 1000;
        if (runningTimeSeconds == 0) {
            return 0.0;
        }
        return (double) getPullTotalCount() / runningTimeSeconds;
    }
    
    /**
     * 获取平均消息大小
     */
    public double getAverageMessageSize() {
        long totalCount = getConsumeTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) consumeTotalBytes.get() / totalCount;
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        consumeSuccessCount.set(0);
        consumeFailureCount.set(0);
        consumeTimeoutCount.set(0);
        pullSuccessCount.set(0);
        pullFailureCount.set(0);
        pullMessageCount.set(0);
        consumeTotalBytes.set(0);
        totalConsumeLatency.set(0);
        totalPullLatency.set(0);
        maxConsumeLatency = 0;
        minConsumeLatency = Long.MAX_VALUE;
        maxPullLatency = 0;
        minPullLatency = Long.MAX_VALUE;
        startTime = System.currentTimeMillis();
        lastConsumeTime = 0;
        lastSuccessConsumeTime = 0;
        lastFailureConsumeTime = 0;
        lastPullTime = 0;
    }
    
    @Override
    public String toString() {
        return "ConsumerStats{" +
                "consumeSuccessCount=" + consumeSuccessCount.get() +
                ", consumeFailureCount=" + consumeFailureCount.get() +
                ", consumeTimeoutCount=" + consumeTimeoutCount.get() +
                ", consumeTotalCount=" + getConsumeTotalCount() +
                ", pullSuccessCount=" + pullSuccessCount.get() +
                ", pullFailureCount=" + pullFailureCount.get() +
                ", pullMessageCount=" + pullMessageCount.get() +
                ", consumeTotalBytes=" + consumeTotalBytes.get() +
                ", consumeSuccessRate=" + String.format("%.2f%%", getConsumeSuccessRate()) +
                ", consumeFailureRate=" + String.format("%.2f%%", getConsumeFailureRate()) +
                ", pullSuccessRate=" + String.format("%.2f%%", getPullSuccessRate()) +
                ", averageConsumeLatency=" + String.format("%.2fms", getAverageConsumeLatency()) +
                ", averagePullLatency=" + String.format("%.2fms", getAveragePullLatency()) +
                ", averageConsumeTPS=" + String.format("%.2f", getAverageConsumeTPS()) +
                ", averagePullTPS=" + String.format("%.2f", getAveragePullTPS()) +
                ", averageMessageSize=" + String.format("%.2f", getAverageMessageSize()) + " bytes" +
                ", runningTime=" + getRunningTime() + "ms" +
                '}';
    }
}
