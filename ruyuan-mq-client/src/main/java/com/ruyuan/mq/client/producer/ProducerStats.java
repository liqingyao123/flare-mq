package com.ruyuan.mq.client.producer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer统计信息
 * 
 * @author RuYuan MQ Team
 */
public class ProducerStats {
    
    /**
     * 发送成功总数
     */
    private final AtomicLong sendSuccessCount = new AtomicLong(0);
    
    /**
     * 发送失败总数
     */
    private final AtomicLong sendFailureCount = new AtomicLong(0);
    
    /**
     * 发送超时总数
     */
    private final AtomicLong sendTimeoutCount = new AtomicLong(0);
    
    /**
     * 发送总字节数
     */
    private final AtomicLong sendTotalBytes = new AtomicLong(0);
    
    /**
     * 最大发送延迟（毫秒）
     */
    private volatile long maxSendLatency = 0;
    
    /**
     * 最小发送延迟（毫秒）
     */
    private volatile long minSendLatency = Long.MAX_VALUE;
    
    /**
     * 总发送延迟（用于计算平均值）
     */
    private final AtomicLong totalSendLatency = new AtomicLong(0);
    
    /**
     * 启动时间
     */
    private long startTime;
    
    /**
     * 最后发送时间
     */
    private volatile long lastSendTime;
    
    /**
     * 最后成功发送时间
     */
    private volatile long lastSuccessSendTime;
    
    /**
     * 最后失败发送时间
     */
    private volatile long lastFailureSendTime;
    
    public ProducerStats() {
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * 记录发送成功
     */
    public void recordSendSuccess(long latency, long messageSize) {
        sendSuccessCount.incrementAndGet();
        sendTotalBytes.addAndGet(messageSize);
        updateLatency(latency);
        lastSendTime = System.currentTimeMillis();
        lastSuccessSendTime = lastSendTime;
    }
    
    /**
     * 记录发送失败
     */
    public void recordSendFailure(long latency) {
        sendFailureCount.incrementAndGet();
        updateLatency(latency);
        lastSendTime = System.currentTimeMillis();
        lastFailureSendTime = lastSendTime;
    }
    
    /**
     * 记录发送超时
     */
    public void recordSendTimeout(long latency) {
        sendTimeoutCount.incrementAndGet();
        updateLatency(latency);
        lastSendTime = System.currentTimeMillis();
        lastFailureSendTime = lastSendTime;
    }
    
    /**
     * 更新延迟统计
     */
    private void updateLatency(long latency) {
        totalSendLatency.addAndGet(latency);
        
        // 更新最大延迟
        if (latency > maxSendLatency) {
            maxSendLatency = latency;
        }
        
        // 更新最小延迟
        if (latency < minSendLatency) {
            minSendLatency = latency;
        }
    }
    
    // Getter方法
    
    public long getSendSuccessCount() {
        return sendSuccessCount.get();
    }
    
    public long getSendFailureCount() {
        return sendFailureCount.get();
    }
    
    public long getSendTimeoutCount() {
        return sendTimeoutCount.get();
    }
    
    public long getSendTotalCount() {
        return sendSuccessCount.get() + sendFailureCount.get() + sendTimeoutCount.get();
    }
    
    public long getSendTotalBytes() {
        return sendTotalBytes.get();
    }
    
    public long getMaxSendLatency() {
        return maxSendLatency;
    }
    
    public long getMinSendLatency() {
        return minSendLatency == Long.MAX_VALUE ? 0 : minSendLatency;
    }
    
    public double getAverageSendLatency() {
        long totalCount = getSendTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) totalSendLatency.get() / totalCount;
    }
    
    public double getSendSuccessRate() {
        long totalCount = getSendTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) sendSuccessCount.get() / totalCount * 100.0;
    }
    
    public double getSendFailureRate() {
        long totalCount = getSendTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) (sendFailureCount.get() + sendTimeoutCount.get()) / totalCount * 100.0;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public long getLastSendTime() {
        return lastSendTime;
    }
    
    public long getLastSuccessSendTime() {
        return lastSuccessSendTime;
    }
    
    public long getLastFailureSendTime() {
        return lastFailureSendTime;
    }
    
    /**
     * 获取运行时间（毫秒）
     */
    public long getRunningTime() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * 获取平均TPS
     */
    public double getAverageTPS() {
        long runningTimeSeconds = getRunningTime() / 1000;
        if (runningTimeSeconds == 0) {
            return 0.0;
        }
        return (double) getSendTotalCount() / runningTimeSeconds;
    }
    
    /**
     * 获取平均消息大小
     */
    public double getAverageMessageSize() {
        long totalCount = getSendTotalCount();
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) sendTotalBytes.get() / totalCount;
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        sendSuccessCount.set(0);
        sendFailureCount.set(0);
        sendTimeoutCount.set(0);
        sendTotalBytes.set(0);
        totalSendLatency.set(0);
        maxSendLatency = 0;
        minSendLatency = Long.MAX_VALUE;
        startTime = System.currentTimeMillis();
        lastSendTime = 0;
        lastSuccessSendTime = 0;
        lastFailureSendTime = 0;
    }
    
    @Override
    public String toString() {
        return "ProducerStats{" +
                "sendSuccessCount=" + sendSuccessCount.get() +
                ", sendFailureCount=" + sendFailureCount.get() +
                ", sendTimeoutCount=" + sendTimeoutCount.get() +
                ", sendTotalCount=" + getSendTotalCount() +
                ", sendTotalBytes=" + sendTotalBytes.get() +
                ", sendSuccessRate=" + String.format("%.2f%%", getSendSuccessRate()) +
                ", sendFailureRate=" + String.format("%.2f%%", getSendFailureRate()) +
                ", averageSendLatency=" + String.format("%.2fms", getAverageSendLatency()) +
                ", maxSendLatency=" + maxSendLatency + "ms" +
                ", minSendLatency=" + getMinSendLatency() + "ms" +
                ", averageTPS=" + String.format("%.2f", getAverageTPS()) +
                ", averageMessageSize=" + String.format("%.2f", getAverageMessageSize()) + " bytes" +
                ", runningTime=" + getRunningTime() + "ms" +
                '}';
    }
}
