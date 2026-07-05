package com.flare.mq.broker.ack;

/**
 * 确认统计信息
 * 
 * @author FlareMQ Team
 */
public class AckStats {
    
    /**
     * 待确认消息数量
     */
    private int pendingAckCount;
    
    /**
     * 已确认消息数量
     */
    private int ackedCount;
    
    /**
     * 死信消息数量
     */
    private int deadLetterCount;
    
    /**
     * 重试队列大小
     */
    private int retryQueueSize;
    
    /**
     * 总消息数量
     */
    private int totalMessageCount;
    
    /**
     * 确认成功率
     */
    private double ackSuccessRate;
    
    /**
     * 平均确认时间（毫秒）
     */
    private double averageAckTime;
    
    /**
     * 最大确认时间（毫秒）
     */
    private long maxAckTime;
    
    /**
     * 最小确认时间（毫秒）
     */
    private long minAckTime;
    
    /**
     * 统计时间
     */
    private long statisticsTime;
    
    public AckStats() {
        this.statisticsTime = System.currentTimeMillis();
    }
    
    // Getter和Setter方法
    
    public int getPendingAckCount() {
        return pendingAckCount;
    }
    
    public void setPendingAckCount(int pendingAckCount) {
        this.pendingAckCount = pendingAckCount;
        updateAckSuccessRate();
    }
    
    public int getAckedCount() {
        return ackedCount;
    }
    
    public void setAckedCount(int ackedCount) {
        this.ackedCount = ackedCount;
        updateAckSuccessRate();
    }
    
    public int getDeadLetterCount() {
        return deadLetterCount;
    }
    
    public void setDeadLetterCount(int deadLetterCount) {
        this.deadLetterCount = deadLetterCount;
        updateAckSuccessRate();
    }
    
    public int getRetryQueueSize() {
        return retryQueueSize;
    }
    
    public void setRetryQueueSize(int retryQueueSize) {
        this.retryQueueSize = retryQueueSize;
    }
    
    public int getTotalMessageCount() {
        return totalMessageCount;
    }
    
    public void setTotalMessageCount(int totalMessageCount) {
        this.totalMessageCount = totalMessageCount;
        updateAckSuccessRate();
    }
    
    public double getAckSuccessRate() {
        return ackSuccessRate;
    }
    
    public void setAckSuccessRate(double ackSuccessRate) {
        this.ackSuccessRate = ackSuccessRate;
    }
    
    public double getAverageAckTime() {
        return averageAckTime;
    }
    
    public void setAverageAckTime(double averageAckTime) {
        this.averageAckTime = averageAckTime;
    }
    
    public long getMaxAckTime() {
        return maxAckTime;
    }
    
    public void setMaxAckTime(long maxAckTime) {
        this.maxAckTime = maxAckTime;
    }
    
    public long getMinAckTime() {
        return minAckTime;
    }
    
    public void setMinAckTime(long minAckTime) {
        this.minAckTime = minAckTime;
    }
    
    public long getStatisticsTime() {
        return statisticsTime;
    }
    
    public void setStatisticsTime(long statisticsTime) {
        this.statisticsTime = statisticsTime;
    }
    
    /**
     * 获取确认失败率
     */
    public double getAckFailureRate() {
        return 100.0 - ackSuccessRate;
    }
    
    /**
     * 获取死信率
     */
    public double getDeadLetterRate() {
        if (totalMessageCount == 0) {
            return 0.0;
        }
        return (double) deadLetterCount / totalMessageCount * 100.0;
    }
    
    /**
     * 获取重试率
     */
    public double getRetryRate() {
        if (totalMessageCount == 0) {
            return 0.0;
        }
        return (double) retryQueueSize / totalMessageCount * 100.0;
    }
    
    /**
     * 获取处理完成的消息数量
     */
    public int getCompletedMessageCount() {
        return ackedCount + deadLetterCount;
    }
    
    /**
     * 获取处理完成率
     */
    public double getCompletionRate() {
        if (totalMessageCount == 0) {
            return 0.0;
        }
        return (double) getCompletedMessageCount() / totalMessageCount * 100.0;
    }
    
    /**
     * 更新确认成功率
     */
    private void updateAckSuccessRate() {
        if (totalMessageCount == 0) {
            ackSuccessRate = 0.0;
        } else {
            ackSuccessRate = (double) ackedCount / totalMessageCount * 100.0;
        }
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        this.pendingAckCount = 0;
        this.ackedCount = 0;
        this.deadLetterCount = 0;
        this.retryQueueSize = 0;
        this.totalMessageCount = 0;
        this.ackSuccessRate = 0.0;
        this.averageAckTime = 0.0;
        this.maxAckTime = 0;
        this.minAckTime = 0;
        this.statisticsTime = System.currentTimeMillis();
    }
    
    /**
     * 检查系统健康状态
     */
    public HealthStatus getHealthStatus() {
        if (ackSuccessRate >= 95.0 && deadLetterCount == 0) {
            return HealthStatus.HEALTHY;
        } else if (ackSuccessRate >= 90.0 && getDeadLetterRate() < 1.0) {
            return HealthStatus.WARNING;
        } else {
            return HealthStatus.CRITICAL;
        }
    }
    
    @Override
    public String toString() {
        return "AckStats{" +
                "pendingAckCount=" + pendingAckCount +
                ", ackedCount=" + ackedCount +
                ", deadLetterCount=" + deadLetterCount +
                ", retryQueueSize=" + retryQueueSize +
                ", totalMessageCount=" + totalMessageCount +
                ", ackSuccessRate=" + String.format("%.2f%%", ackSuccessRate) +
                ", ackFailureRate=" + String.format("%.2f%%", getAckFailureRate()) +
                ", deadLetterRate=" + String.format("%.2f%%", getDeadLetterRate()) +
                ", retryRate=" + String.format("%.2f%%", getRetryRate()) +
                ", completionRate=" + String.format("%.2f%%", getCompletionRate()) +
                ", averageAckTime=" + String.format("%.2fms", averageAckTime) +
                ", maxAckTime=" + maxAckTime + "ms" +
                ", minAckTime=" + minAckTime + "ms" +
                ", healthStatus=" + getHealthStatus() +
                ", statisticsTime=" + statisticsTime +
                '}';
    }
    
    /**
     * 健康状态枚举
     */
    public enum HealthStatus {
        HEALTHY("健康"),
        WARNING("警告"),
        CRITICAL("严重");
        
        private final String description;
        
        HealthStatus(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
        
        @Override
        public String toString() {
            return name() + "(" + description + ")";
        }
    }
}
