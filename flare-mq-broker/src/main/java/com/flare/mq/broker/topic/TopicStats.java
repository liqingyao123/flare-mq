package com.flare.mq.broker.topic;

/**
 * Topic统计信息
 * 
 * @author FlareMQ Team
 */
public class TopicStats {
    
    /**
     * Topic总数
     */
    private int totalTopicCount;
    
    /**
     * 队列总数
     */
    private int totalQueueCount;
    
    /**
     * 启用的Topic数量
     */
    private int enabledTopicCount;
    
    /**
     * 禁用的Topic数量
     */
    private int disabledTopicCount;
    
    /**
     * 只读Topic数量
     */
    private int readOnlyTopicCount;
    
    /**
     * 只写Topic数量
     */
    private int writeOnlyTopicCount;
    
    /**
     * 读写Topic数量
     */
    private int readWriteTopicCount;
    
    /**
     * 平均队列数
     */
    private double averageQueueCount;
    
    /**
     * 最大队列数
     */
    private int maxQueueCount;
    
    /**
     * 最小队列数
     */
    private int minQueueCount;
    
    /**
     * 统计时间
     */
    private long statisticsTime;
    
    public TopicStats() {
        this.statisticsTime = System.currentTimeMillis();
    }
    
    // Getter和Setter方法
    
    public int getTotalTopicCount() {
        return totalTopicCount;
    }
    
    public void setTotalTopicCount(int totalTopicCount) {
        this.totalTopicCount = totalTopicCount;
    }
    
    public int getTotalQueueCount() {
        return totalQueueCount;
    }
    
    public void setTotalQueueCount(int totalQueueCount) {
        this.totalQueueCount = totalQueueCount;
        // 计算平均队列数
        if (totalTopicCount > 0) {
            this.averageQueueCount = (double) totalQueueCount / totalTopicCount;
        }
    }
    
    public int getEnabledTopicCount() {
        return enabledTopicCount;
    }
    
    public void setEnabledTopicCount(int enabledTopicCount) {
        this.enabledTopicCount = enabledTopicCount;
    }
    
    public int getDisabledTopicCount() {
        return disabledTopicCount;
    }
    
    public void setDisabledTopicCount(int disabledTopicCount) {
        this.disabledTopicCount = disabledTopicCount;
    }
    
    public int getReadOnlyTopicCount() {
        return readOnlyTopicCount;
    }
    
    public void setReadOnlyTopicCount(int readOnlyTopicCount) {
        this.readOnlyTopicCount = readOnlyTopicCount;
    }
    
    public int getWriteOnlyTopicCount() {
        return writeOnlyTopicCount;
    }
    
    public void setWriteOnlyTopicCount(int writeOnlyTopicCount) {
        this.writeOnlyTopicCount = writeOnlyTopicCount;
    }
    
    public int getReadWriteTopicCount() {
        return readWriteTopicCount;
    }
    
    public void setReadWriteTopicCount(int readWriteTopicCount) {
        this.readWriteTopicCount = readWriteTopicCount;
    }
    
    public double getAverageQueueCount() {
        return averageQueueCount;
    }
    
    public void setAverageQueueCount(double averageQueueCount) {
        this.averageQueueCount = averageQueueCount;
    }
    
    public int getMaxQueueCount() {
        return maxQueueCount;
    }
    
    public void setMaxQueueCount(int maxQueueCount) {
        this.maxQueueCount = maxQueueCount;
    }
    
    public int getMinQueueCount() {
        return minQueueCount;
    }
    
    public void setMinQueueCount(int minQueueCount) {
        this.minQueueCount = minQueueCount;
    }
    
    public long getStatisticsTime() {
        return statisticsTime;
    }
    
    public void setStatisticsTime(long statisticsTime) {
        this.statisticsTime = statisticsTime;
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        this.totalTopicCount = 0;
        this.totalQueueCount = 0;
        this.enabledTopicCount = 0;
        this.disabledTopicCount = 0;
        this.readOnlyTopicCount = 0;
        this.writeOnlyTopicCount = 0;
        this.readWriteTopicCount = 0;
        this.averageQueueCount = 0.0;
        this.maxQueueCount = 0;
        this.minQueueCount = 0;
        this.statisticsTime = System.currentTimeMillis();
    }
    
    @Override
    public String toString() {
        return "TopicStats{" +
                "totalTopicCount=" + totalTopicCount +
                ", totalQueueCount=" + totalQueueCount +
                ", enabledTopicCount=" + enabledTopicCount +
                ", disabledTopicCount=" + disabledTopicCount +
                ", readOnlyTopicCount=" + readOnlyTopicCount +
                ", writeOnlyTopicCount=" + writeOnlyTopicCount +
                ", readWriteTopicCount=" + readWriteTopicCount +
                ", averageQueueCount=" + String.format("%.2f", averageQueueCount) +
                ", maxQueueCount=" + maxQueueCount +
                ", minQueueCount=" + minQueueCount +
                ", statisticsTime=" + statisticsTime +
                '}';
    }
}
