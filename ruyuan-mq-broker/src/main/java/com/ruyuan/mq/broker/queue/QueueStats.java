package com.ruyuan.mq.broker.queue;

/**
 * Queue统计信息
 * 
 * @author RuYuan MQ Team
 */
public class QueueStats {
    
    /**
     * Queue总数
     */
    private int totalQueueCount;
    
    /**
     * Topic数量
     */
    private int topicCount;
    
    /**
     * 活跃Queue数量
     */
    private int activeQueueCount;
    
    /**
     * 暂停Queue数量
     */
    private int pausedQueueCount;
    
    /**
     * 只读Queue数量
     */
    private int readOnlyQueueCount;
    
    /**
     * 维护中Queue数量
     */
    private int maintenanceQueueCount;
    
    /**
     * 平均每个Topic的Queue数量
     */
    private double averageQueuePerTopic;
    
    /**
     * 最大Queue数量的Topic
     */
    private String maxQueueTopic;
    
    /**
     * 最大Queue数量
     */
    private int maxQueueCount;
    
    /**
     * 最小Queue数量的Topic
     */
    private String minQueueTopic;
    
    /**
     * 最小Queue数量
     */
    private int minQueueCount;
    
    /**
     * 总消息数量
     */
    private long totalMessageCount;
    
    /**
     * 总消息字节数
     */
    private long totalMessageBytes;
    
    /**
     * 统计时间
     */
    private long statisticsTime;
    
    public QueueStats() {
        this.statisticsTime = System.currentTimeMillis();
    }
    
    // Getter和Setter方法
    
    public int getTotalQueueCount() {
        return totalQueueCount;
    }
    
    public void setTotalQueueCount(int totalQueueCount) {
        this.totalQueueCount = totalQueueCount;
    }
    
    public int getTopicCount() {
        return topicCount;
    }
    
    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount;
    }
    
    public int getActiveQueueCount() {
        return activeQueueCount;
    }
    
    public void setActiveQueueCount(int activeQueueCount) {
        this.activeQueueCount = activeQueueCount;
    }
    
    public int getPausedQueueCount() {
        return pausedQueueCount;
    }
    
    public void setPausedQueueCount(int pausedQueueCount) {
        this.pausedQueueCount = pausedQueueCount;
    }
    
    public int getReadOnlyQueueCount() {
        return readOnlyQueueCount;
    }
    
    public void setReadOnlyQueueCount(int readOnlyQueueCount) {
        this.readOnlyQueueCount = readOnlyQueueCount;
    }
    
    public int getMaintenanceQueueCount() {
        return maintenanceQueueCount;
    }
    
    public void setMaintenanceQueueCount(int maintenanceQueueCount) {
        this.maintenanceQueueCount = maintenanceQueueCount;
    }
    
    public double getAverageQueuePerTopic() {
        return averageQueuePerTopic;
    }
    
    public void setAverageQueuePerTopic(double averageQueuePerTopic) {
        this.averageQueuePerTopic = averageQueuePerTopic;
    }
    
    public String getMaxQueueTopic() {
        return maxQueueTopic;
    }
    
    public void setMaxQueueTopic(String maxQueueTopic) {
        this.maxQueueTopic = maxQueueTopic;
    }
    
    public int getMaxQueueCount() {
        return maxQueueCount;
    }
    
    public void setMaxQueueCount(int maxQueueCount) {
        this.maxQueueCount = maxQueueCount;
    }
    
    public String getMinQueueTopic() {
        return minQueueTopic;
    }
    
    public void setMinQueueTopic(String minQueueTopic) {
        this.minQueueTopic = minQueueTopic;
    }
    
    public int getMinQueueCount() {
        return minQueueCount;
    }
    
    public void setMinQueueCount(int minQueueCount) {
        this.minQueueCount = minQueueCount;
    }
    
    public long getTotalMessageCount() {
        return totalMessageCount;
    }
    
    public void setTotalMessageCount(long totalMessageCount) {
        this.totalMessageCount = totalMessageCount;
    }
    
    public long getTotalMessageBytes() {
        return totalMessageBytes;
    }
    
    public void setTotalMessageBytes(long totalMessageBytes) {
        this.totalMessageBytes = totalMessageBytes;
    }
    
    public long getStatisticsTime() {
        return statisticsTime;
    }
    
    public void setStatisticsTime(long statisticsTime) {
        this.statisticsTime = statisticsTime;
    }
    
    /**
     * 计算平均消息大小
     */
    public double getAverageMessageSize() {
        if (totalMessageCount == 0) {
            return 0.0;
        }
        return (double) totalMessageBytes / totalMessageCount;
    }
    
    /**
     * 重置统计信息
     */
    public void reset() {
        this.totalQueueCount = 0;
        this.topicCount = 0;
        this.activeQueueCount = 0;
        this.pausedQueueCount = 0;
        this.readOnlyQueueCount = 0;
        this.maintenanceQueueCount = 0;
        this.averageQueuePerTopic = 0.0;
        this.maxQueueTopic = null;
        this.maxQueueCount = 0;
        this.minQueueTopic = null;
        this.minQueueCount = 0;
        this.totalMessageCount = 0;
        this.totalMessageBytes = 0;
        this.statisticsTime = System.currentTimeMillis();
    }
    
    @Override
    public String toString() {
        return "QueueStats{" +
                "totalQueueCount=" + totalQueueCount +
                ", topicCount=" + topicCount +
                ", activeQueueCount=" + activeQueueCount +
                ", pausedQueueCount=" + pausedQueueCount +
                ", readOnlyQueueCount=" + readOnlyQueueCount +
                ", maintenanceQueueCount=" + maintenanceQueueCount +
                ", averageQueuePerTopic=" + String.format("%.2f", averageQueuePerTopic) +
                ", maxQueueTopic='" + maxQueueTopic + '\'' +
                ", maxQueueCount=" + maxQueueCount +
                ", minQueueTopic='" + minQueueTopic + '\'' +
                ", minQueueCount=" + minQueueCount +
                ", totalMessageCount=" + totalMessageCount +
                ", totalMessageBytes=" + totalMessageBytes +
                ", averageMessageSize=" + String.format("%.2f", getAverageMessageSize()) +
                ", statisticsTime=" + statisticsTime +
                '}';
    }
}
