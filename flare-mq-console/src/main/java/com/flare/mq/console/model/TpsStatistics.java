package com.flare.mq.console.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * TPS统计信息
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class TpsStatistics {
    
    private double currentTps;
    private double avgTps;
    private double maxTps;
    private double minTps;
    private List<TpsDataPoint> history;
    private LocalDateTime lastUpdateTime;
    
    public TpsStatistics() {
        this.history = new ArrayList<>();
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    /**
     * 添加TPS数据点
     */
    public void addDataPoint(double tps) {
        TpsDataPoint dataPoint = new TpsDataPoint(tps, LocalDateTime.now());
        history.add(dataPoint);
        
        // 保持历史数据在合理范围内
        if (history.size() > 100) {
            history.remove(0);
        }
        
        // 更新统计信息
        updateStatistics();
    }
    
    /**
     * 更新统计信息
     */
    private void updateStatistics() {
        if (history.isEmpty()) {
            return;
        }
        
        double sum = 0;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        
        for (TpsDataPoint point : history) {
            double tps = point.getTps();
            sum += tps;
            max = Math.max(max, tps);
            min = Math.min(min, tps);
        }
        
        this.avgTps = sum / history.size();
        this.maxTps = max;
        this.minTps = min;
        this.currentTps = history.get(history.size() - 1).getTps();
        this.lastUpdateTime = LocalDateTime.now();
    }
    
    // Getters and Setters
    public double getCurrentTps() { return currentTps; }
    public void setCurrentTps(double currentTps) { this.currentTps = currentTps; }
    
    public double getAvgTps() { return avgTps; }
    public void setAvgTps(double avgTps) { this.avgTps = avgTps; }
    
    public double getMaxTps() { return maxTps; }
    public void setMaxTps(double maxTps) { this.maxTps = maxTps; }
    
    public double getMinTps() { return minTps; }
    public void setMinTps(double minTps) { this.minTps = minTps; }
    
    public List<TpsDataPoint> getHistory() { return new ArrayList<>(history); }
    
    public LocalDateTime getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(LocalDateTime lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    @Override
    public String toString() {
        return String.format("TpsStatistics{current=%.2f, avg=%.2f, max=%.2f, min=%.2f, points=%d}",
                currentTps, avgTps, maxTps, minTps, history.size());
    }
    
    /**
     * TPS数据点
     */
    public static class TpsDataPoint {
        private double tps;
        private LocalDateTime timestamp;
        
        public TpsDataPoint(double tps, LocalDateTime timestamp) {
            this.tps = tps;
            this.timestamp = timestamp;
        }
        
        public double getTps() { return tps; }
        public LocalDateTime getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format("TpsDataPoint{tps=%.2f, time=%s}", tps, timestamp);
        }
    }
}
