package com.ruyuan.mq.console.controller;

import com.ruyuan.mq.console.model.*;
import com.ruyuan.mq.console.service.MonitorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 监控控制器
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class MonitorController {
    
    private static final Logger logger = LoggerFactory.getLogger(MonitorController.class);
    
    private final MonitorService monitorService;
    
    public MonitorController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }
    
    /**
     * 获取系统概览信息
     */
    public String getSystemOverview() {
        try {
            SystemOverview overview = monitorService.getSystemOverview();
            return overview.toString();
        } catch (Exception e) {
            logger.error("Error getting system overview", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取Broker状态报告
     */
    public String getBrokerStatusReport() {
        try {
            List<BrokerStatus> brokerList = monitorService.getBrokerStatusList();
            StringBuilder sb = new StringBuilder();
            sb.append("=== Broker状态报告 ===\n");
            
            for (BrokerStatus broker : brokerList) {
                sb.append(String.format("Broker: %s (%s)\n", broker.getBrokerName(), broker.getBrokerAddr()));
                sb.append(String.format("  集群: %s, 角色: %s, 状态: %s\n", 
                         broker.getClusterName(), broker.getRole(), broker.getStatus()));
                sb.append(String.format("  健康: %s, TPS: %.2f\n", 
                         broker.isHealthy() ? "正常" : "异常", broker.getCurrentTps()));
                sb.append(String.format("  资源: CPU=%.1f%%, 内存=%.1f%%, 磁盘=%.1f%%\n",
                         broker.getCpuUsage() * 100, broker.getMemoryUsage() * 100, broker.getDiskUsage() * 100));
                sb.append(String.format("  连接数: %d, 消息数: %d\n\n", 
                         broker.getActiveConnections(), broker.getTotalMessages()));
            }
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting broker status report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取Topic统计报告
     */
    public String getTopicStatsReport() {
        try {
            List<TopicStats> topicList = monitorService.getTopicStatsList();
            StringBuilder sb = new StringBuilder();
            sb.append("=== Topic统计报告 ===\n");
            
            for (TopicStats topic : topicList) {
                sb.append(String.format("Topic: %s\n", topic.getTopicName()));
                sb.append(String.format("  队列数: %d, 消息数: %d\n", 
                         topic.getQueueCount(), topic.getTotalMessages()));
                sb.append(String.format("  TPS: %.2f, 大小: %s\n", 
                         topic.getCurrentTps(), formatSize(topic.getTotalSize())));
                sb.append(String.format("  更新时间: %s\n\n", topic.getLastUpdateTime()));
            }
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting topic stats report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取消费者组状态报告
     */
    public String getConsumerGroupReport() {
        try {
            List<ConsumerGroupStatus> groupList = monitorService.getConsumerGroupStatusList();
            StringBuilder sb = new StringBuilder();
            sb.append("=== 消费者组状态报告 ===\n");
            
            for (ConsumerGroupStatus group : groupList) {
                sb.append(String.format("消费者组: %s\n", group.getGroupName()));
                sb.append(String.format("  订阅Topic: %s, 状态: %s\n", 
                         group.getSubscriptionTopic(), group.getStatus()));
                sb.append(String.format("  消费者数: %d, 消费TPS: %.2f\n", 
                         group.getConsumerCount(), group.getConsumeTps()));
                sb.append(String.format("  已消费: %d, 积压: %d\n", 
                         group.getTotalConsumed(), group.getLag()));
                sb.append(String.format("  更新时间: %s\n\n", group.getLastUpdateTime()));
            }
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting consumer group report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取性能指标报告
     */
    public String getPerformanceReport() {
        try {
            PerformanceMetrics metrics = monitorService.getPerformanceMetrics();
            StringBuilder sb = new StringBuilder();
            sb.append("=== 性能指标报告 ===\n");
            sb.append(String.format("读取延迟: %.2f ms\n", metrics.getAvgReadLatency()));
            sb.append(String.format("写入延迟: %.2f ms\n", metrics.getAvgWriteLatency()));
            sb.append(String.format("存储利用率: %.1f%%\n", metrics.getStorageUtilization() * 100));
            sb.append(String.format("缓存命中率: %.1f%%\n", metrics.getCacheHitRate() * 100));
            sb.append(String.format("压缩比: %.2fx\n", metrics.getCompressionRatio()));
            sb.append(String.format("网络吞吐量: %.2f MB/s\n", metrics.getNetworkThroughput()));
            sb.append(String.format("总操作数: %d\n", metrics.getTotalOperations()));
            sb.append(String.format("更新时间: %s\n", metrics.getTimestamp()));
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting performance report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取集群健康报告
     */
    public String getClusterHealthReport() {
        try {
            ClusterHealth health = monitorService.getClusterHealth();
            StringBuilder sb = new StringBuilder();
            sb.append("=== 集群健康报告 ===\n");
            sb.append(String.format("整体状态: %s\n", health.getOverallStatus()));
            sb.append(String.format("节点状态: %d/%d (健康/总数)\n", 
                     health.getHealthyNodes(), health.getTotalNodes()));
            sb.append(String.format("健康比例: %.1f%%\n", health.getHealthRatio() * 100));
            sb.append(String.format("主节点: %s\n", health.getMasterBroker()));
            sb.append(String.format("活跃连接: %d\n", health.getActiveConnections()));
            sb.append(String.format("检查时间: %s\n", health.getLastCheckTime()));
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting cluster health report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取系统告警报告
     */
    public String getSystemAlertsReport() {
        try {
            List<SystemAlert> alerts = monitorService.getSystemAlerts();
            StringBuilder sb = new StringBuilder();
            sb.append("=== 系统告警报告 ===\n");
            
            if (alerts.isEmpty()) {
                sb.append("当前无系统告警\n");
            } else {
                for (SystemAlert alert : alerts) {
                    sb.append(String.format("[%s] %s\n", alert.getAlertType(), alert.getTitle()));
                    sb.append(String.format("  消息: %s\n", alert.getMessage()));
                    sb.append(String.format("  来源: %s, 状态: %s\n", 
                             alert.getSource(), alert.isResolved() ? "已解决" : "未解决"));
                    sb.append(String.format("  时间: %s\n\n", alert.getCreateTime()));
                }
            }
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting system alerts report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取TPS统计报告
     */
    public String getTpsReport() {
        try {
            TpsStatistics tps = monitorService.getTpsStatistics();
            StringBuilder sb = new StringBuilder();
            sb.append("=== TPS统计报告 ===\n");
            sb.append(String.format("当前TPS: %.2f\n", tps.getCurrentTps()));
            sb.append(String.format("平均TPS: %.2f\n", tps.getAvgTps()));
            sb.append(String.format("最大TPS: %.2f\n", tps.getMaxTps()));
            sb.append(String.format("最小TPS: %.2f\n", tps.getMinTps()));
            sb.append(String.format("历史数据点: %d\n", tps.getHistory().size()));
            sb.append(String.format("更新时间: %s\n", tps.getLastUpdateTime()));
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting TPS report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取存储统计报告
     */
    public String getStorageReport() {
        try {
            StorageStatistics storage = monitorService.getStorageStatistics();
            StringBuilder sb = new StringBuilder();
            sb.append("=== 存储统计报告 ===\n");
            sb.append(String.format("总容量: %s\n", storage.formatSize(storage.getTotalSize())));
            sb.append(String.format("已使用: %s\n", storage.formatSize(storage.getUsedSize())));
            sb.append(String.format("剩余空间: %s\n", storage.formatSize(storage.getFreeSize())));
            sb.append(String.format("使用率: %.1f%%\n", storage.getUsageRatio() * 100));
            sb.append(String.format("文件数: %d\n", storage.getTotalFiles()));
            sb.append(String.format("消息数: %d\n", storage.getTotalMessages()));
            sb.append(String.format("压缩比: %.2fx\n", storage.getCompressionRatio()));
            sb.append(String.format("更新时间: %s\n", storage.getLastUpdateTime()));
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error getting storage report", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * 获取完整的监控报告
     */
    public String getFullMonitorReport() {
        StringBuilder sb = new StringBuilder();
        sb.append(getSystemOverview()).append("\n");
        sb.append(getBrokerStatusReport()).append("\n");
        sb.append(getTopicStatsReport()).append("\n");
        sb.append(getConsumerGroupReport()).append("\n");
        sb.append(getPerformanceReport()).append("\n");
        sb.append(getClusterHealthReport()).append("\n");
        sb.append(getTpsReport()).append("\n");
        sb.append(getStorageReport()).append("\n");
        sb.append(getSystemAlertsReport());
        
        return sb.toString();
    }
    
    /**
     * 格式化文件大小
     */
    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
