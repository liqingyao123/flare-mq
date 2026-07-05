package com.flare.mq.console.service;

import com.flare.mq.console.model.*;

import java.util.List;

/**
 * 监控服务接口
 * 
 * @author FlareMQ
 * @version 1.0.0
 */
public interface MonitorService {
    
    /**
     * 启动监控服务
     */
    void start();
    
    /**
     * 关闭监控服务
     */
    void shutdown();
    
    /**
     * 刷新系统指标
     */
    void refreshSystemMetrics();
    
    /**
     * 获取系统概览
     */
    SystemOverview getSystemOverview();
    
    /**
     * 获取Broker状态列表
     */
    List<BrokerStatus> getBrokerStatusList();
    
    /**
     * 获取Topic统计信息
     */
    List<TopicStats> getTopicStatsList();
    
    /**
     * 获取消费者组状态
     */
    List<ConsumerGroupStatus> getConsumerGroupStatusList();

    /**
     * 创建 Topic（通过 NameServer 转发到 Broker）
     */
    boolean createTopic(String topicName, int queueCount);

    /**
     * 删除 Topic（通过 NameServer 转发到 Broker）
     */
    boolean deleteTopic(String topicName);

    /**
     * 获取性能指标
     */
    PerformanceMetrics getPerformanceMetrics();
    
    /**
     * 获取集群健康状态
     */
    ClusterHealth getClusterHealth();
    
    /**
     * 获取系统告警列表
     */
    List<SystemAlert> getSystemAlerts();
    
    /**
     * 获取实时TPS统计
     */
    TpsStatistics getTpsStatistics();
    
    /**
     * 获取存储统计信息
     */
    StorageStatistics getStorageStatistics();
    
    /**
     * 检查服务是否运行中
     */
    boolean isRunning();
}
