package com.ruyuan.mq.console;

import com.ruyuan.mq.console.controller.MonitorController;
import com.ruyuan.mq.console.model.*;
import com.ruyuan.mq.console.service.MonitorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Console应用测试
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class ConsoleApplicationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsoleApplicationTest.class);
    
    private ConsoleApplication consoleApp;
    private MonitorController monitorController;
    private MonitorService monitorService;
    
    @BeforeEach
    void setUp() {
        logger.info("=== 开始Console应用测试 ===");
        consoleApp = new ConsoleApplication();
        consoleApp.start();
        
        monitorController = consoleApp.getMonitorController();
        monitorService = monitorController != null ? 
            new com.ruyuan.mq.console.service.impl.MonitorServiceImpl() : null;
        
        if (monitorService != null) {
            monitorService.start();
        }
    }
    
    @AfterEach
    void tearDown() {
        if (consoleApp != null) {
            consoleApp.shutdown();
        }
        if (monitorService != null) {
            monitorService.shutdown();
        }
        logger.info("Console应用测试完成");
    }
    
    @Test
    void testConsoleApplicationStartup() {
        logger.info("--- 测试Console应用启动 ---");
        
        // 验证应用启动状态
        assertTrue(consoleApp.isRunning(), "Console应用应该处于运行状态");
        assertNotNull(monitorController, "监控控制器应该不为空");
        
        logger.info("Console应用启动测试通过");
    }
    
    @Test
    void testSystemOverview() {
        logger.info("--- 测试系统概览 ---");
        
        String overview = monitorController.getSystemOverview();
        assertNotNull(overview, "系统概览不应为空");
        assertTrue(overview.contains("RuYuan MQ"), "应包含系统名称");
        assertTrue(overview.contains("System Overview"), "应包含概览标题");
        
        logger.info("系统概览: \n{}", overview);
        logger.info("系统概览测试通过");
    }
    
    @Test
    void testBrokerStatusReport() {
        logger.info("--- 测试Broker状态报告 ---");
        
        String report = monitorController.getBrokerStatusReport();
        assertNotNull(report, "Broker状态报告不应为空");
        assertTrue(report.contains("Broker状态报告"), "应包含报告标题");
        
        logger.info("Broker状态报告: \n{}", report);
        logger.info("Broker状态报告测试通过");
    }
    
    @Test
    void testTopicStatsReport() {
        logger.info("--- 测试Topic统计报告 ---");
        
        String report = monitorController.getTopicStatsReport();
        assertNotNull(report, "Topic统计报告不应为空");
        assertTrue(report.contains("Topic统计报告"), "应包含报告标题");
        
        logger.info("Topic统计报告: \n{}", report);
        logger.info("Topic统计报告测试通过");
    }
    
    @Test
    void testConsumerGroupReport() {
        logger.info("--- 测试消费者组报告 ---");
        
        String report = monitorController.getConsumerGroupReport();
        assertNotNull(report, "消费者组报告不应为空");
        assertTrue(report.contains("消费者组状态报告"), "应包含报告标题");
        
        logger.info("消费者组报告: \n{}", report);
        logger.info("消费者组报告测试通过");
    }
    
    @Test
    void testPerformanceReport() {
        logger.info("--- 测试性能指标报告 ---");
        
        String report = monitorController.getPerformanceReport();
        assertNotNull(report, "性能指标报告不应为空");
        assertTrue(report.contains("性能指标报告"), "应包含报告标题");
        assertTrue(report.contains("延迟"), "应包含延迟信息");
        
        logger.info("性能指标报告: \n{}", report);
        logger.info("性能指标报告测试通过");
    }
    
    @Test
    void testClusterHealthReport() {
        logger.info("--- 测试集群健康报告 ---");
        
        String report = monitorController.getClusterHealthReport();
        assertNotNull(report, "集群健康报告不应为空");
        assertTrue(report.contains("集群健康报告"), "应包含报告标题");
        assertTrue(report.contains("整体状态"), "应包含整体状态");
        
        logger.info("集群健康报告: \n{}", report);
        logger.info("集群健康报告测试通过");
    }
    
    @Test
    void testSystemAlertsReport() {
        logger.info("--- 测试系统告警报告 ---");
        
        String report = monitorController.getSystemAlertsReport();
        assertNotNull(report, "系统告警报告不应为空");
        assertTrue(report.contains("系统告警报告"), "应包含报告标题");
        
        logger.info("系统告警报告: \n{}", report);
        logger.info("系统告警报告测试通过");
    }
    
    @Test
    void testTpsReport() {
        logger.info("--- 测试TPS统计报告 ---");
        
        String report = monitorController.getTpsReport();
        assertNotNull(report, "TPS统计报告不应为空");
        assertTrue(report.contains("TPS统计报告"), "应包含报告标题");
        assertTrue(report.contains("当前TPS"), "应包含当前TPS");
        
        logger.info("TPS统计报告: \n{}", report);
        logger.info("TPS统计报告测试通过");
    }
    
    @Test
    void testStorageReport() {
        logger.info("--- 测试存储统计报告 ---");
        
        String report = monitorController.getStorageReport();
        assertNotNull(report, "存储统计报告不应为空");
        assertTrue(report.contains("存储统计报告"), "应包含报告标题");
        assertTrue(report.contains("总容量"), "应包含总容量信息");
        
        logger.info("存储统计报告: \n{}", report);
        logger.info("存储统计报告测试通过");
    }
    
    @Test
    void testFullMonitorReport() {
        logger.info("--- 测试完整监控报告 ---");
        
        String fullReport = monitorController.getFullMonitorReport();
        assertNotNull(fullReport, "完整监控报告不应为空");
        assertTrue(fullReport.length() > 1000, "完整报告应该包含足够的内容");
        
        // 验证包含各个部分
        assertTrue(fullReport.contains("System Overview"), "应包含系统概览");
        assertTrue(fullReport.contains("Broker状态"), "应包含Broker状态");
        assertTrue(fullReport.contains("Topic统计"), "应包含Topic统计");
        assertTrue(fullReport.contains("性能指标"), "应包含性能指标");
        
        logger.info("完整监控报告长度: {} 字符", fullReport.length());
        logger.info("完整监控报告测试通过");
    }
    
    @Test
    void testMonitorServiceDataRefresh() throws InterruptedException {
        logger.info("--- 测试监控服务数据刷新 ---");
        
        if (monitorService != null) {
            // 获取初始数据
            SystemOverview initialOverview = monitorService.getSystemOverview();
            assertNotNull(initialOverview, "初始系统概览不应为空");
            
            // 刷新数据
            monitorService.refreshSystemMetrics();
            
            // 等待一小段时间
            Thread.sleep(100);
            
            // 获取刷新后的数据
            SystemOverview refreshedOverview = monitorService.getSystemOverview();
            assertNotNull(refreshedOverview, "刷新后的系统概览不应为空");
            
            // 验证数据有效性
            assertTrue(refreshedOverview.getTotalBrokers() >= 0, "Broker数量应该非负");
            assertTrue(refreshedOverview.getTotalTopics() >= 0, "Topic数量应该非负");
            assertTrue(refreshedOverview.getCurrentTps() >= 0, "TPS应该非负");
            
            logger.info("监控服务数据刷新测试通过");
        } else {
            logger.warn("监控服务为空，跳过数据刷新测试");
        }
    }
    
    @Test
    void testMonitorServiceComponents() {
        logger.info("--- 测试监控服务组件 ---");
        
        if (monitorService != null) {
            // 测试各个组件
            List<BrokerStatus> brokers = monitorService.getBrokerStatusList();
            assertNotNull(brokers, "Broker状态列表不应为空");
            
            List<TopicStats> topics = monitorService.getTopicStatsList();
            assertNotNull(topics, "Topic统计列表不应为空");
            
            List<ConsumerGroupStatus> groups = monitorService.getConsumerGroupStatusList();
            assertNotNull(groups, "消费者组状态列表不应为空");
            
            PerformanceMetrics metrics = monitorService.getPerformanceMetrics();
            assertNotNull(metrics, "性能指标不应为空");
            
            ClusterHealth health = monitorService.getClusterHealth();
            assertNotNull(health, "集群健康状态不应为空");
            
            TpsStatistics tps = monitorService.getTpsStatistics();
            assertNotNull(tps, "TPS统计不应为空");
            
            StorageStatistics storage = monitorService.getStorageStatistics();
            assertNotNull(storage, "存储统计不应为空");
            
            List<SystemAlert> alerts = monitorService.getSystemAlerts();
            assertNotNull(alerts, "系统告警列表不应为空");
            
            logger.info("监控服务组件测试通过");
        } else {
            logger.warn("监控服务为空，跳过组件测试");
        }
    }
}
