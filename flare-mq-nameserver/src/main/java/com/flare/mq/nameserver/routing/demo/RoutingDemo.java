package com.flare.mq.nameserver.routing.demo;

import com.flare.mq.nameserver.routing.SmartRoutingEngine;
import com.flare.mq.nameserver.routing.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 路由演示程序
 * 
 * @author FlareMQ Team
 */
public class RoutingDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(RoutingDemo.class);
    
    public static void main(String[] args) {
        logger.info("=== FlareMQ 智能路由演示 ===");
        
        // 创建路由引擎
        SmartRoutingEngine routingEngine = new SmartRoutingEngine();
        
        try {
            // 演示基本路由功能
            demonstrateBasicRouting(routingEngine);
            
            // 演示不同消息类型的路由
            demonstrateMessageTypeRouting(routingEngine);
            
            // 演示负载均衡
            demonstrateLoadBalancing(routingEngine);
            
            // 演示故障转移
            demonstrateFailover(routingEngine);
            
            // 显示统计信息
            showStatistics(routingEngine);
            
        } finally {
            routingEngine.shutdown();
        }
        
        logger.info("=== 演示完成 ===");
    }
    
    /**
     * 演示基本路由功能
     */
    private static void demonstrateBasicRouting(SmartRoutingEngine routingEngine) {
        logger.info("\n--- 基本路由功能演示 ---");
        
        // 创建订单消息
        SimpleMessage orderMessage = new SimpleMessage("order.payment", "pay", "order-12345", 
                                                      "支付订单消息".getBytes());
        orderMessage.setBusinessPriority(1); // 高优先级
        orderMessage.setMessageType("order");
        
        RouteResult result = routingEngine.route(orderMessage);
        
        if (result.isSuccess()) {
            logger.info("路由成功:");
            logger.info("  选择的Broker: {}", result.getBroker().getBrokerId());
            logger.info("  选择的队列: {}", result.getQueueId());
            logger.info("  路由路径: {}", result.getRoutePath());
            logger.info("  路由耗时: {}ms", result.getTotalRouteTime());
        } else {
            logger.error("路由失败: {}", result.getErrorMessage());
        }
    }
    
    /**
     * 演示不同消息类型的路由
     */
    private static void demonstrateMessageTypeRouting(SmartRoutingEngine routingEngine) {
        logger.info("\n--- 不同消息类型路由演示 ---");
        
        // 1. 日志消息
        SimpleMessage logMessage = new SimpleMessage("log.audit", "audit", "audit-001", 
                                                    "审计日志".getBytes());
        logMessage.setMessageType("log");
        RouteResult logResult = routingEngine.route(logMessage);
        logger.info("日志消息路由到: {}", logResult.isSuccess() ? 
                   logResult.getBroker().getBrokerId() : "失败");
        
        // 2. 顺序消息
        SimpleMessage orderedMessage = new SimpleMessage("order.sequence", "seq", "user-123", 
                                                        "顺序消息".getBytes());
        orderedMessage.setOrdered(true);
        RouteResult orderedResult = routingEngine.route(orderedMessage);
        logger.info("顺序消息路由到: {} (队列: {})", 
                   orderedResult.isSuccess() ? orderedResult.getBroker().getBrokerId() : "失败",
                   orderedResult.isSuccess() ? orderedResult.getQueueId() : "N/A");
        
        // 3. 事务消息
        SimpleMessage txMessage = new SimpleMessage("order.transaction", "tx", "tx-456", 
                                                   "事务消息".getBytes());
        txMessage.setTransaction(true);
        RouteResult txResult = routingEngine.route(txMessage);
        logger.info("事务消息路由到: {}", txResult.isSuccess() ? 
                   txResult.getBroker().getBrokerId() : "失败");
    }
    
    /**
     * 演示负载均衡
     */
    private static void demonstrateLoadBalancing(SmartRoutingEngine routingEngine) {
        logger.info("\n--- 负载均衡演示 ---");
        
        // 发送多条消息，观察负载分布
        int messageCount = 20;
        java.util.Map<String, Integer> brokerCount = new java.util.HashMap<>();
        
        for (int i = 0; i < messageCount; i++) {
            SimpleMessage message = new SimpleMessage("order.test", "test", "test-" + i, 
                                                     ("测试消息 " + i).getBytes());
            RouteResult result = routingEngine.route(message);
            
            if (result.isSuccess()) {
                String brokerId = result.getBroker().getBrokerId();
                brokerCount.put(brokerId, brokerCount.getOrDefault(brokerId, 0) + 1);
            }
        }
        
        logger.info("负载分布结果 ({}条消息):", messageCount);
        for (java.util.Map.Entry<String, Integer> entry : brokerCount.entrySet()) {
            double percentage = (double) entry.getValue() / messageCount * 100;
            logger.info("  {}: {} 条消息 ({:.1f}%)", entry.getKey(), entry.getValue(), percentage);
        }
    }
    
    /**
     * 演示故障转移
     */
    private static void demonstrateFailover(SmartRoutingEngine routingEngine) {
        logger.info("\n--- 故障转移演示 ---");
        
        // 模拟Broker故障
        logger.info("模拟 order-broker-1 故障...");
        routingEngine.updateBrokerHealth("order-broker-1", false);
        
        // 发送消息测试故障转移
        SimpleMessage message = new SimpleMessage("order.failover", "test", "failover-test", 
                                                 "故障转移测试".getBytes());
        RouteResult result = routingEngine.route(message);
        
        if (result.isSuccess()) {
            logger.info("故障转移成功:");
            logger.info("  路由到Broker: {}", result.getBroker().getBrokerId());
            logger.info("  是否为故障转移: {}", result.getClusterRoute().isFailover());
        } else {
            logger.error("故障转移失败: {}", result.getErrorMessage());
        }
        
        // 恢复Broker健康状态
        logger.info("恢复 order-broker-1 健康状态...");
        routingEngine.updateBrokerHealth("order-broker-1", true);
    }
    
    /**
     * 显示统计信息
     */
    private static void showStatistics(SmartRoutingEngine routingEngine) {
        logger.info("\n--- 路由统计信息 ---");
        
        SmartRoutingEngine.RoutingMetrics metrics = routingEngine.getMetrics();
        logger.info("成功路由数量: {}", metrics.getSuccessCount());
        logger.info("失败路由数量: {}", metrics.getFailureCount());
        logger.info("成功率: {:.2f}%", metrics.getSuccessRate() * 100);
        logger.info("平均路由时间: {:.2f}ms", metrics.getAvgRouteTime());
        logger.info("最大路由时间: {}ms", metrics.getMaxRouteTime());
        logger.info("最小路由时间: {}ms", metrics.getMinRouteTime());
        
        logger.info("\n路由引擎状态:");
        logger.info(routingEngine.getEngineStatus());
    }
}
