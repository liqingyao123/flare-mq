package com.flare.mq.store;

/**
 * 智能存储特性测试
 * 
 * @author FlareMQ Team
 */
public class IntelligentStorageTest {
    
    public static void main(String[] args) {
        System.out.println("开始智能存储特性测试");
        
        try {
            // 测试消息模式分析
            testMessagePattern();
            
            // 测试消息热度分析
            testMessageHeatAnalyzer();
            
            // 测试自适应存储策略
            testAdaptiveStorageStrategy();
            
            // 测试智能存储管理器
            testIntelligentStorageManager();
            
            System.out.println("所有智能存储特性测试通过！");
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testMessagePattern() {
        System.out.println("=== 测试消息模式分析 ===");
        
        // 测试JSON数据
        String jsonData = "{\"name\":\"test\",\"value\":123,\"array\":[1,2,3]}";
        MessagePattern jsonPattern = new MessagePattern(jsonData.getBytes());
        System.out.println("JSON数据分析: " + jsonPattern);
        System.out.println("是否为JSON: " + jsonPattern.isJsonData());
        System.out.println("压缩建议: " + jsonPattern.getCompressionRecommendation());
        
        // 测试文本数据
        String textData = "Hello World! This is a test message with repeated content. " +
                         "Hello World! This is a test message with repeated content.";
        MessagePattern textPattern = new MessagePattern(textData.getBytes());
        System.out.println("\n文本数据分析: " + textPattern);
        System.out.println("是否为文本重度: " + textPattern.isTextHeavy());
        System.out.println("压缩建议: " + textPattern.getCompressionRecommendation());
        
        // 测试二进制数据
        byte[] binaryData = new byte[1024];
        for (int i = 0; i < binaryData.length; i++) {
            binaryData[i] = (byte) (i % 256);
        }
        MessagePattern binaryPattern = new MessagePattern(binaryData);
        System.out.println("\n二进制数据分析: " + binaryPattern);
        System.out.println("是否为二进制: " + binaryPattern.isBinaryData());
        System.out.println("压缩建议: " + binaryPattern.getCompressionRecommendation());
        
        System.out.println("消息模式分析测试通过");
    }
    
    private static void testMessageHeatAnalyzer() {
        System.out.println("\n=== 测试消息热度分析 ===");
        
        MessageHeatAnalyzer analyzer = new MessageHeatAnalyzer();
        analyzer.start();
        
        try {
            // 模拟不同的访问模式
            String topic1 = "hot-topic";
            String topic2 = "warm-topic";
            String topic3 = "cold-topic";
            
            // 热点Topic：频繁访问
            for (int i = 0; i < 100; i++) {
                analyzer.recordAccess(topic1, 0);
                analyzer.recordMessage(topic1, 0, 1024);
            }
            analyzer.setBusinessPriority(topic1, 0, 9); // 高优先级
            
            // 温Topic：中等访问
            for (int i = 0; i < 20; i++) {
                analyzer.recordAccess(topic2, 0);
                analyzer.recordMessage(topic2, 0, 512);
            }
            analyzer.setBusinessPriority(topic2, 0, 5); // 中等优先级
            
            // 冷Topic：很少访问
            for (int i = 0; i < 2; i++) {
                analyzer.recordAccess(topic3, 0);
                analyzer.recordMessage(topic3, 0, 256);
            }
            analyzer.setBusinessPriority(topic3, 0, 2); // 低优先级
            
            // 分析热度
            System.out.println("热点Topic热度: " + analyzer.calculateHeatScore(topic1, 0) + 
                             " 等级: " + analyzer.analyzeHeat(topic1, 0));
            System.out.println("温Topic热度: " + analyzer.calculateHeatScore(topic2, 0) + 
                             " 等级: " + analyzer.analyzeHeat(topic2, 0));
            System.out.println("冷Topic热度: " + analyzer.calculateHeatScore(topic3, 0) + 
                             " 等级: " + analyzer.analyzeHeat(topic3, 0));
            
            // 打印指标详情
            MessageMetrics metrics1 = analyzer.getMetrics(topic1, 0);
            MessageMetrics metrics2 = analyzer.getMetrics(topic2, 0);
            MessageMetrics metrics3 = analyzer.getMetrics(topic3, 0);
            
            System.out.println("\n热点Topic指标: " + metrics1);
            System.out.println("温Topic指标: " + metrics2);
            System.out.println("冷Topic指标: " + metrics3);
            
        } finally {
            analyzer.shutdown();
        }
        
        System.out.println("消息热度分析测试通过");
    }
    
    private static void testAdaptiveStorageStrategy() {
        System.out.println("\n=== 测试自适应存储策略 ===");
        
        MessageHeatAnalyzer analyzer = new MessageHeatAnalyzer();
        analyzer.start();
        
        try {
            DefaultAdaptiveStorageStrategy strategy = new DefaultAdaptiveStorageStrategy(analyzer);
            
            // 创建不同热度的指标
            MessageMetrics hotMetrics = new MessageMetrics("hot-topic", 0);
            hotMetrics.setBusinessPriority(9);
            for (int i = 0; i < 100; i++) {
                hotMetrics.updateAccess();
                hotMetrics.updateMessageStats(1024);
            }
            
            MessageMetrics warmMetrics = new MessageMetrics("warm-topic", 0);
            warmMetrics.setBusinessPriority(5);
            for (int i = 0; i < 20; i++) {
                warmMetrics.updateAccess();
                warmMetrics.updateMessageStats(512);
            }
            
            MessageMetrics coldMetrics = new MessageMetrics("cold-topic", 0);
            coldMetrics.setBusinessPriority(2);
            for (int i = 0; i < 2; i++) {
                coldMetrics.updateAccess();
                coldMetrics.updateMessageStats(256);
            }
            
            // 测试存储类型选择
            StorageType hotStorage = strategy.selectStorageType(hotMetrics);
            StorageType warmStorage = strategy.selectStorageType(warmMetrics);
            StorageType coldStorage = strategy.selectStorageType(coldMetrics);
            
            System.out.println("热点消息存储类型: " + hotStorage);
            System.out.println("温消息存储类型: " + warmStorage);
            System.out.println("冷消息存储类型: " + coldStorage);
            
            // 测试压缩类型选择
            MessagePattern jsonPattern = new MessagePattern("{\"test\":\"data\"}".getBytes());
            MessagePattern textPattern = new MessagePattern("Hello World Hello World".getBytes());
            MessagePattern binaryPattern = new MessagePattern(new byte[1024]);
            
            CompressionType jsonCompression = strategy.selectCompression(jsonPattern);
            CompressionType textCompression = strategy.selectCompression(textPattern);
            CompressionType binaryCompression = strategy.selectCompression(binaryPattern);
            
            System.out.println("JSON数据压缩类型: " + jsonCompression);
            System.out.println("文本数据压缩类型: " + textCompression);
            System.out.println("二进制数据压缩类型: " + binaryCompression);
            
            // 测试策略调整
            PerformanceMetrics performance = new PerformanceMetrics(15.0, 8.0, 0.9, 0.6, 2.5);
            strategy.adjustStrategy(performance);
            
            System.out.println("策略调整后统计: " + strategy.getStats());
            
        } finally {
            analyzer.shutdown();
        }
        
        System.out.println("自适应存储策略测试通过");
    }
    
    private static void testIntelligentStorageManager() {
        System.out.println("\n=== 测试智能存储管理器 ===");
        
        IntelligentStorageManager manager = new IntelligentStorageManager();
        manager.start();
        
        try {
            // 测试消息分析
            Message jsonMessage = new Message("json-topic", "json-tag", "key1", 
                                            "{\"name\":\"test\",\"value\":123}".getBytes());
            jsonMessage.setQueueId(0);
            
            Message textMessage = new Message("text-topic", "text-tag", "key2", 
                                            "Hello World! Repeated content. Hello World!".getBytes());
            textMessage.setQueueId(0);
            
            Message binaryMessage = new Message("binary-topic", "binary-tag", "key3", 
                                              new byte[2048]);
            binaryMessage.setQueueId(0);
            
            // 分析存储决策
            StorageDecision jsonDecision = manager.analyzeMessage(jsonMessage);
            StorageDecision textDecision = manager.analyzeMessage(textMessage);
            StorageDecision binaryDecision = manager.analyzeMessage(binaryMessage);
            
            System.out.println("JSON消息存储决策: " + jsonDecision);
            System.out.println("文本消息存储决策: " + textDecision);
            System.out.println("二进制消息存储决策: " + binaryDecision);
            
            // 模拟访问模式
            for (int i = 0; i < 50; i++) {
                manager.recordMessageAccess("json-topic", 0);
            }
            for (int i = 0; i < 10; i++) {
                manager.recordMessageAccess("text-topic", 0);
            }
            
            // 设置业务优先级
            manager.setBusinessPriority("json-topic", 0, 8);
            manager.setBusinessPriority("text-topic", 0, 5);
            manager.setBusinessPriority("binary-topic", 0, 3);
            
            // 等待一段时间让统计生效
            Thread.sleep(1000);
            
            // 重新分析
            StorageDecision newJsonDecision = manager.analyzeMessage(jsonMessage);
            StorageDecision newTextDecision = manager.analyzeMessage(textMessage);
            StorageDecision newBinaryDecision = manager.analyzeMessage(binaryMessage);
            
            System.out.println("\n访问后JSON消息存储决策: " + newJsonDecision);
            System.out.println("访问后文本消息存储决策: " + newTextDecision);
            System.out.println("访问后二进制消息存储决策: " + newBinaryDecision);
            
            // 获取统计信息
            IntelligentStorageStats stats = manager.getStats();
            System.out.println("\n智能存储统计: " + stats);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            manager.shutdown();
        }
        
        System.out.println("智能存储管理器测试通过");
    }
}
