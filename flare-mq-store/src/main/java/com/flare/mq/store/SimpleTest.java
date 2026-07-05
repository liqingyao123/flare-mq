package com.flare.mq.store;

/**
 * 简单测试类
 * 
 * @author FlareMQ Team
 */
public class SimpleTest {
    
    public static void main(String[] args) {
        System.out.println("开始存储引擎基础测试");
        
        try {
            // 测试消息序列化
            testMessageSerialization();
            
            // 测试ConsumeQueue单元
            testConsumeQueueUnit();
            
            System.out.println("所有基础测试通过！");
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testMessageSerialization() {
        System.out.println("=== 测试消息序列化 ===");
        
        // 创建测试消息
        Message message = new Message("test-topic", "test-tags", "test-key", "Hello World".getBytes());
        message.setQueueId(1);
        message.setFlag(0);
        message.setBornTimestamp(System.currentTimeMillis());
        
        System.out.println("原始消息: " + message);
        
        // 序列化
        byte[] serialized = MessageSerializer.serialize(message);
        if (serialized == null) {
            throw new RuntimeException("消息序列化失败");
        }
        
        System.out.println("序列化成功，大小: " + serialized.length + " bytes");
        
        // 反序列化
        Message deserialized = MessageSerializer.deserialize(serialized);
        if (deserialized == null) {
            throw new RuntimeException("消息反序列化失败");
        }
        
        System.out.println("反序列化消息: " + deserialized);
        
        // 验证内容
        if (!message.getTopic().equals(deserialized.getTopic()) ||
            !message.getTags().equals(deserialized.getTags()) ||
            !message.getKeys().equals(deserialized.getKeys()) ||
            message.getQueueId() != deserialized.getQueueId()) {
            throw new RuntimeException("消息内容验证失败");
        }
        
        System.out.println("消息序列化测试通过");
    }
    
    private static void testConsumeQueueUnit() {
        System.out.println("=== 测试ConsumeQueue单元 ===");
        
        // 创建ConsumeQueue单元
        ConsumeQueueUnit unit = new ConsumeQueueUnit(12345L, 256, 98765L);
        System.out.println("原始单元: " + unit);
        
        // 序列化
        byte[] serialized = unit.serialize();
        if (serialized == null || serialized.length != StoreConstants.CONSUME_QUEUE_UNIT_SIZE) {
            throw new RuntimeException("ConsumeQueue单元序列化失败");
        }
        
        System.out.println("序列化成功，大小: " + serialized.length + " bytes");
        
        // 反序列化
        ConsumeQueueUnit deserialized = ConsumeQueueUnit.deserialize(serialized);
        if (deserialized == null) {
            throw new RuntimeException("ConsumeQueue单元反序列化失败");
        }
        
        System.out.println("反序列化单元: " + deserialized);
        
        // 验证内容
        if (unit.getCommitLogOffset() != deserialized.getCommitLogOffset() ||
            unit.getSize() != deserialized.getSize() ||
            unit.getTagsHashCode() != deserialized.getTagsHashCode()) {
            throw new RuntimeException("ConsumeQueue单元内容验证失败");
        }
        
        System.out.println("ConsumeQueue单元测试通过");
    }
}
