package com.ruyuan.mq.store;

import java.io.File;

/**
 * 调试测试 - 详细日志版本
 * 
 * @author RuYuan MQ Team
 */
public class DebugTest {
    
    public static void main(String[] args) {
        System.out.println("=== 开始调试测试 ===");
        
        try {
            // 测试最基本的功能
            testBasicFunction();
            
            System.out.println("\n✅ 调试测试完成！");
            
        } catch (Exception e) {
            System.err.println("❌ 调试测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testBasicFunction() {
        System.out.println("\n--- 基础功能调试测试 ---");
        
        String testStorePath = System.getProperty("java.io.tmpdir") + File.separator + "ruyuan-mq-debug-test-" + System.currentTimeMillis();
        System.out.println("[DEBUG] 测试存储路径: " + testStorePath);
        
        DefaultMessageStore messageStore = null;
        
        try {
            System.out.println("[DEBUG] 创建DefaultMessageStore...");
            messageStore = new DefaultMessageStore(testStorePath);
            System.out.println("[DEBUG] DefaultMessageStore创建成功");
            
            System.out.println("[DEBUG] 启动MessageStore...");
            messageStore.start();
            System.out.println("[DEBUG] MessageStore启动成功");
            
            // 创建一个简单的测试消息
            System.out.println("[DEBUG] 创建测试消息...");
            Message message = new Message("debug-topic", "debug-tag", "debug-key", "Hello Debug!".getBytes());
            message.setQueueId(0);
            System.out.println("[DEBUG] 测试消息创建成功: " + message);
            
            // 尝试存储消息
            System.out.println("[DEBUG] 开始存储消息...");
            PutMessageResult result = messageStore.putMessage(message);
            System.out.println("[DEBUG] 消息存储结果: " + result);
            
            if (result.isOk()) {
                System.out.println("✓ 消息存储成功");
                
                // 尝试读取消息
                System.out.println("[DEBUG] 开始读取消息...");
                GetMessageResult getResult = messageStore.getMessage("debug-topic", 0, 0, 1);
                System.out.println("[DEBUG] 消息读取结果: " + getResult);
                
                if (getResult.isFound()) {
                    System.out.println("✓ 消息读取成功");
                    Message retrievedMessage = getResult.getMessageList().get(0);
                    System.out.println("[DEBUG] 读取到的消息: " + retrievedMessage);
                    
                    // 验证消息内容
                    if (message.getTopic().equals(retrievedMessage.getTopic()) &&
                        message.getTags().equals(retrievedMessage.getTags()) &&
                        message.getKeys().equals(retrievedMessage.getKeys())) {
                        System.out.println("✓ 消息内容验证成功");
                    } else {
                        System.out.println("❌ 消息内容验证失败");
                    }
                } else {
                    System.out.println("❌ 消息读取失败: " + getResult.getStatus());
                }
            } else {
                System.out.println("❌ 消息存储失败: " + result.getPutMessageStatus());
            }
            
        } catch (Exception e) {
            System.err.println("[ERROR] 测试过程中发生异常: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            if (messageStore != null) {
                System.out.println("[DEBUG] 关闭MessageStore...");
                messageStore.shutdown();
                System.out.println("[DEBUG] MessageStore关闭完成");
            }
        }
        
        System.out.println("基础功能调试测试完成");
    }
}
