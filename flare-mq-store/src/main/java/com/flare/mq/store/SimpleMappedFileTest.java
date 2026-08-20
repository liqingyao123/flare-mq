package com.flare.mq.store;

import java.io.File;

/**
 * SimpleMappedFile独立测试
 * 
 * @author FlareMQ Team
 */
public class SimpleMappedFileTest {
    
    public static void main(String[] args) {
        System.out.println("=== SimpleMappedFile独立测试 ===");
        
        try {
            testSimpleMappedFile();
            System.out.println("\n✅ SimpleMappedFile测试成功！");
        } catch (Exception e) {
            System.err.println("❌ SimpleMappedFile测试失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void testSimpleMappedFile() throws Exception {
        String testPath = System.getProperty("java.io.tmpdir") + File.separator + "simple-mapped-file-test-" + System.currentTimeMillis();
        int fileSize = 64 * 1024; // 64KB
        long fileFromOffset = 0;
        
        System.out.println("[DEBUG] 测试路径: " + testPath);
        System.out.println("[DEBUG] 文件大小: " + fileSize);
        
        SimpleMappedFile simpleMappedFile = null;
        
        try {
            // 创建SimpleMappedFile
            System.out.println("[DEBUG] 创建SimpleMappedFile...");
            simpleMappedFile = new SimpleMappedFile(testPath, fileSize, fileFromOffset);
            System.out.println("[DEBUG] SimpleMappedFile创建成功");
            
            // 测试写入
            System.out.println("[DEBUG] 测试写入数据...");
            String testData = "Hello SimpleMappedFile!";
            byte[] data = testData.getBytes();
            
            long writeResult = simpleMappedFile.appendMessage(data);
            System.out.println("[DEBUG] 写入结果: " + writeResult);

            if (writeResult < 0) {
                throw new RuntimeException("写入失败");
            }
            
            // 测试读取
            System.out.println("[DEBUG] 测试读取数据...");
            byte[] readData = simpleMappedFile.readBytes(0, data.length);
            
            if (readData == null) {
                throw new RuntimeException("读取失败");
            }
            
            String readString = new String(readData);
            System.out.println("[DEBUG] 读取到的数据: " + readString);
            
            if (!testData.equals(readString)) {
                throw new RuntimeException("数据不匹配，期望: " + testData + ", 实际: " + readString);
            }
            
            // 测试多次写入
            System.out.println("[DEBUG] 测试多次写入...");
            for (int i = 0; i < 10; i++) {
                String msg = "Message " + i;
                long result = simpleMappedFile.appendMessage(msg.getBytes());
                if (result < 0) {
                    throw new RuntimeException("第" + i + "次写入失败");
                }
                System.out.println("[DEBUG] 第" + (i + 1) + "次写入成功");
            }
            
            // 测试刷盘
            System.out.println("[DEBUG] 测试刷盘...");
            int flushedPos = simpleMappedFile.flush(0);
            System.out.println("[DEBUG] 刷盘位置: " + flushedPos);
            
            // 测试状态
            System.out.println("[DEBUG] 测试状态信息...");
            System.out.println("  文件名: " + simpleMappedFile.getFileName());
            System.out.println("  文件大小: " + simpleMappedFile.getFileSize());
            System.out.println("  文件偏移量: " + simpleMappedFile.getFileFromOffset());
            System.out.println("  写入位置: " + simpleMappedFile.getWrotePosition());
            System.out.println("  提交位置: " + simpleMappedFile.getCommittedPosition());
            System.out.println("  刷盘位置: " + simpleMappedFile.getFlushedPosition());
            System.out.println("  是否可用: " + simpleMappedFile.isAvailable());
            System.out.println("  是否已满: " + simpleMappedFile.isFull());
            System.out.println("  剩余空间: " + simpleMappedFile.getRemainSpace());
            
            System.out.println("✓ SimpleMappedFile所有功能测试通过");
            
        } finally {
            if (simpleMappedFile != null) {
                System.out.println("[DEBUG] 清理SimpleMappedFile...");
                simpleMappedFile.cleanup();
                System.out.println("[DEBUG] SimpleMappedFile清理完成");
            }
        }
    }
}
