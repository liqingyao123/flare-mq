package com.ruyuan.mq.protocol.zerocopy;

import java.nio.ByteBuffer;

/**
 * 基础测试
 */
public class BasicTest {
    
    public static void main(String[] args) {
        System.out.println("开始基础测试...");
        
        // 测试DirectBuffer基本功能
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        System.out.println("DirectBuffer创建成功: capacity=" + buffer.capacity() + ", isDirect=" + buffer.isDirect());
        
        // 测试MessageLocation
        MessageLocation location = new MessageLocation("test.dat", 100, 1024);
        System.out.println("MessageLocation创建成功: " + location);
        
        System.out.println("基础测试完成");
    }
}
