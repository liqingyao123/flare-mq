package com.ruyuan.mq.protocol;

import com.ruyuan.mq.protocol.client.NettyClient;
import com.ruyuan.mq.protocol.server.NettyServer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单测试
 * 
 * @author RuYuan MQ Team
 */
class SimpleTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleTest.class);
    
    @Test
    void testBasicFunctionality() throws Exception {
        logger.info("开始基本功能测试");
        
        // 测试协议消息创建
        ProtocolMessage heartbeat = ProtocolMessage.createHeartbeatRequest();
        logger.info("心跳消息创建成功: {}", heartbeat);
        
        // 测试服务器创建
        NettyServer server = new NettyServer(18888);
        logger.info("服务器创建成功");
        
        try {
            // 启动服务器
            server.start();
            logger.info("服务器启动成功");
            
            // 等待服务器启动
            Thread.sleep(1000);
            
            // 创建客户端
            NettyClient client = new NettyClient("localhost", 18888);
            logger.info("客户端创建成功");
            
            try {
                // 连接服务器
                client.connect();
                logger.info("客户端连接成功");
                
                // 等待连接建立
                Thread.sleep(500);
                
                // 发送心跳
                ProtocolMessage response = client.sendSync(heartbeat, 5000);
                logger.info("收到响应: {}", response);
                
                if (response != null && response.getType() == MessageType.HEARTBEAT_RESPONSE) {
                    logger.info("心跳测试成功！");
                } else {
                    logger.error("心跳测试失败！");
                }
                
            } finally {
                client.shutdown();
                logger.info("客户端关闭");
            }
            
        } finally {
            server.shutdown();
            logger.info("服务器关闭");
        }
        
        logger.info("基本功能测试完成");
    }
}
