package com.ruyuan.mq.protocol.integration;

import com.ruyuan.mq.protocol.server.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试服务器
 * 
 * @author RuYuan MQ Team
 */
public class TestServer {
    
    private static final Logger logger = LoggerFactory.getLogger(TestServer.class);
    
    public static void main(String[] args) {
        int port = 8888;
        
        NettyServer server = new NettyServer(port);
        
        try {
            // 启动服务器
            server.start();
            logger.info("测试服务器启动成功，端口: {}", port);
            
            // 等待服务器关闭
            server.waitForShutdown();
            
        } catch (Exception e) {
            logger.error("服务器运行异常", e);
        } finally {
            server.shutdown();
        }
    }
}
