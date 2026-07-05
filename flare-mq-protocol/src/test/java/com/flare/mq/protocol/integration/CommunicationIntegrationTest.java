package com.flare.mq.protocol.integration;

import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.protocol.client.NettyClient;
import com.flare.mq.protocol.client.ResponseCallback;
import com.flare.mq.protocol.server.NettyServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 通信框架集成测试
 * 
 * @author FlareMQ Team
 */
class CommunicationIntegrationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(CommunicationIntegrationTest.class);
    
    private static final String HOST = "localhost";
    private static final int PORT = 18888;
    
    private NettyServer server;
    private NettyClient client;
    
    @BeforeEach
    void setUp() throws Exception {
        // 启动服务器
        server = new NettyServer(PORT);
        server.start();
        
        // 等待服务器启动
        Thread.sleep(1000);
        
        // 创建客户端
        client = new NettyClient(HOST, PORT);
        client.connect();
        
        // 等待连接建立
        Thread.sleep(500);
    }
    
    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
    }
    
    @Test
    void testBasicConnection() {
        assertTrue(server.isStarted());
        assertTrue(client.isConnected());
    }
    
    @Test
    void testHeartbeat() throws Exception {
        ProtocolMessage heartbeatRequest = ProtocolMessage.createHeartbeatRequest();
        ProtocolMessage response = client.sendSync(heartbeatRequest, 5000);
        
        assertNotNull(response);
        assertEquals(MessageType.HEARTBEAT_RESPONSE, response.getType());
        assertEquals(heartbeatRequest.getRequestId(), response.getRequestId());
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
    }
    
    @Test
    void testSendMessage() throws Exception {
        byte[] body = "Hello FlareMQ".getBytes();
        ProtocolMessage request = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, body);
        
        ProtocolMessage response = client.sendSync(request, 5000);
        
        assertNotNull(response);
        assertEquals(MessageType.SEND_MESSAGE_RESPONSE, response.getType());
        assertEquals(request.getRequestId(), response.getRequestId());
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertNotNull(response.getBody());
        String responseBody = new String(response.getBody(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(responseBody.contains("successfully"));
    }
    
    @Test
    void testAsyncSendMessage() throws Exception {
        byte[] body = "Async Hello".getBytes();
        ProtocolMessage request = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, body);
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ProtocolMessage> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        
        client.sendAsync(request, new ResponseCallback() {
            @Override
            public void onSuccess(ProtocolMessage response) {
                responseRef.set(response);
                latch.countDown();
            }
            
            @Override
            public void onFailure(Throwable cause) {
                errorRef.set(cause);
                latch.countDown();
            }
        });
        
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(errorRef.get());
        
        ProtocolMessage response = responseRef.get();
        assertNotNull(response);
        assertEquals(MessageType.SEND_MESSAGE_RESPONSE, response.getType());
        assertEquals(request.getRequestId(), response.getRequestId());
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
    }
    
    @Test
    void testMultipleRequests() throws Exception {
        int requestCount = 10;
        CountDownLatch latch = new CountDownLatch(requestCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        for (int i = 0; i < requestCount; i++) {
            byte[] body = ("Request " + i).getBytes();
            ProtocolMessage request = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, body);
            
            client.sendAsync(request, new ResponseCallback() {
                @Override
                public void onSuccess(ProtocolMessage response) {
                    successCount.incrementAndGet();
                    latch.countDown();
                }
                
                @Override
                public void onFailure(Throwable cause) {
                    errorCount.incrementAndGet();
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(requestCount, successCount.get());
        assertEquals(0, errorCount.get());
    }
    
    @Test
    void testDifferentMessageTypes() throws Exception {
        // 测试不同类型的消息
        MessageType[] types = {
                MessageType.SEND_MESSAGE_REQUEST,
                MessageType.PULL_MESSAGE_REQUEST,
                MessageType.CREATE_TOPIC_REQUEST,
                MessageType.QUERY_TOPIC_REQUEST
        };
        
        for (MessageType type : types) {
            byte[] body = ("Test " + type.name()).getBytes();
            ProtocolMessage request = new ProtocolMessage(type, body);
            
            ProtocolMessage response = client.sendSync(request, 5000);
            
            assertNotNull(response, "Response should not be null for " + type);
            assertEquals(ResponseCode.SUCCESS, response.getStatus(), "Response should be success for " + type);
        }
    }
    
    @Test
    void testLargeMessage() throws Exception {
        // 测试大消息（1MB）
        byte[] largeBody = new byte[1024 * 1024];
        for (int i = 0; i < largeBody.length; i++) {
            largeBody[i] = (byte) (i % 256);
        }
        
        ProtocolMessage request = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST, largeBody);
        ProtocolMessage response = client.sendSync(request, 10000);
        
        assertNotNull(response);
        assertEquals(MessageType.SEND_MESSAGE_RESPONSE, response.getType());
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
    }
}
