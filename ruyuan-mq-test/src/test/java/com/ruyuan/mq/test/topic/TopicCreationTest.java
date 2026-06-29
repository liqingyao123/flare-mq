package com.ruyuan.mq.test.topic;

import com.ruyuan.mq.broker.BrokerRequestHandler;
import com.ruyuan.mq.broker.ack.AckManager;
import com.ruyuan.mq.broker.offset.ConsumerOffsetManager;
import com.ruyuan.mq.broker.queue.QueueManager;
import com.ruyuan.mq.broker.topic.TopicManager;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.store.DefaultMessageStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Topic 创建功能集成测试
 */
public class TopicCreationTest {

    private TopicManager topicManager;
    private QueueManager queueManager;
    private DefaultMessageStore messageStore;
    private BrokerRequestHandler handler;
    private ConsumerOffsetManager offsetManager;
    private AckManager ackManager;

    @BeforeEach
    public void setUp() {
        topicManager = new TopicManager();
        queueManager = new QueueManager();
        messageStore = new DefaultMessageStore(null);
        messageStore.start();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "tct-offset-" + System.nanoTime());
        tmpDir.mkdirs();
        offsetManager = new ConsumerOffsetManager(tmpDir.getAbsolutePath());
        ackManager = new AckManager();
        ackManager.start();
        handler = new BrokerRequestHandler(topicManager, queueManager, messageStore, offsetManager, ackManager);
    }

    @AfterEach
    public void tearDown() {
        if (ackManager != null) {
            ackManager.shutdown();
        }
        offsetManager.shutdown();
    }

    // ========== Topic 自动创建（Broker 端 ensureTopicAndQueues）==========

    @Test
    public void testAutoCreateTopicOnSend() {
        // 发送消息到一个不存在的 topic，应该自动创建
        String sendJson = "{\"topic\":\"test-autocreate\",\"tags\":\"test\",\"key\":\"k1\",\"body\":\"hello\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST,
                sendJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus(),
                "Send should succeed with auto-created topic");
        assertTrue(topicManager.topicExists("test-autocreate"),
                "Topic should be auto-created after first message");
    }

    @Test
    public void testAutoCreateTopicIsIdempotent() {
        String sendJson = "{\"topic\":\"test-idempotent\",\"tags\":\"t\",\"key\":\"k\",\"body\":\"msg\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST,
                sendJson.getBytes(StandardCharsets.UTF_8));

        // 发送两次，第二次应该也成功
        ProtocolMessage r1 = handler.handleRequest(null, request);
        ProtocolMessage r2 = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, r1.getStatus());
        assertEquals(ResponseCode.SUCCESS, r2.getStatus());
    }

    // ========== 手动创建 Topic（通过协议）==========

    @Test
    public void testManualCreateTopic() {
        String createJson = "{\"topic\":\"manual-topic\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.CREATE_TOPIC_REQUEST,
                createJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertTrue(topicManager.topicExists("manual-topic"));
    }

    @Test
    public void testCreateTopicWithEmptyName() {
        ProtocolMessage request = new ProtocolMessage(
                MessageType.CREATE_TOPIC_REQUEST,
                "{\"topic\":\"\"}".getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.BAD_REQUEST, response.getStatus());
    }

    // ========== 查询 Topic ==========

    @Test
    public void testQueryTopicExists() {
        topicManager.createTopic("query-test");

        String queryJson = "{\"topic\":\"query-test\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.QUERY_TOPIC_REQUEST,
                queryJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"exists\":true"));
    }

    @Test
    public void testQueryTopicNotExists() {
        String queryJson = "{\"topic\":\"nonexistent\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.QUERY_TOPIC_REQUEST,
                queryJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("\"exists\":false"));
    }

    // ========== 删除 Topic ==========

    @Test
    public void testDeleteTopic() {
        topicManager.createTopic("delete-me");

        String deleteJson = "{\"topic\":\"delete-me\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.DELETE_TOPIC_REQUEST,
                deleteJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertFalse(topicManager.topicExists("delete-me"));
    }

    @Test
    public void testDeleteTopicIdempotent() {
        String deleteJson = "{\"topic\":\"no-such-topic\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.DELETE_TOPIC_REQUEST,
                deleteJson.getBytes(StandardCharsets.UTF_8));

        // 删除不存在的 topic 也不报错
        ProtocolMessage response = handler.handleRequest(null, request);
        assertEquals(ResponseCode.SUCCESS, response.getStatus());
    }

    // ========== 列出 Topic ==========

    @Test
    public void testListTopics() {
        topicManager.createTopic("list-test-1");
        topicManager.createTopic("list-test-2");

        ProtocolMessage request = new ProtocolMessage(
                MessageType.LIST_TOPICS_REQUEST,
                "{}".getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(body.contains("list-test-1"));
        assertTrue(body.contains("list-test-2"));
    }
}
