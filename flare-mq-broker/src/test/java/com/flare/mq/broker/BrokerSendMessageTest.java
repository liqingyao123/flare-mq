package com.flare.mq.broker;

import com.flare.mq.broker.ack.AckManager;
import com.flare.mq.broker.offset.ConsumerOffsetManager;
import com.flare.mq.broker.queue.QueueManager;
import com.flare.mq.broker.topic.TopicConfig;
import com.flare.mq.broker.topic.TopicManager;
import com.flare.mq.broker.topic.TopicPermission;
import com.flare.mq.common.util.JsonUtils;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.store.DefaultMessageStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Broker 发送消息测试 — 覆盖同步刷盘和结构化响应
 */
@DisplayName("Broker 发送消息测试")
class BrokerSendMessageTest {

    private Path tempDir;
    private DefaultMessageStore messageStore;
    private TopicManager topicManager;
    private QueueManager queueManager;
    private AckManager ackManager;
    private ConsumerOffsetManager offsetManager;
    private BrokerRequestHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("broker-test-" + UUID.randomUUID().toString().substring(0, 8));
        String storePath = tempDir.resolve("store").toString();
        String offsetPath = tempDir.resolve("offset").toString();

        messageStore = new DefaultMessageStore(storePath);
        messageStore.start();

        topicManager = new TopicManager();
        queueManager = new QueueManager();
        ackManager = new AckManager();
        offsetManager = new ConsumerOffsetManager(offsetPath);

        handler = new BrokerRequestHandler(
                topicManager, queueManager, messageStore, offsetManager, ackManager);
    }

    @AfterEach
    void tearDown() {
        if (messageStore != null) {
            messageStore.shutdown();
        }
        if (ackManager != null) {
            ackManager.shutdown();
        }
        // force GC to release mmap file handles on Windows before delete
        System.gc();
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    private static void deleteRecursively(File file) {
        if (!file.exists()) return;
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    @Test
    @DisplayName("发送消息 — 异步刷盘（默认），返回结构化响应")
    void testSendMessage_AsyncFlush_ReturnsStructuredResponse() {
        topicManager.createTopic("async-topic", 4, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig("async-topic");
        queueManager.createQueuesForTopic(topicConfig);

        String sendJson = "{\"messageId\":\"msg-001\",\"topic\":\"async-topic\"," +
                "\"tags\":\"TagA\",\"key\":\"key-1\",\"body\":\"Hello Async\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST, sendJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertNotNull(response.getBody());

        String responseJson = new String(response.getBody(), StandardCharsets.UTF_8);
        BrokerRequestHandler.SendResponse parsed = JsonUtils.fromJson(
                responseJson, BrokerRequestHandler.SendResponse.class);

        assertNotNull(parsed);
        assertEquals("msg-001", parsed.messageId);
        assertEquals("async-topic", parsed.topic);
        assertTrue(parsed.queueId >= 0, "queueId should be >= 0, was: " + parsed.queueId);
        assertTrue(parsed.offset >= 0, "offset should be >= 0, was: " + parsed.offset);
    }

    @Test
    @DisplayName("发送消息 — 同步刷盘，返回结构化响应")
    void testSendMessage_SyncFlush_ReturnsStructuredResponse() {
        topicManager.createTopic("sync-topic", 4, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig("sync-topic");
        topicConfig.setSyncFlush(true);
        queueManager.createQueuesForTopic(topicConfig);

        String sendJson = "{\"messageId\":\"msg-002\",\"topic\":\"sync-topic\"," +
                "\"tags\":\"TagB\",\"key\":\"key-2\",\"body\":\"Hello Sync\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST, sendJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertNotNull(response.getBody());

        String responseJson = new String(response.getBody(), StandardCharsets.UTF_8);
        BrokerRequestHandler.SendResponse parsed = JsonUtils.fromJson(
                responseJson, BrokerRequestHandler.SendResponse.class);

        assertNotNull(parsed);
        assertEquals("msg-002", parsed.messageId);
        assertEquals("sync-topic", parsed.topic);
        assertTrue(parsed.offset >= 0, "sync flush offset should be valid, was: " + parsed.offset);
    }

    @Test
    @DisplayName("发送消息 — Topic 不存在时自动创建")
    void testSendMessage_AutoCreateTopic() {
        String sendJson = "{\"messageId\":\"msg-003\",\"topic\":\"auto-topic\"," +
                "\"tags\":\"TagC\",\"key\":\"key-3\",\"body\":\"Auto Created\"}";
        ProtocolMessage request = new ProtocolMessage(
                MessageType.SEND_MESSAGE_REQUEST, sendJson.getBytes(StandardCharsets.UTF_8));

        ProtocolMessage response = handler.handleRequest(null, request);

        assertEquals(ResponseCode.SUCCESS, response.getStatus());
        assertTrue(topicManager.topicExists("auto-topic"), "Topic should be auto-created");
    }

    @Test
    @DisplayName("发送消息 — 空 body 返回错误")
    void testSendMessage_EmptyBody_ReturnsError() {
        ProtocolMessage request = ProtocolMessage.createSuccessResponse(
                MessageType.SEND_MESSAGE_REQUEST, 1, null);

        ProtocolMessage response = handler.handleRequest(null, request);
        assertEquals(ResponseCode.BAD_REQUEST, response.getStatus());
    }

    @Test
    @DisplayName("发送多条消息 — 连续发送验证 offset 递增")
    void testSendMessage_MultipleMessages_OffsetIncrements() {
        topicManager.createTopic("multi-topic", 4, TopicPermission.READ_WRITE);
        TopicConfig topicConfig = topicManager.getTopicConfig("multi-topic");
        queueManager.createQueuesForTopic(topicConfig);

        long lastOffset = -1;

        for (int i = 0; i < 5; i++) {
            String sendJson = String.format(
                    "{\"messageId\":\"multi-msg-%d\",\"topic\":\"multi-topic\"," +
                            "\"tags\":\"Tag\",\"key\":\"key-%d\",\"body\":\"Message %d\"}",
                    i, i, i);
            ProtocolMessage request = new ProtocolMessage(
                    MessageType.SEND_MESSAGE_REQUEST, sendJson.getBytes(StandardCharsets.UTF_8));

            ProtocolMessage response = handler.handleRequest(null, request);
            assertEquals(ResponseCode.SUCCESS, response.getStatus(),
                    "Message " + i + " should succeed");

            String responseJson = new String(response.getBody(), StandardCharsets.UTF_8);
            BrokerRequestHandler.SendResponse parsed = JsonUtils.fromJson(
                    responseJson, BrokerRequestHandler.SendResponse.class);

            assertNotNull(parsed);
            assertTrue(parsed.offset > lastOffset,
                    "Offset should increment: msg=" + i + " offset=" + parsed.offset +
                            " lastOffset=" + lastOffset);
            lastOffset = parsed.offset;
        }
    }

    @Test
    @DisplayName("同步刷盘 vs 异步刷盘 — syncFlush 不影响响应结构")
    void testSendMessage_SyncVsAsyncFlush_ResponseIdentical() {
        // async topic
        topicManager.createTopic("cmp-async", 2, TopicPermission.READ_WRITE);
        queueManager.createQueuesForTopic(topicManager.getTopicConfig("cmp-async"));

        // sync topic
        topicManager.createTopic("cmp-sync", 2, TopicPermission.READ_WRITE);
        TopicConfig syncCfg = topicManager.getTopicConfig("cmp-sync");
        syncCfg.setSyncFlush(true);
        queueManager.createQueuesForTopic(syncCfg);

        String json = "{\"messageId\":\"cmp-msg\",\"topic\":\"%s\"," +
                "\"tags\":\"Tag\",\"key\":\"k\",\"body\":\"Compare\"}";

        ProtocolMessage reqAsync = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST,
                String.format(json, "cmp-async").getBytes(StandardCharsets.UTF_8));
        ProtocolMessage reqSync = new ProtocolMessage(MessageType.SEND_MESSAGE_REQUEST,
                String.format(json, "cmp-sync").getBytes(StandardCharsets.UTF_8));

        ProtocolMessage respAsync = handler.handleRequest(null, reqAsync);
        ProtocolMessage respSync = handler.handleRequest(null, reqSync);

        assertEquals(ResponseCode.SUCCESS, respAsync.getStatus(), "async should succeed");
        assertEquals(ResponseCode.SUCCESS, respSync.getStatus(), "sync should succeed");

        BrokerRequestHandler.SendResponse parsedAsync = JsonUtils.fromJson(
                new String(respAsync.getBody(), StandardCharsets.UTF_8),
                BrokerRequestHandler.SendResponse.class);
        BrokerRequestHandler.SendResponse parsedSync = JsonUtils.fromJson(
                new String(respSync.getBody(), StandardCharsets.UTF_8),
                BrokerRequestHandler.SendResponse.class);

        // Both should have the same response structure
        assertNotNull(parsedAsync);
        assertNotNull(parsedSync);
        assertEquals(parsedAsync.messageId, parsedSync.messageId);
        assertEquals(parsedAsync.topic, "cmp-async");
        assertEquals(parsedSync.topic, "cmp-sync");
    }
}
