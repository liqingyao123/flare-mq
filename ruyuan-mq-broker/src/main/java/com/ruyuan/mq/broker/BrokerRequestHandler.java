package com.ruyuan.mq.broker;

import com.ruyuan.mq.broker.queue.QueueConfig;
import com.ruyuan.mq.broker.queue.QueueManager;
import com.ruyuan.mq.broker.topic.TopicConfig;
import com.ruyuan.mq.broker.topic.TopicManager;
import com.ruyuan.mq.common.util.JsonUtils;
import com.ruyuan.mq.protocol.MessageType;
import com.ruyuan.mq.protocol.ProtocolMessage;
import com.ruyuan.mq.protocol.ResponseCode;
import com.ruyuan.mq.protocol.server.ServerRequestHandler;
import com.ruyuan.mq.store.DefaultMessageStore;
import com.ruyuan.mq.store.GetMessageResult;
import com.ruyuan.mq.store.Message;
import com.ruyuan.mq.store.PutMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Broker侧请求处理器：接入 Topic/Queue/Store 的真实实现
 */
public class BrokerRequestHandler implements ServerRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(BrokerRequestHandler.class);

    private final TopicManager topicManager;
    private final QueueManager queueManager;
    private final DefaultMessageStore messageStore;

    public BrokerRequestHandler(TopicManager topicManager,
                                QueueManager queueManager,
                                DefaultMessageStore messageStore) {
        this.topicManager = topicManager;
        this.queueManager = queueManager;
        this.messageStore = messageStore;
    }

    @Override
    public ProtocolMessage handleRequest(ChannelHandlerContext ctx, ProtocolMessage request) {
        try {
            if (request.getType() == MessageType.HEARTBEAT_REQUEST) {
                return ProtocolMessage.createHeartbeatResponse(request.getRequestId());
            }

            switch (request.getType()) {
                case SEND_MESSAGE_REQUEST:
                    return handleSendMessage(request);
                case PULL_MESSAGE_REQUEST:
                    return handlePullMessage(request);
                case ACK_MESSAGE_REQUEST:
                    return handleAckMessage(request);
                case CREATE_TOPIC_REQUEST:
                    return handleCreateTopic(request);
                case QUERY_TOPIC_REQUEST:
                    return handleQueryTopic(request);
                case DELETE_TOPIC_REQUEST:
                    return handleDeleteTopic(request);
                case LIST_TOPICS_REQUEST:
                    return handleListTopics(request);
                default:
                    logger.warn("Unknown request type: {}", request.getType());
                    return ProtocolMessage.createErrorResponse(
                            MessageType.RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
            }
        } catch (Exception e) {
            logger.error("handleRequest error, type=" + request.getType(), e);
            return ProtocolMessage.createErrorResponse(MessageType.RESPONSE, request.getRequestId(), ResponseCode.INTERNAL_ERROR);
        }
    }

    private ProtocolMessage handleSendMessage(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(MessageType.SEND_MESSAGE_RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        SendRequest sendReq = JsonUtils.fromJson(json, SendRequest.class);
        if (sendReq == null || sendReq.topic == null || sendReq.topic.trim().isEmpty()) {
            return ProtocolMessage.createErrorResponse(MessageType.SEND_MESSAGE_RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        // 确保Topic与Queue存在
        ensureTopicAndQueues(sendReq.topic);

        // 选择队列（简单策略：选择消息数最少的队列，否则0号队列）
        QueueConfig queue = queueManager.selectLeastLoadedQueue(sendReq.topic);
        int queueId = queue != null ? queue.getQueueId() : 0;

        // 构造存储层消息
        Message storeMsg = new Message(sendReq.topic, sendReq.tags, sendReq.key, sendReq.body != null ? sendReq.body.getBytes(StandardCharsets.UTF_8) : new byte[0]);
        storeMsg.setQueueId(queueId);

        PutMessageResult putRes = messageStore.putMessage(storeMsg);
        if (putRes != null && putRes.isOk()) {
            return ProtocolMessage.createSuccessResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    "OK".getBytes(StandardCharsets.UTF_8));
        } else {
            return ProtocolMessage.createErrorResponse(
                    MessageType.SEND_MESSAGE_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.INTERNAL_ERROR);
        }
    }

    private ProtocolMessage handlePullMessage(ProtocolMessage request) {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return ProtocolMessage.createErrorResponse(MessageType.PULL_MESSAGE_RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        String json = new String(body, StandardCharsets.UTF_8);
        PullRequest pullReq = JsonUtils.fromJson(json, PullRequest.class);
        if (pullReq == null || pullReq.topic == null) {
            return ProtocolMessage.createErrorResponse(MessageType.PULL_MESSAGE_RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        if (!topicManager.topicExists(pullReq.topic)) {
            // Topic不存在，返回无消息
            String payload = "{\"messages\":[],\"nextBeginOffset\":0,\"minOffset\":0,\"maxOffset\":0}";
            return ProtocolMessage.createSuccessResponse(MessageType.PULL_MESSAGE_RESPONSE, request.getRequestId(), payload.getBytes(StandardCharsets.UTF_8));
        }

        GetMessageResult res = messageStore.getMessage(pullReq.topic, pullReq.queueId, pullReq.offset, pullReq.maxNums);
        if (res == null) {
            String payload = "{\"messages\":[],\"nextBeginOffset\":0,\"minOffset\":0,\"maxOffset\":0}";
            return ProtocolMessage.createSuccessResponse(MessageType.PULL_MESSAGE_RESPONSE, request.getRequestId(), payload.getBytes(StandardCharsets.UTF_8));
        }

        // 构造响应（客户端目前未解析，先返回兼容结构）
        List<SimpleMessage> msgs = new ArrayList<>();
        if (res.getMessageList() != null) {
            for (Message m : res.getMessageList()) {
                SimpleMessage sm = new SimpleMessage();
                sm.topic = m.getTopic();
                sm.tags = m.getTags();
                sm.body = m.getBody() != null ? new String(m.getBody(), StandardCharsets.UTF_8) : "";
                msgs.add(sm);
            }
        }
        PullResponse pr = new PullResponse();
        pr.messages = msgs;
        pr.nextBeginOffset = res.getNextBeginOffset();
        pr.minOffset = res.getMinOffset();
        pr.maxOffset = res.getMaxOffset();

        String payload = JsonUtils.toJson(pr);
        return ProtocolMessage.createSuccessResponse(
                MessageType.PULL_MESSAGE_RESPONSE,
                request.getRequestId(),
                payload != null ? payload.getBytes(StandardCharsets.UTF_8) : null);
    }

    private ProtocolMessage handleAckMessage(ProtocolMessage request) {
        // 当前客户端不解析ACK结果，直接返回成功
        return ProtocolMessage.createSuccessResponse(
                MessageType.ACK_MESSAGE_RESPONSE,
                request.getRequestId(),
                "OK".getBytes(StandardCharsets.UTF_8));
    }

    private ProtocolMessage handleCreateTopic(ProtocolMessage request) {
        String json = request.getBody() != null ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        CreateTopicRequest req = json != null ? JsonUtils.fromJson(json, CreateTopicRequest.class) : null;
        String topic = req != null ? req.topic : null;
        if (topic == null || topic.trim().isEmpty()) {
            return ProtocolMessage.createErrorResponse(MessageType.CREATE_TOPIC_RESPONSE, request.getRequestId(), ResponseCode.BAD_REQUEST);
        }

        boolean created = ensureTopicAndQueues(topic);
        if (created) {
            return ProtocolMessage.createSuccessResponse(MessageType.CREATE_TOPIC_RESPONSE, request.getRequestId(), "OK".getBytes(StandardCharsets.UTF_8));
        }
        return ProtocolMessage.createErrorResponse(MessageType.CREATE_TOPIC_RESPONSE, request.getRequestId(), ResponseCode.INTERNAL_ERROR);
    }

    private ProtocolMessage handleQueryTopic(ProtocolMessage request) {
        String json = request.getBody() != null ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        QueryTopicRequest req = json != null ? JsonUtils.fromJson(json, QueryTopicRequest.class) : null;
        String topic = req != null ? req.topic : null;
        boolean exists = topic != null && topicManager.topicExists(topic);
        String payload = "{\"exists\":" + exists + "}";
        return ProtocolMessage.createSuccessResponse(MessageType.QUERY_TOPIC_RESPONSE, request.getRequestId(), payload.getBytes(StandardCharsets.UTF_8));
    }

    private ProtocolMessage handleDeleteTopic(ProtocolMessage request) {
        String json = request.getBody() != null ? new String(request.getBody(), StandardCharsets.UTF_8) : null;
        DeleteTopicRequest req = json != null ? JsonUtils.fromJson(json, DeleteTopicRequest.class) : null;
        String topic = req != null ? req.topic : null;

        if (topic == null || topic.trim().isEmpty()) {
            return ProtocolMessage.createErrorResponse(
                    MessageType.DELETE_TOPIC_RESPONSE,
                    request.getRequestId(),
                    ResponseCode.BAD_REQUEST);
        }

        // 清理队列（幂等：队列不存在也返回成功）
        queueManager.deleteQueuesForTopic(topic);

        // 删除 topic 配置
        boolean deleted = topicManager.deleteTopic(topic);

        String payload = "{\"success\":" + deleted + "}";
        return ProtocolMessage.createSuccessResponse(
                MessageType.DELETE_TOPIC_RESPONSE,
                request.getRequestId(),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    private ProtocolMessage handleListTopics(ProtocolMessage request) {
        Map<String, TopicConfig> allConfigs = topicManager.getAllTopicConfigs();

        List<Map<String, Object>> topicList = new ArrayList<>();
        for (Map.Entry<String, TopicConfig> entry : allConfigs.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("name", entry.getKey());
            item.put("queueCount", entry.getValue().getQueueCount());
            item.put("permission", entry.getValue().getPermission());
            topicList.add(item);
        }

        String payload = JsonUtils.toJson(topicList);
        return ProtocolMessage.createSuccessResponse(
                MessageType.LIST_TOPICS_RESPONSE,
                request.getRequestId(),
                payload != null ? payload.getBytes(StandardCharsets.UTF_8) : null);
    }

    private boolean ensureTopicAndQueues(String topic) {
        if (!topicManager.topicExists(topic)) {
            boolean ok = topicManager.createTopic(topic);
            if (!ok) return false;
            TopicConfig cfg = topicManager.getTopicConfig(topic);
            if (cfg == null) return false;
            return queueManager.createQueuesForTopic(cfg);
        }
        return true;
    }

    // ===== 简单请求/响应DTO =====
    static class SendRequest { public String messageId; public String topic; public String tags; public String key; public String body; }
    static class PullRequest { public String topic; public int queueId; public long offset; public int maxNums; public String consumerGroup; public String tags; }
    static class CreateTopicRequest { public String topic; }
    static class QueryTopicRequest { public String topic; }
    static class DeleteTopicRequest { public String topic; }
    static class SimpleMessage { public String topic; public String tags; public String body; }
    static class PullResponse { public List<SimpleMessage> messages; public long nextBeginOffset; public long minOffset; public long maxOffset; }
}

