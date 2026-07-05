package com.flare.mq.client.consumer;

import com.flare.mq.protocol.client.NettyClient;
import com.flare.mq.protocol.ProtocolMessage;
import com.flare.mq.protocol.MessageType;
import com.flare.mq.protocol.ResponseCode;
import com.flare.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 队列分配管理器 — Consumer 注册、心跳、Rebalance 状态机
 */
public class QueueAllocationManager {

    private static final Logger logger = LoggerFactory.getLogger(QueueAllocationManager.class);

    // ===== Rebalance 状态机状态 =====
    private enum State { IDLE, REBALANCE_WAIT, REBALANCE_IN_PROGRESS }

    private final String nameServerHost;
    private final int nameServerPort;
    private final String consumerGroup;
    private final String consumerId;
    private final List<String> topics;

    private NettyClient nameServerClient;
    private final ScheduledExecutorService scheduler;

    // 当前状态
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private volatile ScheduledFuture<?> rebalanceTimer;

    // 本地缓存：topic → 当前分配的 queueId 列表
    private final ConcurrentHashMap<String, List<Integer>> allocatedQueues;

    // Rebalance 回调（由 ConsumerImpl 设置）
    private volatile RebalanceListener rebalanceListener;

    public interface RebalanceListener {
        void onRebalance(String topic, List<Integer> oldQueues, List<Integer> newQueues);
    }

    public QueueAllocationManager(String nameServerHost, int nameServerPort,
                                   String consumerGroup, String consumerId, List<String> topics) {
        this.nameServerHost = nameServerHost;
        this.nameServerPort = nameServerPort;
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
        this.topics = new ArrayList<>(topics);
        this.allocatedQueues = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "QueueAllocator-" + consumerId);
            t.setDaemon(false);
            return t;
        });
    }

    public void setRebalanceListener(RebalanceListener listener) {
        this.rebalanceListener = listener;
    }

    /**
     * 初始化：连接 NameServer → 随机 sleep → 注册 → 计算初始分配
     */
    public void initialize() throws Exception {
        nameServerClient = new NettyClient(nameServerHost, nameServerPort);
        nameServerClient.connect();

        // 随机 sleep 0~3s 错开多 consumer 同时启动
        int delay = new Random().nextInt(3000);
        logger.info("Random startup delay: {}ms", delay);
        Thread.sleep(delay);

        // 注册到 NameServer
        List<String> consumerIds = registerToNameServer();
        logger.info("Registered, group '{}' has {} consumers: {}", consumerGroup, consumerIds.size(), consumerIds);

        // 获取路由信息，计算初始分配
        for (String topic : topics) {
            int queueCount = fetchQueueCount(topic);
            if (queueCount > 0) {
                List<Integer> queues = calculateMyAllocation(topic, queueCount, consumerIds);
                allocatedQueues.put(topic, queues);
                logger.info("Initial allocation for topic '{}': queues={}", topic, queues);
            }
        }

        // 启动心跳（30s 间隔）
        scheduler.scheduleWithFixedDelay(this::sendHeartbeat, 30, 30, TimeUnit.SECONDS);

        // 启动 rebalance 检查（30s 间隔）
        scheduler.scheduleWithFixedDelay(this::checkRebalance, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * 获取某 topic 当前分配的 queueId 列表
     */
    public List<Integer> getAllocatedQueueIds(String topic) {
        List<Integer> queues = allocatedQueues.get(topic);
        return queues != null ? new ArrayList<>(queues) : Collections.emptyList();
    }

    // ===== Rebalance 状态机 =====

    private void checkRebalance() {
        try {
            // 拉取最新 consumer 列表
            List<String> latestIds = fetchConsumerIds();
            if (latestIds.isEmpty()) return;

            // 与本地缓存对比（用 allocatedQueues 的 consumer 数推断）
            for (String topic : topics) {
                int queueCount = fetchQueueCount(topic);
                if (queueCount <= 0) continue;

                List<Integer> currentAllocation = allocatedQueues.get(topic);
                List<Integer> newAllocation = calculateMyAllocation(topic, queueCount, latestIds);

                if (!Objects.equals(currentAllocation, newAllocation)) {
                    logger.info("Rebalance needed for topic '{}': current={}, new={}, group={}",
                            topic, currentAllocation, newAllocation, latestIds);
                    triggerRebalance(topic, newAllocation);
                }
            }
        } catch (Exception e) {
            logger.error("Error in rebalance check", e);
        }
    }

    private void triggerRebalance(String topic, List<Integer> newQueues) {
        State currentState = state.get();
        if (currentState == State.REBALANCE_WAIT) {
            // Coalesce: 重置 timer
            cancelTimer();
        } else if (currentState == State.IDLE) {
            // 首次触发
            state.set(State.REBALANCE_WAIT);
        } else {
            // IN_PROGRESS: 当前正在执行，不打断（下次 check 会覆盖）
            return;
        }

        // 启动随机等待 timer (3~10s)
        int waitMs = 3000 + new Random().nextInt(7000);
        rebalanceTimer = scheduler.schedule(() -> executeRebalance(topic, newQueues),
                waitMs, TimeUnit.MILLISECONDS);
        logger.debug("Rebalance wait started for topic '{}': {}ms", topic, waitMs);
    }

    private void executeRebalance(String topic, List<Integer> newQueues) {
        state.set(State.REBALANCE_IN_PROGRESS);
        try {
            List<Integer> oldQueues = allocatedQueues.getOrDefault(topic, Collections.emptyList());
            allocatedQueues.put(topic, newQueues);

            logger.info("Rebalance executing for topic '{}': old={} → new={}", topic, oldQueues, newQueues);

            if (rebalanceListener != null) {
                rebalanceListener.onRebalance(topic, oldQueues, newQueues);
            }
        } finally {
            state.set(State.IDLE);
        }
    }

    private void cancelTimer() {
        ScheduledFuture<?> timer = rebalanceTimer;
        if (timer != null && !timer.isDone()) {
            timer.cancel(false);
        }
    }

    // ===== 确定性分配算法 =====

    /**
     * 按 consumerId 排序后平均分配 queueId，返回当前 consumer 负责的 queue 列表
     */
    public List<Integer> calculateMyAllocation(String topic, int queueCount,
                                                List<String> sortedConsumerIds) {
        int index = sortedConsumerIds.indexOf(this.consumerId);
        if (index < 0) return Collections.emptyList();

        int consumerCount = sortedConsumerIds.size();
        int base = queueCount / consumerCount;
        int remainder = queueCount % consumerCount;

        int start = index * base + Math.min(index, remainder);
        int count = base + (index < remainder ? 1 : 0);

        List<Integer> result = new ArrayList<>();
        for (int q = start; q < start + count; q++) {
            result.add(q);
        }
        return result;
    }

    // ===== NameServer 通信 =====

    private List<String> registerToNameServer() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("consumerGroup", consumerGroup);
        req.put("consumerId", consumerId);
        req.put("topics", topics);

        ProtocolMessage response = sendToNameServer(
                MessageType.CONSUMER_REGISTER_REQUEST, JsonUtils.toJson(req));
        if (response == null || response.getStatus() != ResponseCode.SUCCESS) return Collections.emptyList();

        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        Map<String, Object> respMap = JsonUtils.fromJson(body, Map.class);
        if (respMap == null || !respMap.containsKey("consumerIdList")) return Collections.emptyList();

        @SuppressWarnings("unchecked")
        List<String> ids = (List<String>) respMap.get("consumerIdList");
        return ids != null ? ids : Collections.emptyList();
    }

    private List<String> fetchConsumerIds() {
        // 重新注册（轻量级，NameServer 返回当前组列表）
        return registerToNameServer();
    }

    private int fetchQueueCount(String topic) {
        try {
            String reqJson = "{\"topic\":\"" + topic + "\"}";
            ProtocolMessage response = sendToNameServer(
                    MessageType.GET_ROUTEINFO_BY_TOPIC_REQUEST, reqJson);
            if (response == null || response.getStatus() != ResponseCode.SUCCESS) return 0;

            String body = new String(response.getBody(), StandardCharsets.UTF_8);
            Map<String, Object> respMap = JsonUtils.fromJson(body, Map.class);
            if (respMap == null) return 0;

            @SuppressWarnings("unchecked")
            Map<String, Object> routeData = (Map<String, Object>) respMap.get("topicRouteData");
            if (routeData == null) return 0;

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> queueDatas = (List<Map<String, Object>>) routeData.get("queueDatas");
            if (queueDatas == null || queueDatas.isEmpty()) return 0;

            // 取第一个 broker 的 writeQueueNums
            Object nums = queueDatas.get(0).get("writeQueueNums");
            return nums instanceof Number ? ((Number) nums).intValue() : 0;
        } catch (Exception e) {
            logger.error("Failed to fetch queue count for topic: " + topic, e);
            return 0;
        }
    }

    private void sendHeartbeat() {
        try {
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("consumerGroup", consumerGroup);
            req.put("consumerId", consumerId);
            sendToNameServer(MessageType.CONSUMER_HEARTBEAT_REQUEST, JsonUtils.toJson(req));
        } catch (Exception e) {
            logger.warn("Failed to send heartbeat for consumer: {}", consumerId, e);
        }
    }

    private ProtocolMessage sendToNameServer(MessageType type, String bodyJson) {
        try {
            if (nameServerClient == null || !nameServerClient.isConnected()) {
                nameServerClient = new NettyClient(nameServerHost, nameServerPort);
                nameServerClient.connect();
            }
            ProtocolMessage msg = new ProtocolMessage(type,
                    bodyJson != null ? bodyJson.getBytes(StandardCharsets.UTF_8) : null);
            return nameServerClient.sendSync(msg, 5000);
        } catch (Exception e) {
            logger.error("Failed to communicate with NameServer: type={}", type, e);
            return null;
        }
    }

    public void shutdown() {
        cancelTimer();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (nameServerClient != null) {
            nameServerClient.disconnect();
        }
        logger.info("QueueAllocationManager shutdown for consumer: {}", consumerId);
    }
}
