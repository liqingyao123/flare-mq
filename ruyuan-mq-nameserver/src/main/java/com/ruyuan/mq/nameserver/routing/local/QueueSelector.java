package com.ruyuan.mq.nameserver.routing.local;

import com.ruyuan.mq.nameserver.routing.RouteConstants;
import com.ruyuan.mq.nameserver.routing.model.RoutableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 队列选择器
 * 
 * 负责在Broker内选择合适的队列
 * 
 * @author RuYuan MQ Team
 */
public class QueueSelector {
    
    private static final Logger logger = LoggerFactory.getLogger(QueueSelector.class);
    
    /**
     * 轮询计数器
     */
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    /**
     * 当前选择策略
     */
    private String strategy = RouteConstants.QUEUE_SELECT_HASH;
    
    /**
     * MD5消息摘要
     */
    private MessageDigest md5;
    
    public QueueSelector() {
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("初始化MD5失败", e);
            throw new RuntimeException("无法初始化MD5", e);
        }
    }
    
    /**
     * 选择队列
     */
    public int selectQueue(RoutableMessage message, int queueCount) {
        if (queueCount <= 0) {
            logger.warn("队列数量无效: {}", queueCount);
            return 0;
        }
        
        if (queueCount == 1) {
            return 0;
        }
        
        int selectedQueue;
        
        // 根据策略选择队列
        switch (strategy) {
            case RouteConstants.QUEUE_SELECT_HASH:
                selectedQueue = selectByHash(message, queueCount);
                break;
            case RouteConstants.QUEUE_SELECT_ROUND_ROBIN:
                selectedQueue = selectByRoundRobin(queueCount);
                break;
            case RouteConstants.QUEUE_SELECT_RANDOM:
                selectedQueue = selectByRandom(queueCount);
                break;
            default:
                logger.warn("未知的队列选择策略: {}, 使用默认策略", strategy);
                selectedQueue = selectByHash(message, queueCount);
                break;
        }
        
        logger.debug("队列选择: strategy={}, queue={}, total={}", strategy, selectedQueue, queueCount);
        return selectedQueue;
    }
    
    /**
     * 基于哈希选择队列
     */
    private int selectByHash(RoutableMessage message, int queueCount) {
        String routingKey = buildRoutingKey(message);
        
        // 顺序消息特殊处理：确保相同Key的消息进入同一队列
        if (message.isOrderedMessage()) {
            return selectByConsistentHash(routingKey, queueCount);
        }
        
        // 普通消息使用简单哈希
        int hash = routingKey.hashCode();
        int queueId = Math.abs(hash) % queueCount;
        
        logger.debug("哈希选择队列: key={}, hash={}, queue={}", routingKey, hash, queueId);
        return queueId;
    }
    
    /**
     * 基于一致性哈希选择队列
     */
    private int selectByConsistentHash(String routingKey, int queueCount) {
        synchronized (md5) {
            md5.reset();
            md5.update(routingKey.getBytes());
            byte[] digest = md5.digest();
            
            // 取前4个字节构造int值
            int hash = 0;
            for (int i = 0; i < 4; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            
            int queueId = Math.abs(hash) % queueCount;
            logger.debug("一致性哈希选择队列: key={}, hash={}, queue={}", routingKey, hash, queueId);
            return queueId;
        }
    }
    
    /**
     * 轮询选择队列
     */
    private int selectByRoundRobin(int queueCount) {
        int queueId = roundRobinCounter.getAndIncrement() % queueCount;
        logger.debug("轮询选择队列: queue={}, counter={}", queueId, roundRobinCounter.get());
        return queueId;
    }
    
    /**
     * 随机选择队列
     */
    private int selectByRandom(int queueCount) {
        int queueId = ThreadLocalRandom.current().nextInt(queueCount);
        logger.debug("随机选择队列: queue={}", queueId);
        return queueId;
    }
    
    /**
     * 基于消息特征选择队列
     */
    public int selectByMessageCharacteristics(RoutableMessage message, int queueCount) {
        // 顺序消息必须使用哈希策略确保顺序
        if (message.isOrderedMessage()) {
            String routingKey = buildRoutingKey(message);
            return selectByConsistentHash(routingKey, queueCount);
        }
        
        // 事务消息优先使用哈希策略确保一致性
        if (message.isTransactionMessage()) {
            return selectByHash(message, queueCount);
        }
        
        // 大消息可以使用轮询分散负载
        if (message.getMessageSize() > 1024 * 1024) { // 1MB
            return selectByRoundRobin(queueCount);
        }
        
        // 高优先级消息使用哈希确保稳定路由
        if (message.getBusinessPriority() <= 2) {
            return selectByHash(message, queueCount);
        }
        
        // 默认使用配置的策略
        return selectQueue(message, queueCount);
    }
    
    /**
     * 基于负载选择队列
     */
    public int selectByLoad(RoutableMessage message, int queueCount, LocalRouter.QueueLoadInfo loadInfo) {
        if (loadInfo == null) {
            return selectQueue(message, queueCount);
        }
        
        // 选择负载最轻的队列
        int leastLoadedQueue = loadInfo.getLeastLoadedQueue(queueCount);
        
        // 如果是顺序消息，仍然需要保证相同Key进入同一队列
        if (message.isOrderedMessage()) {
            String routingKey = buildRoutingKey(message);
            int hashQueue = selectByConsistentHash(routingKey, queueCount);
            
            // 检查哈希队列的负载是否过高
            long hashQueueLoad = loadInfo.getQueueMessageCount(hashQueue);
            long leastLoad = loadInfo.getQueueMessageCount(leastLoadedQueue);
            
            // 如果哈希队列负载不超过最少负载队列的2倍，则使用哈希队列
            if (hashQueueLoad <= leastLoad * 2) {
                logger.debug("顺序消息负载选择队列: hash={}, load={}", hashQueue, hashQueueLoad);
                return hashQueue;
            }
        }
        
        logger.debug("负载选择队列: queue={}, load={}", leastLoadedQueue, 
                    loadInfo.getQueueMessageCount(leastLoadedQueue));
        return leastLoadedQueue;
    }
    
    /**
     * 构建路由键
     */
    private String buildRoutingKey(RoutableMessage message) {
        // 优先使用消息的Key
        String keys = message.getKeys();
        if (keys != null && !keys.isEmpty()) {
            return keys;
        }
        
        // 其次使用Topic + Tags
        String topic = message.getTopic();
        String tags = message.getTags();
        
        if (tags != null && !tags.isEmpty()) {
            return topic + "#" + tags;
        }
        
        // 最后使用Topic
        return topic;
    }
    
    /**
     * 验证队列选择的一致性
     */
    public boolean validateConsistency(RoutableMessage message, int queueCount, int expectedQueue) {
        if (strategy.equals(RouteConstants.QUEUE_SELECT_HASH)) {
            int actualQueue = selectByHash(message, queueCount);
            return actualQueue == expectedQueue;
        }
        
        // 其他策略不保证一致性
        return true;
    }
    
    /**
     * 设置选择策略
     */
    public void setStrategy(String strategy) {
        this.strategy = strategy;
        logger.info("设置队列选择策略: {}", strategy);
    }
    
    /**
     * 获取当前选择策略
     */
    public String getStrategy() {
        return strategy;
    }
    
    /**
     * 获取选择器统计信息
     */
    public String getSelectorStats() {
        return String.format("QueueSelector Stats: strategy=%s, roundRobinCounter=%d", 
                           strategy, roundRobinCounter.get());
    }
    
    /**
     * 重置轮询计数器
     */
    public void resetRoundRobinCounter() {
        roundRobinCounter.set(0);
        logger.debug("重置轮询计数器");
    }
}
