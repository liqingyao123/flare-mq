package com.ruyuan.mq.broker.ack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * 消息确认管理器
 * 
 * 负责管理消息的确认状态、重试机制和死信处理
 * 
 * @author RuYuan MQ Team
 */
public class AckManager {
    
    private static final Logger logger = LoggerFactory.getLogger(AckManager.class);
    
    /**
     * 消息确认记录
     * Key: messageId, Value: AckRecord
     */
    private final ConcurrentMap<String, AckRecord> ackRecords = new ConcurrentHashMap<>();
    
    /**
     * 重试队列
     * Key: messageId, Value: RetryRecord
     */
    private final ConcurrentMap<String, RetryRecord> retryQueue = new ConcurrentHashMap<>();
    
    /**
     * 死信队列
     * Key: messageId, Value: DeadLetterRecord
     */
    private final ConcurrentMap<String, DeadLetterRecord> deadLetterQueue = new ConcurrentHashMap<>();
    
    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    /**
     * 默认确认超时时间（毫秒）
     */
    private static final long DEFAULT_ACK_TIMEOUT = 30 * 1000; // 30秒
    
    /**
     * 默认最大重试次数
     */
    private static final int DEFAULT_MAX_RETRY_TIMES = 16;
    
    /**
     * 重试检查间隔（毫秒）
     */
    private static final long RETRY_CHECK_INTERVAL = 5 * 1000; // 5秒
    
    /**
     * 启动确认管理器
     */
    public void start() {
        // 启动重试检查任务
        scheduler.scheduleWithFixedDelay(
            this::checkRetryMessages,
            RETRY_CHECK_INTERVAL,
            RETRY_CHECK_INTERVAL,
            TimeUnit.MILLISECONDS
        );
        
        // 启动确认超时检查任务
        scheduler.scheduleWithFixedDelay(
            this::checkAckTimeout,
            DEFAULT_ACK_TIMEOUT,
            DEFAULT_ACK_TIMEOUT,
            TimeUnit.MILLISECONDS
        );
        
        logger.info("AckManager启动成功");
    }
    
    /**
     * 关闭确认管理器
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("AckManager关闭完成");
    }
    
    /**
     * 添加待确认消息
     */
    public void addPendingAck(String messageId, String consumerGroup, String topic, int queueId) {
        AckRecord record = new AckRecord(messageId, consumerGroup, topic, queueId);
        ackRecords.put(messageId, record);
        
        logger.debug("添加待确认消息: messageId={}, consumerGroup={}", messageId, consumerGroup);
    }
    
    /**
     * 确认消息
     */
    public AckResult ackMessage(String messageId, String consumerGroup) {
        AckRecord record = ackRecords.get(messageId);
        if (record == null) {
            logger.warn("消息不存在: messageId={}", messageId);
            return AckResult.messageNotExist(messageId);
        }
        
        if (!record.getConsumerGroup().equals(consumerGroup)) {
            logger.warn("消费者组不匹配: messageId={}, expected={}, actual={}", 
                       messageId, record.getConsumerGroup(), consumerGroup);
            return AckResult.failure("消费者组不匹配", messageId);
        }
        
        if (record.getStatus() == AckStatus.ACKED) {
            logger.warn("消息已确认: messageId={}", messageId);
            return AckResult.alreadyAcked(messageId);
        }
        
        // 更新确认状态
        record.setStatus(AckStatus.ACKED);
        record.setAckTime(System.currentTimeMillis());
        
        // 从重试队列中移除
        retryQueue.remove(messageId);
        
        logger.info("消息确认成功: messageId={}, consumerGroup={}", messageId, consumerGroup);
        return AckResult.success(messageId);
    }
    
    /**
     * 批量确认消息
     */
    public AckResult ackMessages(List<String> messageIds, String consumerGroup) {
        List<String> successIds = new ArrayList<>();
        List<String> failedIds = new ArrayList<>();
        
        for (String messageId : messageIds) {
            AckResult result = ackMessage(messageId, consumerGroup);
            if (result.isSuccess()) {
                successIds.add(messageId);
            } else {
                failedIds.add(messageId);
            }
        }
        
        if (failedIds.isEmpty()) {
            return AckResult.success(successIds);
        } else {
            return AckResult.partialSuccess(successIds, failedIds);
        }
    }
    
    /**
     * 消息消费失败，加入重试队列
     */
    public void addToRetryQueue(String messageId, String reason) {
        AckRecord ackRecord = ackRecords.get(messageId);
        if (ackRecord == null) {
            logger.warn("消息不存在，无法加入重试队列: messageId={}", messageId);
            return;
        }
        
        RetryRecord retryRecord = retryQueue.get(messageId);
        if (retryRecord == null) {
            retryRecord = new RetryRecord(messageId, ackRecord.getConsumerGroup(), 
                                        ackRecord.getTopic(), ackRecord.getQueueId());
            retryQueue.put(messageId, retryRecord);
        }
        
        retryRecord.incrementRetryCount();
        retryRecord.setLastRetryTime(System.currentTimeMillis());
        retryRecord.setFailureReason(reason);

        // 更新AckRecord状态为重试中
        ackRecord.setStatus(AckStatus.RETRYING);

        // 计算下次重试时间（指数退避）
        long nextRetryTime = calculateNextRetryTime(retryRecord.getRetryCount());
        retryRecord.setNextRetryTime(System.currentTimeMillis() + nextRetryTime);
        
        logger.info("消息加入重试队列: messageId={}, retryCount={}, nextRetryTime={}", 
                   messageId, retryRecord.getRetryCount(), retryRecord.getNextRetryTime());
    }
    
    /**
     * 获取需要重试的消息
     */
    public List<RetryRecord> getRetryMessages() {
        List<RetryRecord> retryMessages = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        
        for (RetryRecord record : retryQueue.values()) {
            if (record.getNextRetryTime() <= currentTime && 
                record.getRetryCount() <= DEFAULT_MAX_RETRY_TIMES) {
                retryMessages.add(record);
            }
        }
        
        return retryMessages;
    }
    
    /**
     * 移动消息到死信队列
     */
    public void moveToDeadLetterQueue(String messageId, String reason) {
        AckRecord ackRecord = ackRecords.get(messageId);
        RetryRecord retryRecord = retryQueue.remove(messageId);
        
        if (ackRecord == null) {
            logger.warn("消息不存在，无法移动到死信队列: messageId={}", messageId);
            return;
        }
        
        DeadLetterRecord deadLetterRecord = new DeadLetterRecord(
            messageId, 
            ackRecord.getConsumerGroup(),
            ackRecord.getTopic(),
            ackRecord.getQueueId(),
            retryRecord != null ? retryRecord.getRetryCount() : 0,
            reason
        );
        
        deadLetterQueue.put(messageId, deadLetterRecord);
        
        // 更新确认记录状态
        ackRecord.setStatus(AckStatus.DEAD_LETTER);
        
        logger.warn("消息移动到死信队列: messageId={}, reason={}", messageId, reason);
    }
    
    /**
     * 获取消息确认状态
     */
    public AckStatus getMessageAckStatus(String messageId) {
        AckRecord record = ackRecords.get(messageId);
        return record != null ? record.getStatus() : null;
    }
    
    /**
     * 获取确认统计信息
     */
    public AckStats getAckStats() {
        AckStats stats = new AckStats();
        
        int pendingCount = 0;
        int ackedCount = 0;
        int deadLetterCount = 0;
        
        for (AckRecord record : ackRecords.values()) {
            switch (record.getStatus()) {
                case PENDING:
                    pendingCount++;
                    break;
                case ACKED:
                    ackedCount++;
                    break;
                case RETRYING:
                    pendingCount++; // 重试中的消息仍算作待确认
                    break;
                case TIMEOUT:
                    pendingCount++; // 超时的消息仍算作待确认
                    break;
                case DEAD_LETTER:
                    deadLetterCount++;
                    break;
            }
        }
        
        stats.setPendingAckCount(pendingCount);
        stats.setAckedCount(ackedCount);
        stats.setDeadLetterCount(deadLetterCount);
        stats.setRetryQueueSize(retryQueue.size());
        stats.setTotalMessageCount(ackRecords.size());
        
        return stats;
    }
    
    /**
     * 清理已确认的消息记录
     */
    public void cleanupAckedMessages(long beforeTime) {
        int cleanedCount = 0;
        
        for (Map.Entry<String, AckRecord> entry : ackRecords.entrySet()) {
            AckRecord record = entry.getValue();
            if (record.getStatus() == AckStatus.ACKED && 
                record.getAckTime() > 0 && 
                record.getAckTime() < beforeTime) {
                ackRecords.remove(entry.getKey());
                cleanedCount++;
            }
        }
        
        logger.info("清理已确认消息记录: {} 条", cleanedCount);
    }
    
    /**
     * 检查重试消息
     */
    private void checkRetryMessages() {
        try {
            List<RetryRecord> retryMessages = getRetryMessages();
            
            for (RetryRecord record : retryMessages) {
                if (record.getRetryCount() > DEFAULT_MAX_RETRY_TIMES) {
                    // 超过最大重试次数，移动到死信队列
                    moveToDeadLetterQueue(record.getMessageId(), "超过最大重试次数");
                } else {
                    // 重新投递消息（这里简化处理，实际应该重新发送到消费者）
                    logger.info("重试消息: messageId={}, retryCount={}", 
                               record.getMessageId(), record.getRetryCount());
                }
            }
            
        } catch (Exception e) {
            logger.error("检查重试消息失败", e);
        }
    }
    
    /**
     * 检查确认超时
     */
    private void checkAckTimeout() {
        try {
            long currentTime = System.currentTimeMillis();
            long timeoutThreshold = currentTime - DEFAULT_ACK_TIMEOUT;
            
            for (AckRecord record : ackRecords.values()) {
                if (record.getStatus() == AckStatus.PENDING && 
                    record.getCreateTime() < timeoutThreshold) {
                    // 确认超时，加入重试队列
                    addToRetryQueue(record.getMessageId(), "确认超时");
                }
            }
            
        } catch (Exception e) {
            logger.error("检查确认超时失败", e);
        }
    }
    
    /**
     * 计算下次重试时间（指数退避算法）
     */
    private long calculateNextRetryTime(int retryCount) {
        // 基础延迟时间：1秒
        long baseDelay = 1000;
        
        // 指数退避：1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
        long delay = Math.min(baseDelay * (1L << Math.min(retryCount - 1, 6)), 60000);
        
        return delay;
    }
    
    /**
     * 获取死信队列消息
     */
    public List<DeadLetterRecord> getDeadLetterMessages() {
        return new ArrayList<>(deadLetterQueue.values());
    }
    
    /**
     * 获取重试队列消息
     */
    public List<RetryRecord> getAllRetryMessages() {
        return new ArrayList<>(retryQueue.values());
    }
    
    /**
     * 获取待确认消息
     */
    public List<AckRecord> getPendingAckMessages() {
        List<AckRecord> pendingMessages = new ArrayList<>();
        for (AckRecord record : ackRecords.values()) {
            if (record.getStatus() == AckStatus.PENDING) {
                pendingMessages.add(record);
            }
        }
        return pendingMessages;
    }
}
