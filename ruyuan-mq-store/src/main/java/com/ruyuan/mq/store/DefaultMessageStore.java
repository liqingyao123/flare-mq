package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 默认消息存储实现
 * 
 * 整合CommitLog和ConsumeQueue，提供完整的消息存储服务
 * 
 * @author RuYuan MQ Team
 */
public class DefaultMessageStore {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class);
    
    /**
     * 存储路径
     */
    private final String storePath;
    
    /**
     * CommitLog管理器
     */
    private final CommitLogManager commitLogManager;
    
    /**
     * ConsumeQueue管理器
     */
    private final ConsumeQueueManager consumeQueueManager;
    
    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduledExecutorService;
    
    /**
     * 是否已启动
     */
    private volatile boolean started = false;
    
    /**
     * 构造函数
     */
    public DefaultMessageStore(String storePath) {
        this.storePath = storePath != null ? storePath : StoreConstants.DEFAULT_STORE_PATH;
        this.commitLogManager = new CommitLogManager(this.storePath);
        this.consumeQueueManager = new ConsumeQueueManager(this.storePath);
        this.scheduledExecutorService = Executors.newScheduledThreadPool(3);
        
        logger.info("DefaultMessageStore初始化完成: storePath={}", this.storePath);
    }
    
    /**
     * 启动存储服务
     */
    public void start() {
        if (started) {
            logger.warn("DefaultMessageStore已经启动");
            return;
        }

        // 从磁盘恢复ConsumeQueue状态
        consumeQueueManager.recover();

        // 启动定时刷盘任务
        scheduledExecutorService.scheduleAtFixedRate(
                this::flushCommitLog, 
                StoreConstants.FLUSH_INTERVAL_MS, 
                StoreConstants.FLUSH_INTERVAL_MS, 
                TimeUnit.MILLISECONDS
        );
        
        // 启动定时刷盘ConsumeQueue任务
        scheduledExecutorService.scheduleAtFixedRate(
                this::flushConsumeQueue, 
                StoreConstants.FLUSH_INTERVAL_MS, 
                StoreConstants.FLUSH_INTERVAL_MS, 
                TimeUnit.MILLISECONDS
        );
        
        // 启动统计任务
        scheduledExecutorService.scheduleAtFixedRate(
                this::printStats, 
                60000, // 1分钟
                60000, 
                TimeUnit.MILLISECONDS
        );
        
        started = true;
        logger.info("DefaultMessageStore启动成功");
    }
    
    /**
     * 存储消息
     */
    public PutMessageResult putMessage(Message message) {
        System.out.println("[DEBUG] DefaultMessageStore.putMessage开始: topic=" + (message != null ? message.getTopic() : "null"));
        if (!started) {
            System.out.println("[DEBUG] 服务未启动");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (message == null) {
            System.out.println("[DEBUG] 消息为空");
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 验证消息
        System.out.println("[DEBUG] 验证消息...");
        if (!validateMessage(message)) {
            System.out.println("[DEBUG] 消息验证失败");
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        System.out.println("[DEBUG] 消息验证通过");

        try {
            // 写入CommitLog
            System.out.println("[DEBUG] 开始写入CommitLog...");
            AppendMessageResult appendResult = commitLogManager.appendMessage(message);
            System.out.println("[DEBUG] CommitLog写入结果: " + appendResult);
            if (!appendResult.isSuccess()) {
                System.out.println("[DEBUG] 写入CommitLog失败: " + appendResult);
                logger.error("写入CommitLog失败: {}", appendResult);
                return new PutMessageResult(PutMessageStatus.PUT_MESSAGE_FAILED, appendResult);
            }
            System.out.println("[DEBUG] CommitLog写入成功");
            
            // 构建ConsumeQueue索引
            boolean indexResult = consumeQueueManager.putMessageIndex(
                    message.getTopic(),
                    message.getQueueId(),
                    appendResult.getWroteOffset(),
                    appendResult.getWroteBytes(),
                    message.getTagsHashCode()
            );
            
            if (!indexResult) {
                logger.error("构建ConsumeQueue索引失败: topic={}, queueId={}", 
                           message.getTopic(), message.getQueueId());
                return new PutMessageResult(PutMessageStatus.PUT_MESSAGE_FAILED, appendResult);
            }
            
            logger.debug("消息存储成功: topic={}, queueId={}, offset={}", 
                        message.getTopic(), message.getQueueId(), appendResult.getWroteOffset());
            
            return new PutMessageResult(PutMessageStatus.PUT_OK, appendResult);
            
        } catch (Exception e) {
            logger.error("存储消息异常", e);
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }
    
    /**
     * 根据偏移量获取消息
     */
    public Message getMessage(long commitLogOffset, int size) {
        if (!started) {
            return null;
        }
        
        return commitLogManager.getMessage(commitLogOffset, size);
    }
    
    /**
     * 拉取消息
     */
    public GetMessageResult getMessage(String topic, int queueId, long offset, int maxCount) {
        return getMessage(topic, queueId, offset, maxCount, null);
    }
    
    /**
     * 根据Tags拉取消息
     */
    public GetMessageResult getMessage(String topic, int queueId, long offset, int maxCount, String tags) {
        if (!started) {
            return new GetMessageResult(GetMessageStatus.SERVICE_NOT_AVAILABLE);
        }
        
        try {
            // 获取ConsumeQueue单元
            List<ConsumeQueueUnit> units;
            if (tags != null && !tags.isEmpty()) {
                units = consumeQueueManager.getConsumeQueueUnitsByTags(topic, queueId, offset, maxCount, tags);
            } else {
                units = consumeQueueManager.getConsumeQueueUnits(topic, queueId, offset, maxCount);
            }
            
            if (units == null || units.isEmpty()) {
                return new GetMessageResult(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }
            
            GetMessageResult result = new GetMessageResult(GetMessageStatus.FOUND);
            
            // 根据ConsumeQueue单元获取消息
            for (ConsumeQueueUnit unit : units) {
                Message message = commitLogManager.getMessage(unit.getCommitLogOffset(), unit.getSize());
                if (message != null) {
                    result.addMessage(message);
                }
            }
            
            // 设置下一个偏移量
            result.setNextBeginOffset(offset + units.size());
            
            return result;
            
        } catch (Exception e) {
            logger.error("拉取消息异常: topic={}, queueId={}, offset={}", topic, queueId, offset, e);
            return new GetMessageResult(GetMessageStatus.UNKNOWN_ERROR);
        }
    }
    
    /**
     * 获取队列的最大偏移量
     */
    public long getMaxOffset(String topic, int queueId) {
        return consumeQueueManager.getMaxOffset(topic, queueId);
    }
    
    /**
     * 获取队列的最小偏移量
     */
    public long getMinOffset(String topic, int queueId) {
        return consumeQueueManager.getMinOffset(topic, queueId);
    }
    
    /**
     * 验证消息
     */
    private boolean validateMessage(Message message) {
        if (message.getTopic() == null || message.getTopic().isEmpty()) {
            logger.warn("消息Topic为空");
            return false;
        }
        
        if (message.getBody() == null) {
            logger.warn("消息Body为空");
            return false;
        }
        
        if (message.getBody().length > StoreConstants.MESSAGE_MAX_SIZE) {
            logger.warn("消息Body过大: {}", message.getBody().length);
            return false;
        }
        
        return true;
    }
    
    /**
     * 同步刷盘 — 强制将 CommitLog 和 ConsumeQueue 落盘
     */
    public void syncFlush() {
        commitLogManager.flush();
        consumeQueueManager.flushAll();
    }

    /**
     * 刷盘CommitLog
     */
    private void flushCommitLog() {
        try {
            commitLogManager.flush();
        } catch (Exception e) {
            logger.error("刷盘CommitLog异常", e);
        }
    }
    
    /**
     * 刷盘ConsumeQueue
     */
    private void flushConsumeQueue() {
        try {
            consumeQueueManager.flushAll();
        } catch (Exception e) {
            logger.error("刷盘ConsumeQueue异常", e);
        }
    }
    
    /**
     * 打印统计信息
     */
    private void printStats() {
        try {
            ConsumeQueueStats stats = consumeQueueManager.getStats();
            logger.info("存储统计: CommitLog文件数={}, ConsumeQueue统计={}", 
                       commitLogManager.getMappedFileCount(), stats);
        } catch (Exception e) {
            logger.error("打印统计信息异常", e);
        }
    }
    
    /**
     * 关闭存储服务
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        
        logger.info("开始关闭DefaultMessageStore...");
        
        // 关闭定时任务
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 最后一次刷盘
        flushCommitLog();
        flushConsumeQueue();
        
        // 关闭组件
        commitLogManager.shutdown();
        consumeQueueManager.shutdown();
        
        started = false;
        logger.info("DefaultMessageStore关闭完成");
    }
    
    // ========== Getter方法 ==========
    
    public String getStorePath() {
        return storePath;
    }
    
    public boolean isStarted() {
        return started;
    }
    
    public CommitLogManager getCommitLogManager() {
        return commitLogManager;
    }
    
    public ConsumeQueueManager getConsumeQueueManager() {
        return consumeQueueManager;
    }

    /**
     * 获取持久化的消息总数（基于ConsumeQueue的maxOffset求和）
     */
    public long getTotalMessageCount() {
        return consumeQueueManager.getStats().getTotalMessages();
    }

    /**
     * 获取所有已知队列的 (topic, queueId) 列表（供消费组统计采集）
     */
    public java.util.List<String[]> getAllQueueKeys() {
        return consumeQueueManager.getAllQueueKeys();
    }
}
