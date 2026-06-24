package com.ruyuan.mq.broker.stream;

import com.ruyuan.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 内置流处理引擎API
 * 
 * @author RuYuan MQ Team
 */
public class StreamAPI {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamAPI.class);
    
    private final StreamEngine streamEngine;
    private final ConcurrentHashMap<String, MessageStreamImpl> activeStreams;
    private final ScheduledExecutorService scheduler;
    
    public StreamAPI() {
        this.streamEngine = new StreamEngine();
        this.activeStreams = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(4);
        logger.info("StreamAPI initialized");
    }
    
    /**
     * 创建消息流
     */
    public MessageStream createStream(String topic) {
        logger.info("Creating stream for topic: {}", topic);
        MessageStreamImpl stream = new MessageStreamImpl(topic, streamEngine);
        activeStreams.put(topic, stream);
        return stream;
    }
    
    /**
     * 获取现有流
     */
    public MessageStream getStream(String topic) {
        return activeStreams.get(topic);
    }
    
    /**
     * 关闭流
     */
    public void closeStream(String topic) {
        MessageStreamImpl stream = activeStreams.remove(topic);
        if (stream != null) {
            stream.close();
            logger.info("Stream closed for topic: {}", topic);
        }
    }
    
    /**
     * 关闭所有流
     */
    public void shutdown() {
        logger.info("Shutting down StreamAPI...");
        activeStreams.values().forEach(MessageStreamImpl::close);
        activeStreams.clear();
        streamEngine.shutdown();
        scheduler.shutdown();
        logger.info("StreamAPI shutdown completed");
    }
    
    /**
     * 获取流统计信息
     */
    public StreamStatistics getStatistics() {
        return streamEngine.getStatistics();
    }
    
    /**
     * 消息流接口
     */
    public interface MessageStream {
        
        /**
         * 过滤消息
         */
        MessageStream filter(Predicate<Message> predicate);
        
        /**
         * 转换消息
         */
        MessageStream map(Function<Message, Message> mapper);
        
        /**
         * 窗口操作
         */
        MessageStream window(WindowSpec windowSpec);
        
        /**
         * 聚合操作
         */
        MessageStream aggregate(AggregateFunction aggregator);
        
        /**
         * 输出到目标Topic
         */
        void to(String outputTopic);
        
        /**
         * 启动流处理
         */
        void start();
        
        /**
         * 停止流处理
         */
        void stop();
        
        /**
         * 获取流状态
         */
        StreamStatus getStatus();
    }
}
