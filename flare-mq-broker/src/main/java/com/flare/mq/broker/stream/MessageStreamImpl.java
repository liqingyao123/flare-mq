package com.flare.mq.broker.stream;

import com.flare.mq.store.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 消息流实现类
 * 
 * @author FlareMQ Team
 */
public class MessageStreamImpl implements StreamAPI.MessageStream {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageStreamImpl.class);
    
    private final String sourceTopic;
    private final StreamEngine streamEngine;
    private final List<StreamOperation> operations;
    private volatile StreamStatus status;
    private volatile String outputTopic;
    
    public MessageStreamImpl(String sourceTopic, StreamEngine streamEngine) {
        this.sourceTopic = sourceTopic;
        this.streamEngine = streamEngine;
        this.operations = new ArrayList<>();
        this.status = StreamStatus.INITIALIZED;
        
        logger.info("MessageStream created for topic: {}", sourceTopic);
    }
    
    @Override
    public StreamAPI.MessageStream filter(Predicate<Message> predicate) {
        operations.add(new FilterOperation(predicate));
        logger.debug("Added filter operation to stream: {}", sourceTopic);
        return this;
    }
    
    @Override
    public StreamAPI.MessageStream map(Function<Message, Message> mapper) {
        operations.add(new MapOperation(mapper));
        logger.debug("Added map operation to stream: {}", sourceTopic);
        return this;
    }
    
    @Override
    public StreamAPI.MessageStream window(WindowSpec windowSpec) {
        operations.add(new WindowOperation(windowSpec));
        logger.debug("Added window operation to stream: {} with spec: {}", sourceTopic, windowSpec);
        return this;
    }
    
    @Override
    public StreamAPI.MessageStream aggregate(AggregateFunction aggregator) {
        operations.add(new AggregateOperation(aggregator));
        logger.debug("Added aggregate operation to stream: {} with function: {}", 
                    sourceTopic, aggregator.getName());
        return this;
    }
    
    @Override
    public void to(String outputTopic) {
        this.outputTopic = outputTopic;
        logger.info("Stream output set to topic: {}", outputTopic);
    }
    
    @Override
    public void start() {
        if (!status.canStart()) {
            logger.warn("Cannot start stream in status: {}", status);
            return;
        }
        
        status = StreamStatus.RUNNING;
        logger.info("Stream started for topic: {} -> {}", sourceTopic, outputTopic);
    }
    
    @Override
    public void stop() {
        if (!status.canStop()) {
            logger.warn("Cannot stop stream in status: {}", status);
            return;
        }
        
        status = StreamStatus.STOPPED;
        logger.info("Stream stopped for topic: {}", sourceTopic);
    }
    
    @Override
    public StreamStatus getStatus() {
        return status;
    }
    
    /**
     * 处理消息
     */
    public void processMessage(Message message) {
        if (!status.isActive()) {
            logger.debug("Stream not active, ignoring message: {}", message.getKeys());
            return;
        }
        
        try {
            // 应用所有操作
            Message processedMessage = applyOperations(message);
            
            if (processedMessage != null && outputTopic != null) {
                // 发送到输出Topic（这里简化处理，实际应该集成到消息发送系统）
                logger.debug("Processed message sent to {}: {}", outputTopic, processedMessage.getKeys());
            }
            
        } catch (Exception e) {
            status = StreamStatus.ERROR;
            logger.error("Error processing message in stream: {}", sourceTopic, e);
        }
    }
    
    /**
     * 应用所有流操作
     */
    private Message applyOperations(Message message) {
        Message current = message;
        
        for (StreamOperation operation : operations) {
            current = operation.apply(current, streamEngine);
            if (current == null) {
                // 消息被过滤掉
                break;
            }
        }
        
        return current;
    }
    
    /**
     * 关闭流
     */
    public void close() {
        status = StreamStatus.CLOSED;
        operations.clear();
        logger.info("Stream closed for topic: {}", sourceTopic);
    }
    
    // Getters
    public String getSourceTopic() { return sourceTopic; }
    public String getOutputTopic() { return outputTopic; }
    public int getOperationCount() { return operations.size(); }
}

/**
 * 流操作接口
 */
interface StreamOperation {
    Message apply(Message message, StreamEngine engine);
}

/**
 * 过滤操作
 */
class FilterOperation implements StreamOperation {
    private final Predicate<Message> predicate;
    
    public FilterOperation(Predicate<Message> predicate) {
        this.predicate = predicate;
    }
    
    @Override
    public Message apply(Message message, StreamEngine engine) {
        return predicate.test(message) ? message : null;
    }
}

/**
 * 映射操作
 */
class MapOperation implements StreamOperation {
    private final Function<Message, Message> mapper;
    
    public MapOperation(Function<Message, Message> mapper) {
        this.mapper = mapper;
    }
    
    @Override
    public Message apply(Message message, StreamEngine engine) {
        return mapper.apply(message);
    }
}

/**
 * 窗口操作
 */
class WindowOperation implements StreamOperation {
    private static final Logger logger = LoggerFactory.getLogger(WindowOperation.class);
    private final WindowSpec windowSpec;

    public WindowOperation(WindowSpec windowSpec) {
        this.windowSpec = windowSpec;
    }

    @Override
    public Message apply(Message message, StreamEngine engine) {
        // 将消息添加到窗口管理器
        engine.processWindowMessage(message, windowSpec, window -> {
            // 窗口完成时的处理逻辑
            logger.debug("Window completed with {} messages", window.getMessageCount());
        });

        // 窗口操作不直接返回消息，而是通过窗口完成回调处理
        return null;
    }
}

/**
 * 聚合操作
 */
class AggregateOperation implements StreamOperation {
    private final AggregateFunction aggregator;
    
    public AggregateOperation(AggregateFunction aggregator) {
        this.aggregator = aggregator;
    }
    
    @Override
    public Message apply(Message message, StreamEngine engine) {
        // 聚合操作通常与窗口操作结合使用
        // 这里简化处理，实际应该在窗口完成时进行聚合
        List<Message> messages = new ArrayList<>();
        messages.add(message);
        return aggregator.aggregate(messages);
    }
}
