package com.flare.mq.client.consumer;

import com.flare.mq.client.producer.Message;
import java.util.List;

/**
 * 消息监听器接口
 * 
 * @author FlareMQ Team
 */
public interface MessageListener {
    
    /**
     * 消费单条消息
     * 
     * @param message 消息
     * @return 消费状态
     */
    ConsumeStatus consumeMessage(Message message);
    
    /**
     * 批量消费消息
     * 
     * @param messages 消息列表
     * @return 消费状态
     */
    default ConsumeStatus consumeMessages(List<Message> messages) {
        // 默认实现：逐个消费
        for (Message message : messages) {
            ConsumeStatus status = consumeMessage(message);
            if (status != ConsumeStatus.CONSUME_SUCCESS) {
                return status;
            }
        }
        return ConsumeStatus.CONSUME_SUCCESS;
    }
}
