package com.flare.mq.broker.ack;

/**
 * 重试消息处理器 — 由 Broker 层注入，负责消息的重新投递
 */
public interface RetryMessageHandler {
    void onRetryMessage(RetryRecord retryRecord, AckRecord ackRecord);
}
