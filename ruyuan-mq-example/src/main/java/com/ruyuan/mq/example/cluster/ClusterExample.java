package com.ruyuan.mq.example.cluster;

import com.ruyuan.mq.client.producer.Producer;
import com.ruyuan.mq.client.producer.ProducerImpl;
import com.ruyuan.mq.client.producer.ProducerConfig;
import com.ruyuan.mq.client.producer.Message;
import com.ruyuan.mq.client.producer.SendResult;
import com.ruyuan.mq.client.consumer.Consumer;
import com.ruyuan.mq.client.consumer.ConsumerImpl;
import com.ruyuan.mq.client.consumer.ConsumerConfig;
import com.ruyuan.mq.client.consumer.MessageListener;
import com.ruyuan.mq.client.consumer.ConsumeStatus;
import com.ruyuan.mq.client.consumer.ConsumeMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Cluster mode example
 * 
 * Demonstrates the cluster features of RuYuan MQ:
 * - Multi-NameServer configuration
 * - Cluster consumption mode
 * - Broadcast consumption mode
 * - Failover
 * 
 * @author RuYuan MQ Team
 */
public class ClusterExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterExample.class);
    
    public static void main(String[] args) {
        logger.info("=== RuYuan MQ cluster mode example ===");
        
        try {
            // Cluster consumption example
            clusteringConsumeExample();
            
            // Broadcast consumption example
            broadcastingConsumeExample();
            
            // Multi-NameServer example
            multiNameServerExample();
            
            // Failover example
            failoverExample();
            
            logger.info("=== Cluster mode example completed ===");
            
        } catch (Exception e) {
            logger.error("Cluster mode example failed to run", e);
        }
    }
    
    /**
     * Cluster consumption example
     */
    private static void clusteringConsumeExample() throws Exception {
        logger.info("--- Cluster consumption example ---");
        
        // Producer configuration
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("cluster_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(producerConfig);
        
        // 创建多个Consumer实例模拟集群
        ConsumerConfig consumerConfig1 = new ConsumerConfig();
        consumerConfig1.setConsumerGroup("cluster_consumer_group_1");
        consumerConfig1.setNameServerAddr("localhost:9876");
        consumerConfig1.setConsumeMode(ConsumeMode.CLUSTERING); // 集群消费

        ConsumerConfig consumerConfig2 = new ConsumerConfig();
        consumerConfig2.setConsumerGroup("cluster_consumer_group_2");
        consumerConfig2.setNameServerAddr("localhost:9876");
        consumerConfig2.setConsumeMode(ConsumeMode.CLUSTERING);
        
        Consumer consumer1 = new ConsumerImpl(consumerConfig1);
        Consumer consumer2 = new ConsumerImpl(consumerConfig2);
        
        try {
            // 启动Producer和Consumers
            producer.start();
            consumer1.start();
            consumer2.start();
            
            CountDownLatch latch = new CountDownLatch(10);
            
            // Consumer1监听器
            consumer1.subscribe("ClusterTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Consumer1 cluster consumption: messageId={}, body={}", 
                               message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // Consumer2监听器
            consumer2.subscribe("ClusterTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Consumer2 cluster consumption: messageId={}, body={}", 
                               message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // 发送消息
            for (int i = 0; i < 10; i++) {
                Message message = new Message("ClusterTopic", "ClusterTag", 
                                            ("Cluster message-" + i).getBytes());
                message.setKey("cluster-key-" + i);
                
                SendResult result = producer.send(message);
                logger.info("Send cluster message: index={}, messageId={}", i, result.getMessageId());
            }
            
            // 等待消费完成
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Cluster consumption completed, messages are consumed by two Consumers");
            } else {
                logger.warn("Cluster consumption timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
        
        logger.info("--- Cluster consumption example completed ---");
    }
    
    /**
     * 广播消费示例
     */
    private static void broadcastingConsumeExample() throws Exception {
        logger.info("--- Broadcast consumption example ---");
        
        // Producer configuration
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("broadcast_producer_group");
        producerConfig.setNameServerAddr("localhost:9876");
        
        Producer producer = new ProducerImpl(producerConfig);
        
        // 创建多个Consumer实例，使用广播模式
        ConsumerConfig consumerConfig1 = new ConsumerConfig();
        consumerConfig1.setConsumerGroup("broadcast_consumer_group_1");
        consumerConfig1.setNameServerAddr("localhost:9876");
        consumerConfig1.setConsumeMode(ConsumeMode.BROADCASTING); // 广播消费

        ConsumerConfig consumerConfig2 = new ConsumerConfig();
        consumerConfig2.setConsumerGroup("broadcast_consumer_group_2");
        consumerConfig2.setNameServerAddr("localhost:9876");
        consumerConfig2.setConsumeMode(ConsumeMode.BROADCASTING);
        
        Consumer consumer1 = new ConsumerImpl(consumerConfig1);
        Consumer consumer2 = new ConsumerImpl(consumerConfig2);
        
        try {
            // 启动Producer和Consumers
            producer.start();
            consumer1.start();
            consumer2.start();
            
            CountDownLatch latch = new CountDownLatch(10); // 5条消息 * 2个Consumer = 10次消费
            
            // Consumer1监听器
            consumer1.subscribe("BroadcastTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Consumer1 broadcast consumption: messageId={}, body={}", 
                               message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // Consumer2监听器
            consumer2.subscribe("BroadcastTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Consumer2 broadcast consumption: messageId={}, body={}", 
                               message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // 发送消息
            for (int i = 0; i < 5; i++) {
                Message message = new Message("BroadcastTopic", "BroadcastTag", 
                                            ("Broadcast message-" + i).getBytes());
                message.setKey("broadcast-key-" + i);
                
                SendResult result = producer.send(message);
                logger.info("Send broadcast message: index={}, messageId={}", i, result.getMessageId());
            }
            
            // 等待消费完成
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Broadcast consumption completed, each message is consumed by two Consumers");
            } else {
                logger.warn("Broadcast consumption timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
        
        logger.info("--- Broadcast consumption example completed ---");
    }
    
    /**
     * 多NameServer示例
     */
    private static void multiNameServerExample() throws Exception {
        logger.info("--- Multi-NameServer example ---");
        
        // Configure multiple NameServer addresses
        String nameServerAddrs = "localhost:9876;localhost:9877;localhost:9878";
        
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("multi_ns_producer_group");
        producerConfig.setNameServerAddr(nameServerAddrs);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setConsumerGroup("multi_ns_consumer_group");
        consumerConfig.setNameServerAddr(nameServerAddrs);
        
        Producer producer = new ProducerImpl(producerConfig);
        Consumer consumer = new ConsumerImpl(consumerConfig);
        
        try {
            producer.start();
            consumer.start();
            
            CountDownLatch latch = new CountDownLatch(5);
            
            consumer.subscribe("MultiNSTopic", "*", new MessageListener() {
                @Override
                public ConsumeStatus consumeMessage(Message message) {
                    logger.info("Multi-NameServer consumption: messageId={}, body={}", 
                               message.getMessageId(), new String(message.getBody()));
                    latch.countDown();
                    return ConsumeStatus.CONSUME_SUCCESS;
                }
            });
            
            // 发送消息测试多NameServer
            for (int i = 0; i < 5; i++) {
                Message message = new Message("MultiNSTopic", "MultiNSTag", 
                                            ("Multi-NameServer message-" + i).getBytes());
                message.setKey("multi-ns-key-" + i);
                
                SendResult result = producer.send(message);
                logger.info("Send message via multi-NameServer: index={}, messageId={}", i, result.getMessageId());
            }
            
            // 等待消费完成
            boolean success = latch.await(30, TimeUnit.SECONDS);
            if (success) {
                logger.info("Multi-NameServer example completed");
            } else {
                logger.warn("Multi-NameServer example timed out");
            }
            
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
        
        logger.info("--- Multi-NameServer example completed ---");
    }
    
    /**
     * 故障转移示例
     */
    private static void failoverExample() throws Exception {
        logger.info("--- Failover example ---");
        
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setProducerGroup("failover_producer_group");
        producerConfig.setNameServerAddr("localhost:9876;localhost:9877");
        producerConfig.setRetryTimesWhenSendFailed(3); // 失败重试3次
        producerConfig.setSendLatencyFaultEnable(true); // 启用延迟容错
        
        Producer producer = new ProducerImpl(producerConfig);
        
        try {
            producer.start();
            
            // 发送消息测试故障转移
            for (int i = 0; i < 10; i++) {
                try {
                    Message message = new Message("FailoverTopic", "FailoverTag", 
                                                ("Failover message-" + i).getBytes());
                    message.setKey("failover-key-" + i);
                    
                    SendResult result = producer.send(message);
                    logger.info("Failover send successful: index={}, messageId={}, queueId={}", 
                               i, result.getMessageId(), result.getQueueId());
                    
                } catch (Exception e) {
                    logger.warn("Failover send failed: index={}, error={}", i, e.getMessage());
                }
                
                // 模拟网络延迟
                Thread.sleep(100);
            }
            
        } finally {
            producer.shutdown();
        }
        
        logger.info("--- Failover example completed ---");
    }
}
