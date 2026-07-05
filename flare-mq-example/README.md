# FlareMQ 使用示例

本模块提供了FlareMQ的完整使用示例，涵盖了从基础功能到高级特性的各种使用场景。

## 目录结构

```
flare-mq-example/
├── src/main/java/com/ruyuan/mq/example/
│   ├── quickstart/           # 快速开始示例
│   │   └── QuickStartExample.java
│   ├── producer/             # Producer示例
│   │   └── SimpleProducerExample.java
│   ├── consumer/             # Consumer示例
│   │   └── SimpleConsumerExample.java
│   ├── advanced/             # 高级特性示例
│   │   └── AdvancedFeaturesExample.java
│   ├── performance/          # 性能测试示例
│   │   └── PerformanceTestExample.java
│   └── cluster/              # 集群模式示例
│       └── ClusterExample.java
├── src/main/resources/
│   └── logback.xml           # 日志配置
├── pom.xml                   # Maven配置
└── README.md                 # 本文档
```

## 示例说明

### 1. 快速开始示例 (QuickStartExample)

最基础的使用示例，演示：
- 基本的Producer和Consumer创建
- 同步发送消息
- 异步发送消息
- 消息监听和消费

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.quickstart.QuickStartExample
```

### 2. Producer示例 (SimpleProducerExample)

详细的Producer使用示例，演示：
- 同步发送消息
- 异步发送消息
- 单向发送消息
- 批量发送消息
- 消息属性设置
- 延迟消息发送

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.producer.SimpleProducerExample
```

### 3. Consumer示例 (SimpleConsumerExample)

详细的Consumer使用示例，演示：
- Push模式消费
- Pull模式消费
- 标签过滤消费
- 批量消费
- 消息确认机制

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.consumer.SimpleConsumerExample
```

### 4. 高级特性示例 (AdvancedFeaturesExample)

高级功能使用示例，演示：
- 顺序消息
- 事务消息
- 延迟消息
- 消息重试和死信队列
- 消息过滤

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.advanced.AdvancedFeaturesExample
```

### 5. 性能测试示例 (PerformanceTestExample)

性能测试和压力测试示例，演示：
- 吞吐量测试
- 延迟测试
- 并发测试
- 大消息测试

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.performance.PerformanceTestExample
```

### 6. 集群模式示例 (ClusterExample)

集群功能使用示例，演示：
- 集群消费模式
- 广播消费模式
- 多NameServer配置
- 故障转移机制

**运行方式：**
```bash
java -cp target/classes com.flare.mq.example.cluster.ClusterExample
```

## 运行前准备

### 1. 启动NameServer

```bash
# 启动NameServer
java -cp flare-mq-nameserver.jar com.flare.mq.nameserver.NameServerStartup
```

### 2. 启动Broker

```bash
# 启动Broker
java -cp flare-mq-broker.jar com.flare.mq.broker.BrokerStartup
```

### 3. 编译示例代码

```bash
# 在项目根目录执行
mvn clean compile -pl flare-mq-example
```

## 配置说明

### Producer配置

```java
ProducerConfig config = new ProducerConfig();
config.setProducerGroup("example_producer_group");    // Producer组名
config.setNameServerAddr("localhost:9876");           // NameServer地址
config.setSendMsgTimeout(5000);                       // 发送超时时间
config.setRetryTimesWhenSendFailed(3);               // 失败重试次数
config.setMaxBatchSize(10);                          // 批量发送大小
config.setBatchMaxWaitTime(100);                     // 批量等待时间
```

### Consumer配置

```java
ConsumerConfig config = new ConsumerConfig();
config.setConsumerGroup("example_consumer_group");    // Consumer组名
config.setNameServerAddr("localhost:9876");           // NameServer地址
config.setConsumeMode(ConsumeMode.CLUSTERING);        // 消费模式
config.setConsumeType(ConsumeType.CONSUME_PASSIVELY); // 消费类型
config.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET); // 消费位置
config.setConsumeTimeout(15000);                      // 消费超时时间
config.setMaxRetryTimes(3);                          // 最大重试次数
```

## 常见问题

### 1. 连接失败

确保NameServer和Broker已经启动，并且地址配置正确。

### 2. 消息发送失败

检查Topic是否存在，Producer配置是否正确。

### 3. 消息消费失败

检查Consumer配置，确保订阅的Topic和Tag正确。

### 4. 性能问题

调整批量大小、线程数等配置参数，参考性能测试示例。

## 最佳实践

1. **Producer最佳实践**
   - 使用异步发送提高吞吐量
   - 合理设置批量大小
   - 处理发送失败的情况
   - 及时关闭Producer释放资源

2. **Consumer最佳实践**
   - 合理设置消费线程数
   - 处理消费失败的情况
   - 避免消费逻辑过于复杂
   - 及时确认消息消费

3. **性能优化**
   - 使用合适的消息大小
   - 调整网络和存储参数
   - 监控系统资源使用情况
   - 定期清理过期数据

## 技术支持

如有问题，请参考：
- [系统架构设计文档](../docs/SYSTEM_ARCHITECTURE_DESIGN.md)
- [开发指南](../docs/DEVELOPMENT_GUIDE.md)
- [各阶段完成报告](../docs/)

或联系FlareMQ开发团队。
