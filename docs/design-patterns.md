# FlareMQ 设计模式分析

## 1. 单例模式 (Singleton) — 双重检查锁

`DirectBufferPool` 使用经典的 double-checked locking 实现线程安全的单例。

**关键文件:** `flare-mq-protocol/src/main/java/com/flare/mq/protocol/zerocopy/DirectBufferPool.java:76-85`

```java
public static DirectBufferPool getInstance() {
    if (instance == null) {
        synchronized (DirectBufferPool.class) {
            if (instance == null) {
                instance = new DirectBufferPool(100);
            }
        }
    }
    return instance;
}
```

DirectBuffer 的分配和回收成本很高，全局复用同一个池可以显著减少 GC 压力。

---

## 2. 策略模式 (Strategy) — 算法族可动态切换

策略模式在项目中多处使用，将不同算法封装为可替换的策略：

| 位置 | 策略上下文 | 具体策略 |
|------|-----------|---------|
| `GlobalLoadBalancer` (nameserver) | `switch (strategy)` 分发 | 轮询、随机、一致性哈希、最少活跃、加权轮询 |
| `BrokerSelector` (nameserver) | `switch (strategy)` 分发 | 同上 5 种 + 按消息特征选择（性能/磁盘） |
| `LoadBalancer` (broker) | `LoadBalanceStrategy` 枚举 | 多种 Broker 负载均衡算法 |
| `AdaptiveStorageStrategy` (store) | 接口 → `DefaultAdaptiveStorageStrategy` | 根据消息热度选择冷/热/温存储 + 压缩策略 |

**关键文件:**
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/global/GlobalLoadBalancer.java`
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/cluster/BrokerSelector.java`
- `flare-mq-broker/src/main/java/com/flare/mq/broker/cluster/LoadBalancer.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/AdaptiveStorageStrategy.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/DefaultAdaptiveStorageStrategy.java`

策略模式让负载均衡算法和存储策略在运行时动态切换，完全符合开闭原则——新增策略无需修改调用方代码。

---

## 3. 门面模式 (Facade) — 存储引擎的统一入口

`DefaultMessageStore` 是存储层的门面类，对外暴露简洁的 `putMessage()` / `getMessage()` API，内部协调了复杂的子系统：

```
DefaultMessageStore (门面)
  ├── CommitLogManager   — 消息顺序追加写入
  ├── ConsumeQueueManager — 索引构建与查询
  └── 定时刷盘 + 统计任务
```

**关键文件:** `flare-mq-store/src/main/java/com/flare/mq/store/DefaultMessageStore.java`

调用方（`BrokerRequestHandler`）完全不需要知道 CommitLog + ConsumeQueue 的内部协作细节（如序列化格式、文件分片、偏移量计算等）。

---

## 4. 责任链模式 (Chain of Responsibility) — Netty Pipeline

通过 Netty 的 `ChannelPipeline` 构建消息处理链，每个 Handler 职责单一：

```
ByteBuf 流入
  → ProtocolDecoder  （字节 → ProtocolMessage，零拷贝优化）
  → ProtocolEncoder  （ProtocolMessage → 字节，出站时生效）
  → ServerHandler    （业务分发 → ServerRequestHandler 接口）
  → BrokerRequestHandler / NameServerRequestHandler（具体业务处理）
```

**关键文件:**
- `flare-mq-protocol/src/main/java/com/flare/mq/protocol/server/NettyServer.java:64-79`
- `flare-mq-protocol/src/main/java/com/flare/mq/protocol/server/ServerHandler.java`
- `flare-mq-protocol/src/main/java/com/flare/mq/protocol/server/ServerRequestHandler.java`

每个 Handler 只需关注自己这一环，可插拔、可替换。

---

## 5. 观察者模式 (Observer) — 健康检查与热度分析

- **`HealthChecker`** — 定时轮询 Broker 心跳表，当 Broker 超时未心跳时主动将其剔除。Broker 定期"通知"（心跳），HealthChecker 在观察到异常时触发清理动作，是发布-订阅模式的变体。
- **`MessageHeatAnalyzer`** — 记录并分析每条消息的访问频率，`IntelligentStorageManager` 根据热度变化调整消息的存储层级。

**关键文件:**
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/health/HealthChecker.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/MessageHeatAnalyzer.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/IntelligentStorageManager.java`

---

## 6. 静态工厂方法 (Static Factory Method)

`ProtocolMessage` 提供静态工厂方法创建不同类型的响应消息：

```java
ProtocolMessage.createSuccessResponse(type, requestId, body)
ProtocolMessage.createErrorResponse(type, requestId, code)
ProtocolMessage.createHeartbeatResponse(requestId)
```

**关键文件:** `flare-mq-protocol/src/main/java/com/flare/mq/protocol/ProtocolMessage.java`

相比直接 `new ProtocolMessage(...)` 再逐字段 set，静态工厂方法语义更清晰，封装了通用的构造逻辑。

---

## 7. 对象池模式 (Object Pool) — DirectBuffer 复用

`DirectBufferPool` 维护了一组不同大小的 `ConcurrentLinkedQueue<ByteBuffer>` 池：

- `acquire(size)` — 从池中复用已有 buffer，命中返回池中对象，未命中才分配新 DirectBuffer
- `release(buffer)` — 归还 buffer 到池中，避免 GC 频繁回收 DirectByteBuffer

**关键文件:** `flare-mq-protocol/src/main/java/com/flare/mq/protocol/zerocopy/DirectBufferPool.java`

DirectBuffer 的分配和回收不受 JVM 堆 GC 管理，成本高。对象池模式在这里是经典的性能优化手段。

---

## 8. 适配器模式 (Adapter) — 协议编解码

`ProtocolEncoder` / `ProtocolDecoder` 继承 Netty 的 `ByteToMessageDecoder` / `MessageToByteEncoder`，将两种不兼容的模型对接：

```
业务层: ProtocolMessage (totalLength + type + requestId + status + body)
          ↕ 适配
传输层: ByteBuf (原始字节流)
```

**关键文件:**
- `flare-mq-protocol/src/main/java/com/flare/mq/protocol/codec/ProtocolEncoder.java`
- `flare-mq-protocol/src/main/java/com/flare/mq/protocol/codec/ProtocolDecoder.java`

---

## 9. 桥接模式 (Bridge) — MappedFileInterface 的两种实现

`MappedFileInterface` 定义统一的文件读写接口，有两个维度截然不同的实现，但对外暴露完全一致的 API：

| 实现 | 底层 IO 方式 | 性能 |
|------|-------------|------|
| `MappedFile` | `MappedByteBuffer` (mmap) | 高（零拷贝） |
| `SimpleMappedFile` | `RandomAccessFile.seek()+read()/write()` | 低（传统 IO） |

**关键文件:**
- `flare-mq-store/src/main/java/com/flare/mq/store/MappedFileInterface.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/MappedFile.java`
- `flare-mq-store/src/main/java/com/flare/mq/store/SimpleMappedFile.java`

上层 `CommitLogManager` 通过 `ConcurrentSkipListMap<Long, MappedFileInterface>` 管理文件集合，完全不感知底层实现。当 mmap 创建失败时自动降级为 `SimpleMappedFile`，对上层透明——服务不中断。

**三级降级策略（MappedFile.java:194-251）：**
1. 尝试 mmap 大文件（3 次重试，递增等待）
2. 失败后 mmap 更小的文件（64KB）
3. 仍失败则降级为 RandomAccessFile 传统 IO

---

## 10. 代理模式 (Proxy) — 客户端 SDK

`ProducerImpl` / `ConsumerImpl` 实现 `Producer` / `Consumer` 接口，对使用者隐藏了背后的复杂网络通信细节：

```
用户代码 → ProducerImpl.send(msg)
              → 查 NameServer 获取路由（带本地缓存）
              → 连接目标 Broker
              → 序列化 + Netty 发送
              → 反序列化响应
           ← 返回 SendResult
```

**关键文件:**
- `flare-mq-client/src/main/java/com/flare/mq/client/producer/ProducerImpl.java`
- `flare-mq-client/src/main/java/com/flare/mq/client/consumer/ConsumerImpl.java`

用户感知的只是一个 `send(Message)` 方法调用，实际经历了路由发现、连接管理、协议编解码、故障转移等多个复杂步骤。本地 TopicRouteInfo 缓存 + 自动过期刷新进一步隐藏了网络延迟。

---

## 11. 读写锁模式 (ReadWriteLock)

在注册中心和路由管理器中广泛使用 `ReentrantReadWriteLock` 进行并发控制：

- **读操作**（查询路由、获取 Broker 列表）— 拿读锁，允许高并发
- **写操作**（注册/注销 Broker、更新路由表）— 拿写锁，互斥执行

**关键文件:**
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/registry/ServiceRegistry.java`
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/route/RouteInfoManager.java`

这是读多写少场景下的经典并发优化，用读写分离替代互斥锁，大幅提升查询吞吐。

---

## 12. 命令模式 (Command) — 协议消息分发

`BrokerRequestHandler.handleRequest()` 通过 `switch (request.getType())` 将不同的消息类型分发到不同的处理方法：

```
SEND_MESSAGE_REQUEST    → handleSendMessage()
PULL_MESSAGE_REQUEST    → handlePullMessage()
ACK_MESSAGE_REQUEST     → handleAckMessage()
CREATE_TOPIC_REQUEST    → handleCreateTopic()
DELETE_TOPIC_REQUEST    → handleDeleteTopic()
LIST_TOPICS_REQUEST     → handleListTopics()
UPDATE_CONSUMER_OFFSET  → handleUpdateConsumerOffset()
QUERY_CONSUMER_OFFSET   → handleQueryConsumerOffset()
```

**关键文件:** `flare-mq-broker/src/main/java/com/flare/mq/broker/BrokerRequestHandler.java:53-88`

每条消息携带完整的请求数据（消息类型 + 请求体），类似于 Command 对象封装了操作和参数。

---

## 13. 分层架构 + 组合模式 (Layered / Composite Routing)

`SmartRoutingEngine` 将路由决策分为三个独立层，每层专注不同粒度的路由：

```
SmartRoutingEngine.route(message)
  → 第1层 GlobalRouter   — 一致性哈希选集群（跨地域全局路由）
  → 第2层 ClusterRouter  — 负载均衡选 Broker（集群内路由）
  → 第3层 LocalRouter    — 选具体队列（Broker 内本地路由）
```

**关键文件:**
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/SmartRoutingEngine.java`
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/global/GlobalRouter.java`
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/cluster/ClusterRouter.java`
- `flare-mq-nameserver/src/main/java/com/flare/mq/nameserver/routing/local/LocalRouter.java`

每一层独立封装，层与层之间通过明确的输入/输出对象衔接（`GlobalRoute` → `ClusterRoute` → `RouteResult`），体现分层解耦的架构思想。新增路由层或替换某一层算法均不影响其他层。

---

## 总结

| 设计模式 | 应用场景 | 核心价值 |
|---------|---------|---------|
| **单例** | DirectBuffer 池 | 全局复用，减少分配开销 |
| **策略** | 负载均衡、存储选择 | 算法可运行时切换，符合开闭原则 |
| **门面** | 存储引擎入口 | 简化调用方，隐藏子系统复杂度 |
| **责任链** | Netty 消息处理管道 | 各环节独立，可插拔 |
| **观察者** | 健康检查、热度分析 | 解耦事件源与响应逻辑 |
| **静态工厂** | 协议消息创建 | 语义清晰，封装构造逻辑 |
| **对象池** | DirectBuffer 复用 | 减少 GC 压力，提升性能 |
| **适配器** | 协议编解码 | 对接 Netty 框架与自定义协议 |
| **桥接** | mmap/传统IO双实现 | 降级透明，服务不中断 |
| **代理** | 客户端 SDK | 隐藏网络通信复杂度 |
| **读写锁** | 注册/路由表并发控制 | 读多写少场景的并发优化 |
| **命令** | 消息类型分发处理 | 请求封装，集中路由 |
| **分层/组合** | 三层智能路由 | 每层独立，粒度分明 |
