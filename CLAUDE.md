# CLAUDE.md

这份文件为 Claude Code (claude.ai/code) 在本仓库中工作时提供指导。

## 构建与测试命令

```bash
# 构建所有模块
mvn clean compile

# 运行所有测试
mvn test

# 运行单个测试类
mvn test -pl flare-mq-store -Dtest=DefaultMessageStoreTest

# 构建指定模块及其依赖
mvn clean package -pl flare-mq-broker -am

# 运行单个测试方法
mvn test -pl flare-mq-broker -Dtest=TopicManagerTest#testCreateTopic
```

项目基于 Java 8，测试框架使用 JUnit 5 + Mockito，源码编码为 UTF-8。

## 架构概览

FlareMQ 是一个自研的分布式消息队列系统，架构设计大量参考了 RocketMQ。通信层基于 Netty，使用自定义二进制协议。

### 模块依赖层级（自底向上）

```
flare-mq-common, flare-mq-protocol     ← 最底层（无内部依赖）
flare-mq-store                          ← 仅依赖 common
flare-mq-nameserver                     ← 依赖 protocol
flare-mq-broker                         ← 依赖 protocol、store、nameserver
flare-mq-client                         ← 依赖 protocol、common
flare-mq-console                        ← 监控管理控制台
flare-mq-example, flare-mq-test, flare-mq-integration-test  ← 顶层
```

### 核心模块

**flare-mq-protocol** — 网络通信层。定义了 `ProtocolMessage`（12 字节定长头：总长度 + 消息类型 + 请求ID + 状态码 + 变长消息体）、`NettyServer`/`NettyClient` 以及 `ProtocolEncoder`/`ProtocolDecoder` 编解码器。`ServerRequestHandler` 是 NameServer 和 Broker 接收请求的统一 SPI 接口。还包含零拷贝传输工具（`DirectBufferPool`、`ZeroCopyMessageTransfer`）。

**flare-mq-store** — 消息持久化引擎。采用 RocketMQ 的 CommitLog + ConsumeQueue 存储模型：
- `CommitLogManager` — 所有消息按顺序追加写入 CommitLog（每个 Topic 一个文件）
- `ConsumeQueueManager` — 按 Topic/Queue 维度的索引，指向 CommitLog 的物理偏移量
- `DefaultMessageStore` — 顶层门面类，协调写入 CommitLog → 构建 ConsumeQueue 索引 → 通过 ConsumeQueue 索引查找并读取消息的完整流程
- `MappedFile` / `SimpleMappedFile` — 基于内存映射文件的 IO，追求高吞吐
- `IntelligentStorageManager` + `MessageHeatAnalyzer` — 基于消息访问热度的自适应存储策略

**flare-mq-nameserver** — 注册与路由中心（无状态设计，类似 RocketMQ NameServer）：
- `ServiceRegistry` — 存储 Broker 注册信息（集群、地址、队列）
- `ServiceDiscovery` — 为客户端解析 Topic → Broker 地址
- `HealthChecker` — 定时剔除不活跃的 Broker
- `SmartRoutingEngine` — 三层智能路由：全局层（一致性哈希跨集群路由）→ 集群层（根据负载选择 Broker）→ 本地层（选择具体队列）
- `RouteInfoManager` — 管理 Topic→Queue→Broker 的路由表

**flare-mq-broker** — 消息代理服务器：
- `ClusterManager` — 编排 Broker 的完整生命周期：启动 Netty 服务器、向 NameServer 注册、管理 Master 选举（基于 brokerId 最小值）、集群健康监控、定期状态同步
- `BrokerRequestHandler` — 多路复用处理所有客户端请求：SEND_MESSAGE、PULL_MESSAGE、ACK_MESSAGE、CREATE_TOPIC、QUERY_TOPIC。发送流程委托给 TopicManager→QueueManager→DefaultMessageStore
- `TopicManager` — Topic 的增删改查，通过 NameServer RPC 完成默认 Topic 初始化
- `QueueManager` — 管理每个 Topic 下的队列分配，通过 `selectLeastLoadedQueue()` 实现写入负载均衡
- `StreamEngine` — 消息流处理，支持窗口聚合计算
- `AckManager` — 消费确认追踪，包含重试记录和死信队列

**flare-mq-client** — 客户端 SDK，包含 Producer 和 Consumer：
- `ProducerImpl` — 同步/异步/单向（oneway）发送。先连接 NameServer 获取 `TopicRouteInfo`（带缓存），再连接到目标 Broker。支持配置多个 NameServer 地址并具备故障转移能力
- `ConsumerImpl` — 基于 Pull 的消费循环（定时拉取 → 消费线程池处理 → 自动 ACK）。通过 `SubscriptionData.matchTag()` 支持 Tag 过滤
- 路由缓存：Producer 和 Consumer 均在本地缓存 TopicRouteInfo，基于过期时间自动刷新

### 数据流转

```
Producer → NameServer (获取路由信息) → Broker (发送消息)
                                           ↓
Consumer ← Broker (拉取消息) ← CommitLog ← ConsumeQueue 索引
    ↓
Broker (消费确认)
```

### 启动顺序

1. **NameServer** — `NameServerStartup`（默认端口 9876）。启动 Netty 服务器，开启定时健康扫描和路由清理任务
2. **Broker** — `BrokerStartup`（默认端口 10911）。创建 `ClusterManager`，启动 Netty 服务器，向 NameServer 注册，加入集群并完成 Master 选举
3. **客户端** — `ProducerImpl.start()` / `ConsumerImpl.start()` 先连接 NameServer，再按需与 Broker 建立长连接

### 协议消息类型

在 `MessageType` 枚举中定义：SEND_MESSAGE (10/11)、PULL_MESSAGE (20/21)、ACK_MESSAGE (22/23)、CREATE_TOPIC (30/31)、QUERY_TOPIC (32/33)、GET_ROUTEINFO_BY_TOPIC (34/35)、REGISTER_BROKER (38/39)，以及心跳类型。

### 核心性能优化深度解析

#### 1. 内存映射文件 (Memory-Mapped File)

整个存储引擎基于 `MappedFile`（`flare-mq-store/.../MappedFile.java:23`）构建，这是该项目追求极致 IO 性能的基石。

**操作系统层面的原理**

Java NIO 的 `FileChannel.map(MapMode.READ_WRITE, 0, fileSize)` 返回 `MappedByteBuffer`，它在操作系统内部做的事情是：

```
传统 IO 路径：
  用户态 buffer → read() 系统调用 → 内核空间 buffer → 磁盘控制器 → 磁盘
                         ↑                    ↑
                   上下文切换            数据拷贝（2次）
                   （2次）            

mmap 路径：
  用户态虚拟地址 ─── 直接映射 ─── 内核页缓存 ─── 磁盘
        ↑                              ↑
   内存操作即磁盘操作          由 OS 负责换页
```

关键优势：
- **零拷贝读取**：mmap 后读取变成了直接内存访问（`mappedByteBuffer.get(position)`），不经过 `read()` 系统调用，省去内核态→用户态的数据拷贝
- **OS 页缓存复用**：多个进程/线程可以共享同一页缓存，不会像普通 IO 那样每个 buffer 都独立占用堆内存
- **懒加载**：OS 只在真正访问时才从磁盘加载对应页（缺页中断），对一个大文件不必全部加载到 RAM

**MappedFile 中的三阶段写入位置追踪**

`MappedFile` 用三个 `AtomicInteger` 精确追踪消息的生命周期状态（`MappedFile.java:70-80`）：

```
磁盘 ←── flushedPosition ←── committedPosition ←── wrotePosition
         （已落盘）            （已提交到文件系统缓存）    （应用层写入）

wrotePosition  - committedPosition  = 脏数据（在 JVM 堆/直接内存，未提交）
committedPosition - flushedPosition = 脏数据（在 OS 页缓存，未落盘）
flushedPosition                      = 安全数据（已落到磁盘）
```

写入流程（`MappedFile.java:278 synchronized`）：
1. 原子地读取 `wrotePosition` 获取当前写入点
2. 检查剩余空间是否足够
3. `mappedByteBuffer.position(currentPos); mappedByteBuffer.put(data)` —— 纯内存操作
4. `wrotePosition.addAndGet(length)` —— 原子更新

刷盘流程（`MappedFile.java:399`）：
- `commit()`：将 wrotePosition 的值复制到 committedPosition，标记"可刷盘"
- `flush()`：调用 `mappedByteBuffer.force()` 通知 OS 将脏页写回磁盘，完成后更新 flushedPosition
- 刷盘策略支持最少页数参数 `flushLeastPages`：如果脏页不够指定页数，跳过本次刷盘以减少无效 IO

**预热机制（MappedFile.java:256-266）**

```java
private void warmupMappedBuffer(MappedByteBuffer buffer) {
    int pageSize = 4096;  // 4KB 页大小
    for (int i = 0; i < fileSize; i += pageSize) {
        buffer.get(i);    // 触发页面加载
    }
}
```

预热的作用：文件刚 mmap 完成后，OS 尚未将任何物理页与虚拟地址绑定。此时第一次访问每个页都会触发**缺页中断**（major page fault），需要真正去读磁盘，延迟很高（毫秒级）。预热就是在启动阶段一页一页地访问，把整个文件"触碰"一遍，让 OS 提前把所有页都加载进内存。这样业务线程在正式读写时，页已经在内存中了，不会再遇到缺页中断。

**三级降级策略（MappedFile.java:194-251）**

`createMappedBuffer()` 实现了三级重试 + 降级：

```
第一级：尝试 mmap 大文件（3 次重试，递增等待 100ms/200ms/300ms）
         ↓ 失败（文件太大或系统 mmap 数量耗尽）
第二级：mmap 更小的文件（64KB），标记 useMmap = true
         ↓ 失败
第三级：useMmap = false，后续所有读写降级为 RandomAccessFile.seek() + read()/write()
```

故障时由 `CommitLogManager.createFallbackMappedFile()` 创建 `SimpleMappedFile` 作为后备（`CommitLogManager.java:322-372`）。`SimpleMappedFile` 不使用 mmap，直接通过 `RandomAccessFile` 进行传统文件 IO，但实现了相同的 `MappedFileInterface` 接口，对上层完全透明。降级后性能会显著下降，但**服务不中断**。

**文件预分配（MappedFile.java:154-158）**

```java
if (!this.file.exists() || this.file.length() < fileSize) {
    this.randomAccessFile.setLength(fileSize);
}
```

在初始化阶段就用 `setLength()` 预先分配好整个文件的空间。好处：
- 避免写入过程中文件系统因空间不足而报错
- 文件在磁盘上是连续的（大多数文件系统对预分配的文件会分配连续块），利于后续的 mmap 映射

**MappedFileInterface 统一接口**

`MappedFile` 和 `SimpleMappedFile` 都实现 `MappedFileInterface`（`MappedFileInterface.java`），对外暴露完全一致的 API。CommitLog 和 ConsumeQueue 通过 `ConcurrentSkipListMap<Long, MappedFileInterface>` 管理文件集合，因此降级切换对上层完全透明。

---

#### 2. CommitLog + ConsumeQueue 存储模型

这是 RocketMQ 经典的设计模式，核心思想是**"读写分离"**——用两种数据结构分别优化写入路径和读取路径。

**整体架构**

```
写入路径：
  Producer ─→ Broker ─→ DefaultMessageStore.putMessage()
                              │
                              ├─→ CommitLogManager.appendMessage()  ← 顺序追加写入
                              │       └─→ MappedFile (mmap, 顺序写)
                              │
                              └─→ ConsumeQueueManager.putMessageIndex()  ← 构建索引
                                      └─→ ConsumeQueue (mmap, 每个队列独立)

读取路径：
  Consumer ─→ Broker ─→ DefaultMessageStore.getMessage()
                              │
                              ├─→ ConsumeQueue.getConsumeQueueUnit()  ← 查索引 (O(1) 定位)
                              │       └─→ 返回 (commitLogOffset, size, tagsHashCode)
                              │
                              └─→ CommitLogManager.getMessage()  ← 根据偏移量读消息
                                      └─→ MappedFile (mmap, 随机读)
```

**CommitLog — 顺序写入的极致**

`CommitLogManager`（`CommitLogManager.java:21`）用 `ConcurrentSkipListMap<Long, MappedFileInterface>` 管理一组固定的 CommitLog 文件：

- **文件名即是偏移量**：文件名是 20 位数字（如 `00000000000000000000`），代表该文件中第一条消息在全局 CommitLog 中的物理偏移量
- **文件大小固定**：`COMMIT_LOG_FILE_SIZE = 1MB`（`StoreConstants.java:15`，测试用，生产可调大比如 1GB）
- **查找文件 O(log N)**：`ConcurrentSkipListMap.floorKey(offset)` 在有序的 key 中二分查找，快速定位到包含指定偏移量的文件

`appendMessage()` 的写入流程（`CommitLogManager.java:109-172`）：

```java
// 1. 序列化消息为字节数组（不持锁）
byte[] messageBytes = MessageSerializer.serialize(message);

// 2. 获取或创建可写的 MappedFile（读锁，非阻塞）
MappedFileInterface mappedFile = getOrCreateMappedFile(messageBytes.length);

// 3. 读锁保护写入（允许多个写入线程并发操作不同文件）
readWriteLock.readLock().lock();
try {
    long msgOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
    mappedFile.appendMessage(messageBytes);  // 纯内存操作（mmap 写入）
    currentWriteOffset = msgOffset + messageBytes.length;
} finally {
    readWriteLock.readLock().unlock();
}
```

关键设计点：
- **读写锁而非互斥锁**：`appendMessage` 拿读锁（允许并发写入），只有创建新文件时才拿写锁（互斥）。多个线程可以同时写入不同的文件，或同一文件的剩余空间足够时顺序写入
- **序列化在锁外完成**：消息序列化（`MessageSerializer.serialize()`）是 CPU 密集型操作，在获取锁之前完成，减少锁持有时间
- **创建新文件带超时**：`createNewMappedFile()` 用 `Future.get(5, TimeUnit.SECONDS)` 超时保护，超时后走降级策略，防止 mmap 阻塞整个写入流程（`CommitLogManager.java:248-272`）

**CommitLog 写入的并发模型（读写锁分离）**

```
线程 A 写入文件 1    ─── 读锁 ✓
线程 B 写入文件 1    ─── 读锁 ✓  (只要文件 1 空间够)
线程 C 创建新文件 2  ─── 写锁 ✗  (等 A、B 释放读锁)
线程 D 读取文件 1    ─── 读锁 ✓  (读和写可并发)
```

`MappedFile.appendMessage()` 本身是 `synchronized` 的（`MappedFile.java:278`），保证同一个文件内写入的线程安全。

**ConsumeQueue — 轻量级索引**

ConsumeQueue 本质上是将"消费"和"存储"解耦的索引结构。每个 `(topic, queueId)` 对应唯一的 `ConsumeQueue` 实例（`ConsumeQueueManager.java:44-63` 用 `ConcurrentHashMap` 管理）。

索引单元 `ConsumeQueueUnit` 仅 20 字节（`ConsumeQueueUnit.java:50-56`）：

```
CommitLogOffset (8B) | Size (4B) | TagsHashCode (8B)
```

- **固定大小 = O(1) 定位**：因为每条记录都是固定 20 字节，所以根据 offset 找索引记录不需要遍历，直接计算文件内偏移 `offset * 20` 然后 `mappedFile.readBytes(relativeOffset, 20)` 即可（`ConsumeQueue.java:177-187`）
- **小文件**：每个 ConsumeQueue 文件 64KB，一个文件可存约 3200 条索引，查询时 mmap 后几乎全部在内存
- **Tags 过滤**：`putMessageIndex()` 时将 message.tags 的 hashCode 写入 `tagsHashCode` 字段，消费时 `matchTags()` 在索引层就过滤掉不匹配的消息，避免回查 CommitLog

**消息的二进制序列化格式**

`MessageSerializer`（`MessageSerializer.java:32-100`）定义了 CommitLog 中的消息存储格式：

```
TotalSize(4B)       ← 消息总长度
MagicCode(4B)       ← 魔数 0xAABBCCDD，用于快速校验
BodyCRC(4B)         ← 消息体的 CRC32 校验值
QueueId(4B)         ← 目标队列ID
Flag(4B)            ← 消息标志位
BornTimestamp(8B)   ← 消息生成时间戳（生产者设置）
StoreTimestamp(8B)  ← 消息存储时间戳（Broker 写入时设置）
BodyLength(4B)      ← 消息体长度
TopicLength(2B) + Topic(var)
TagsLength(2B) + Tags(var)
KeysLength(2B) + Keys(var)
PropertiesLength(2B) + Properties(var)
Body(var)
                    ─────────
固定头部 40 字节 + 可变长度属性
```

反序列化时（`MessageSerializer.java:105`）：
1. 先验证 `MagicCode`，快速排除非消息数据
2. 对 Body 做 CRC32 校验，与存储时的 `BodyCRC` 比对——检测磁盘静默损坏
3. 按顺序读取各字段

**读写路径的数据流对照**

```
【写入】DefaultMessageStore.putMessage() — StoreConstants.java:15, MESSAGE_MAX_SIZE=4MB
  1. validateMessage(message)                    ↔ 校验（topic 非空、body 非空、大小不超 4MB）
  2. appendResult = commitLogManager.appendMessage(message)  ↔ 顺序追加到 CommitLog
  3. consumeQueueManager.putMessageIndex(                    ↔ 构建 ConsumeQueue 索引
       topic, queueId, appendResult.wroteOffset,
       appendResult.wroteBytes, tagsHashCode
     )

【读取】DefaultMessageStore.getMessage(topic, queueId, offset, maxCount) — StoreConstants.java:20
  1. units = consumeQueueManager.getConsumeQueueUnits(      ↔ 从 ConsumeQueue 获取索引单元列表
       topic, queueId, offset, maxCount
     )
  2. for each unit:
       message = commitLogManager.getMessage(              ↔ 根据索引中的物理偏移量
         unit.commitLogOffset, unit.size                    从 CommitLog 读取完整消息
       )
  3. result.nextBeginOffset = offset + units.size()        ↔ 更新消费进度偏移量
```

**为什么这个模型性能好**

1. **写入快**：CommitLog 只做顺序追加。无论是 HDD 还是 SSD，顺序写速度都远大于随机写（HDD 可达 100-200MB/s vs 随机 1-2MB/s）
2. **读取也快**：ConsumeQueue 文件很小（64KB），mmap 后全部在系统页缓存中。索引查找是 O(1) 加一次 CommitLog 的随机读，但 CommitLog 的数据刚被写入时还热在 OS 页缓存中，实际的随机读也基本命中了内存
3. **存储浪费小**：ConsumeQueue 每条索引仅 20 字节。假设 1 亿条消息（每条 1KB），CommitLog ≈ 100GB，ConsumeQueue ≈ 2GB（2% 的存储开销）
4. **消息校验可靠**：CRC32 校验 + MagicCode 验证确保数据完整性，在 mmap 直接操作内存的场景下尤为重要（OS 页缓存可能被回收后再加载时发生损坏）

### 关键设计模式

- **CommitLog + ConsumeQueue**：所有消息顺序写入 CommitLog，ConsumeQueue 是按 Topic、按队列维度的索引，记录 (偏移量, 大小, tagsHash) → CommitLog 物理偏移。这种设计使写入吞吐与读取模式解耦
- **NameServer 注册中心**：无状态设计，NameServer 之间无数据同步。Broker 主动推送元数据，客户端按需拉取路由信息
- **Pull 模式消费**：ConsumerImpl 为每个订阅的 Topic 运行定时 Pull 循环，本地管理消费进度偏移量，消费成功后自动 ACK
- **Master 选举**：简单的 brokerId 最小优先算法 — 存活节点中 ID 最小的 Broker 成为 Master，无需额外引入外部协调服务
