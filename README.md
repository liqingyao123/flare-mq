# FlareMQ 架构设计与消息存储结构详解

> 本文档以图形化的方式展示 FlareMQ 的完整架构、消息从发送到持久化再到消费的全链路、以及存储引擎中每个最小数据单元的结构。

---

## 目录

- [一、系统整体架构](#一系统整体架构)
- [二、消息发送→持久化的完整链路（时序图）](#二消息发送持久化的完整链路)
- [三、消息消费的完整链路（时序图）](#三消息消费的完整链路)
- [四、存储引擎完整层级结构](#四存储引擎完整层级结构)
- [五、磁盘文件物理布局](#五磁盘文件物理布局)
- [六、MappedFile 内部状态机与并发模型](#六mappedfile-内部状态机与并发模型)
- [七、从 ConsumeQueue 索引到 CommitLog 数据的寻址关系](#七从-consumequeue-索引到-commitlog-数据的寻址关系)
- [八、各模块核心类关系图](#八各模块核心类关系图)

---

## 一、系统整体架构

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              FlareMQ 分布式消息队列系统                                      │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                             │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      ┌───────────────────┐         │
│  │  Producer 1  │   │  Producer 2  │   │  Producer N  │      │  Console 监控后台   │         │
│  │ flare-mq-   │   │ flare-mq-   │   │ flare-mq-   │      │ flare-mq-console │         │
│  │   client     │   │   client     │   │   client     │      │  MonitorController│         │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘      └────────┬──────────┘         │
│         │                  │                  │                       │                    │
│         │   ① 获取路由      │                  │                       │ 查询监控            │
│         ▼                  ▼                  ▼                       ▼                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐          │
│  │                    NameServer 集群 (无状态)                                    │          │
│  │                    flare-mq-nameserver                                      │          │
│  │  ┌──────────────────────────────────────────────────────────────────────┐   │          │
│  │  │  ServiceRegistry  │  ServiceDiscovery  │  HealthChecker               │   │          │
│  │  │  (Broker 注册)     │  (路由查询)          │  (Broker 心跳/剔除)          │   │          │
│  │  ├──────────────────────────────────────────────────────────────────────┤   │          │
│  │  │  RouteInfoManager │  SmartRoutingEngine                             │   │          │
│  │  │   Topic → Broker   │  三层路由: 全局(一致性哈希)→集群(负载感知)→本地(队列选择)   │   │          │
│  │  └──────────────────────────────────────────────────────────────────────┘   │          │
│  └─────────────────────────────────────────────────────────────────────────────┘          │
│                                       │                                                     │
│             ② Broker 注册/心跳       │                                                     │
│                       ┌──────────────┼──────────────┐                                      │
│                       ▼              ▼              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                      Broker 集群 (Master-Slave)                                        │  │
│  │                      flare-mq-broker                                                  │  │
│  │                                                                                       │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────────────┐ │  │
│  │  │  ClusterManager                                                                  │ │  │
│  │  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐      │ │  │
│  │  │  │ReplicationMgr │  │ FailoverMgr   │  │ LoadBalancer  │  │ Master选举    │      │ │  │
│  │  │  └───────────────┘  └───────────────┘  └───────────────┘  └──────────────┘      │ │  │
│  │  └─────────────────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                                       │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────────────┐ │  │
│  │  │  BrokerRequestHandler (Netty Server, 端口 10911)                                  │ │  │
│  │  │  SEND_MESSAGE │ PULL_MESSAGE │ ACK_MESSAGE │ CREATE_TOPIC │ QUERY_TOPIC         │ │  │
│  │  └─────────────────────────────────────────────────────────────────────────────────┘ │  │
│  │                                          │                                            │  │
│  │           ┌──────────────────────────────┼──────────────────────────────┐             │  │
│  │           ▼                              ▼                              ▼             │  │
│  │  ┌──────────────────┐  ┌──────────────────────────────┐  ┌──────────────────┐        │  │
│  │  │  TopicManager     │  │       QueueManager           │  │  AckManager      │        │  │
│  │  │  Topic CRUD       │  │  selectLeastLoadedQueue()    │  │  消费确认/重试     │        │  │
│  │  │  默认Topic初始化   │  │  createQueuesForTopic()      │  │  死信队列         │        │  │
│  │  └──────────────────┘  └─────────────┬────────────────┘  └──────────────────┘        │  │
│  │                                      │                                                │  │
│  │  ┌───────────────────────────────────▼────────────────────────────────────────────┐  │  │
│  │  │                      flare-mq-store 存储引擎                                   │  │  │
│  │  │  ┌─────────────────────────────┐   ┌──────────────────────────────────┐       │  │  │
│  │  │  │  DefaultMessageStore (门面)  │   │  IntelligentStorageManager       │       │  │  │
│  │  │  │  putMessage() / getMessage()│   │  热度分析→热/温/冷/归档四级存储    │       │  │  │
│  │  │  └──────────┬──────────────────┘   └──────────────────────────────────┘       │  │  │
│  │  │             │                                                                  │  │  │
│  │  │    ┌────────▼──────────┐          ┌────────────────────────────┐              │  │  │
│  │  │    │  CommitLogManager │          │  ConsumeQueueManager       │              │  │  │
│  │  │    │  顺序追加写入       │  ────▶   │  索引构建 (20B/entry)       │              │  │  │
│  │  │    │  mmap 文件操作     │  索引    │  按 Topic+QueueId 维度     │              │  │  │
│  │  │    └────────┬──────────┘          └────────────────────────────┘              │  │  │
│  │  │             │                                                                  │  │  │
│  │  │    ┌────────▼──────────┐                                                       │  │  │
│  │  │    │    磁盘文件         │                                                       │  │  │
│  │  │    │  CommitLog 文件     │                                                       │  │  │
│  │  │    │  ConsumeQueue 文件  │                                                       │  │  │
│  │  │    └────────────────────┘                                                       │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                             │
│       ③ 消费消息(PULL)             ⑤ 消费确认(ACK)                                          │
│         ▲                           │                                                      │
│  ┌──────┴───────┐   ┌──────┴───────┐   ┌──────┴───────┐                                    │
│  │  Consumer 1  │   │  Consumer 2  │   │  Consumer N  │                                    │
│  │ flare-mq-   │   │ flare-mq-   │   │ flare-mq-   │                                    │
│  │   client     │   │   client     │   │   client     │                                    │
│  └──────────────┘   └──────────────┘   └──────────────┘                                    │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 架构要点说明

| 组件 | 职责 | 端口 |
|------|------|------|
| NameServer | 无状态注册中心，存储 Broker 集群信息，提供 Topic→Broker 路由查询 | 9876 |
| Broker | 消息代理，接收生产消息、存储、提供消费拉取 | 10911 |
| Producer | 消息生产者，先查 NameServer 获取路由，再连 Broker 发送 | 动态分配 |
| Consumer | 消息消费者，定时 Pull 模式拉取消息，消费后 ACK | 动态分配 |
| Console | 管理后台，提供系统监控和运维功能 | 动态分配 |

---

## 二、消息发送→持久化的完整链路

```
Producer             NameServer               Broker                 DefaultMessageStore        CommitLogManager         ConsumeQueueManager          磁盘
   │                     │                       │                          │                         │                        │                      │
   │  ① GET_ROUTEINFO    │                       │                          │                         │                        │                      │
   │  (topic="order")    │                       │                          │                         │                        │                      │
   │────────────────────▶│                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │  ② TopicRouteData   │                       │                          │                         │                        │                      │
   │  (broker=broker-a,   │                       │                          │                         │                        │                      │
   │   queueId=0..3)     │                       │                          │                         │                        │                      │
   │◀────────────────────│                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │  ③ SEND_MESSAGE     │                       │                          │                         │                        │                      │
   │  topic="order"       │                       │                          │                         │                        │                      │
   │  tags="pay"          │                       │                          │                         │                        │                      │
   │  body="{...}"        │                       │                          │                         │                        │                      │
   │─────────────────────────────────────────────▶│                          │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │  ④ BrokerRequestHandler  │                         │                        │                      │
   │                     │                       │  handleSendMessage()     │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │  ⑤ ensureTopicAndQueues  │                         │                        │                      │
   │                     │                       │   · TopicManager.createTopic("order")          │                        │                      │
   │                     │                       │   · QueueManager.createQueuesForTopic()        │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │  ⑥ selectLeastLoadedQueue│                         │                        │                      │
   │                     │                       │  → queueId = 2           │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │  ⑦ putMessage(message)   │                         │                        │                      │
   │                     │                       │─────────────────────────▶│                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │  ⑧ validateMessage()   │                        │                      │
   │                     │                       │                          │  · topic 非空            │                        │                      │
   │                     │                       │                          │  · body 非空             │                        │                      │
   │                     │                       │                          │  · body.len ≤ 4MB        │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │  ⑨ appendMessage()     │                        │                      │
   │                     │                       │                          │────────────────────────▶│                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │  ⑩ serialize(message)│                      │
   │                     │                       │                          │                         │   结构化二进制编码      │                      │
   │                     │                       │                          │                         │   (含 CRC32 校验)     │                      │
   │                     │                       │                          │                         │   → byte[240]         │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │  ⑪ getOrCreateMapped  │                      │
   │                     │                       │                          │                         │     File()            │                      │
   │                     │                       │                          │                         │     读写锁保护          │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │  ⑫ mmap 顺序写入       │                      │
   │                     │                       │                          │                         │  byte[240] →           │                      │
   │                     │                       │                          │                         │  mappedByteBuffer      │                      │
   │                     │                       │                          │                         │─────────────────────────────────────────────────▶│
   │                     │                       │                          │                         │                    OS 页缓存 (Page Cache)                     │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │  ⑬ AppendMessageResult│                      │
   │                     │                       │                          │                         │  (SUCCESS,             │                      │
   │                     │                       │                          │                         │   offset=5000,         │                      │
   │                     │                       │                          │                         │   size=240)            │                      │
   │                     │                       │                          │◀────────────────────────│                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │  ⑭ putMessageIndex()   │                        │                      │
   │                     │                       │                          │─────────────────────────────────────────────────▶│                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │                        │  ⑮ 构造 ConsumeQueue  │
   │                     │                       │                          │                         │                        │     Unit (20字节)     │
   │                     │                       │                          │                         │                        │   · commitLogOff=5000  │
   │                     │                       │                          │                         │                        │   · size=240           │
   │                     │                       │                          │                         │                        │   · tagsHashCode       │
   │                     │                       │                          │                         │                        │      =0xABCD          │
   │                     │                       │                          │                         │                        │                       │
   │                     │                       │                          │                         │                        │  ⑯ appendMessage()    │
   │                     │                       │                          │                         │                        │  byte[20] → Consume   │
   │                     │                       │                          │                         │                        │  Queue MappedFile     │
   │                     │                       │                          │                         │                        │──────────────────────▶│
   │                     │                       │                          │                         │                        │         ConsumeQueue 文件 (64KB, mmap)         │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │  ⑰ SEND_MESSAGE_RESPONSE │                         │                        │                      │
   │                     │                       │  (SUCCESS, msgId=...)    │                         │                        │                      │
   │◀────────────────────────────────────────────│                          │                         │                        │                      │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │   ═══════════════════════ 定时刷盘 (1s 间隔) ═══════════════════════        │
   │                     │                       │                          │                         │                        │                      │
   │                     │                       │                          │                         │  ⑱ flush() / force()  │  ⑲ flushAll()        │
   │                     │                       │                          │                         │  ByteBuffer.force()    │  ByteBuffer.force()  │
   │                     │                       │                          │                         │  ← CommitLog 文件       │  ← ConsumeQueue 文件  │
   │                     │                       │                          │                         │──────────────────────▶│◀─────────────────────│
   │                     │                       │                          │                         │     OS 脏页 → 磁盘       │     OS 脏页 → 磁盘    │
```

### 串联描述

1. **Producer 先问路**：通过 `GET_ROUTEINFO_BY_TOPIC` 请求从 NameServer 获取 Topic 的路由信息（包含 Broker 地址、Queue 列表）
2. **NameServer 返回路由**：返回 `TopicRouteData`，包含 Broker 的 Master 地址和该 Topic 下所有读写队列
3. **Producer 连 Broker 发消息**：通过 `SEND_MESSAGE_REQUEST` 协议消息发送到目标 Broker
4. **Broker 确保 Topic 和 Queue 存在**：若不存在则自动创建
5. **选择负载最小的队列**：`selectLeastLoadedQueue()` 轮询各队列消息数，选择最少的那一个
6. **序列化消息**：`MessageSerializer.serialize()` 将 Message 对象编码为二进制字节数组（含 CRC32 校验码）
7. **写入 CommitLog**：在 mmap 映射的虚拟内存空间上追加写入字节数组，纯内存操作，极快
8. **构建 ConsumeQueue 索引**：在对应的 ConsumeQueue 文件中追加一条 20 字节的索引记录
9. **返回发送结果**：Broker 返回 `SEND_MESSAGE_RESPONSE` 给 Producer
10. **定时异步刷盘**：`DefaultMessageStore` 每 1 秒触发 `flush()`，调用 `MappedByteBuffer.force()` 将 CommitLog 和 ConsumeQueue 的 OS 脏页写回磁盘

---

## 三、消息消费的完整链路

```
Consumer                NameServer              Broker              ConsumeQueueManager       CommitLogManager          磁盘
   │                        │                      │                        │                        │                     │
   │  ① subscribe("order",  │                      │                        │                        │                     │
   │     "pay", listener)   │                      │                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ══════════════ 启动定时 Pull 调度器 ══════════════                      │                        │                     │
   │  每 pullInterval 毫秒触发一次 pullMessageForTopic("order")              │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ② 读取本地消费进度     │                      │                        │                        │                     │
   │  consumeProgress       │                      │                        │                        │                     │
   │  ["order_0"] = 1024    │                      │                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ③ PULL_MESSAGE_REQUEST│                      │                        │                        │                     │
   │  topic="order"         │                      │                        │                        │                     │
   │  queueId=0 offset=1024 │                      │                        │                        │                     │
   │  maxNums=32            │                      │                        │                        │                     │
   │───────────────────────────────────────────────▶│                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ④ BrokerRequestHandler│                        │                     │
   │                        │                      │  handlePullMessage()   │                        │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ⑤ getMessage(         │                        │                     │
   │                        │                      │     topic, queueId=0,  │                        │                     │
   │                        │                      │     offset=1024,       │                        │                     │
   │                        │                      │     maxCount=32)       │                        │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ⑥ getConsumeQueueUnits │                       │                     │
   │                        │                      │───────────────────────▶│                        │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │                        │  ┌─────────────────┐   │                     │
   │                        │                      │                        │  │ O(1) 定位:       │   │                     │
   │                        │                      │                        │  │ 文件偏移 =        │   │                     │
   │                        │                      │                        │  │ (1024%3276)*20   │   │                     │
   │                        │                      │                        │  │ 读取 32×20=640B  │   │                     │
   │                        │                      │                        │  │ → ConsumeQueue   │   │                     │
   │                        │                      │                        │  │   Unit 数组      │   │                     │
   │                        │                      │                        │  │─────────────────▶│   │                     │
   │                        │                      │                        │  └─────────────────┘   │  ConsumeQueue 文件    │
   │                        │                      │                        │                        │  (64KB, mmap)        │
   │                        │                      │                        │                        │                     │
   │                        │                      │                        │  ⑦ List<ConsumeQueue   │                     │
   │                        │                      │                        │     Unit>               │                     │
   │                        │                      │◀───────────────────────│                        │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ⑧ for each unit in units:                     │                     │
   │                        │                      │     if (unit.matchTags("pay")):                │                     │
   │                        │                      │        继续                                    │                     │
   │                        │                      │     else:                                     │                     │
   │                        │                      │        skip (Tags 过滤在索引层完成)             │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ⑨ commitLogManager    │                        │                     │
   │                        │                      │     .getMessage(       │                        │                     │
   │                        │                      │       commitLogOffset, │                        │                     │
   │                        │                      │       size)            │                        │                     │
   │                        │                      │────────────────────────────────────────────────▶│                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │                        │  ┌─────────────────┐   │                     │
   │                        │                      │                        │  │ floorKey(offset) │   │                     │
   │                        │                      │                        │  │ → 定位 MappedFile │   │                     │
   │                        │                      │                        │  │ 计算相对偏移量     │   │                     │
   │                        │                      │                        │  │ .readBytes(pos,  │   │                     │
   │                        │                      │                        │  │  size)           │   │                     │
   │                        │                      │                        │  │ → byte[]         │   │                     │
   │                        │                      │                        │  │─────────────────▶│   │  CommitLog 文件      │
   │                        │                      │                        │  └─────────────────┘   │  (1MB, mmap)         │
   │                        │                      │                        │                        │                     │
   │                        │                      │                        │  ⑩ Message.deserialize│                     │
   │                        │                      │                        │     (byte[])           │                     │
   │                        │                      │                        │     验证 MagicCode     │                     │
   │                        │                      │                        │     验证 CRC32         │                     │
   │                        │                      │                        │     → Message 对象     │                     │
   │                        │                      │                        │                        │                     │
   │                        │                      │  ⑪ GetMessageResult    │                        │                     │
   │                        │                      │  (messages=[msg1,msg2,  │                        │                     │
   │                        │                      │   ...],                 │                        │                     │
   │                        │                      │   nextBeginOffset=1056) │                        │                     │
   │                        │                      │◀───────────────────────│                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⑫ PULL_MESSAGE_RESPONSE                     │                        │                        │                     │
   │◀──────────────────────────────────────────────│                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⑬ 提交到消费线程池    │                      │                        │                        │                     │
   │  consumeExecutor:      │                      │                        │                        │                     │
   │  for each msg:         │                      │                        │                        │                     │
   │    listener.consume()  │                      │                        │                        │                     │
   │    消费成功 → 自动 ACK │                      │                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⑭ ACK_MESSAGE_REQUEST │                      │                        │                        │                     │
   │  messageId="order_..."  │                      │                        │                        │                     │
   │───────────────────────────────────────────────▶│                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⑮ ACK_MESSAGE_RESPONSE│                      │                        │                        │                     │
   │◀───────────────────────│──────────────────────│                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⑯ 更新消费进度        │                      │                        │                        │                     │
   │  consumeProgress       │                      │                        │                        │                     │
   │  ["order_0"] = 1056    │                      │                        │                        │                     │
   │                        │                      │                        │                        │                     │
   │  ⏱ 等待下一个 Pull 周期│                      │                        │                        │                     │
```

### 消费链路要点

- Consumer 采用 **定时 Pull** 模式，`ConsumerConfig.pullInterval` 控制拉取频率
- 拉取请求不经过 NameServer，Consumer 本地缓存了 `TopicRouteInfo`（5 分钟过期）
- **Tags 过滤在 ConsumeQueue 索引层完成**：索引中记录了 `tagsHashCode`，不匹配的索引条目直接跳过，避免了无意义的 CommitLog 回查
- 消费成功后 **自动 ACK**（`ConsumerImpl.consumeMessages()` 中调用 `ackMessage()`）
- 本地消费进度 `consumeProgress` 记录每个 (topic, queueId) 的已消费偏移量

---

## 四、存储引擎完整层级结构

```
DefaultMessageStore (flare-mq-store/.../DefaultMessageStore.java)
│
├─ putMessage(message)  ──── 消息写入入口
│   │
│   ├─ ① validateMessage(message)
│   │     · topic 非空
│   │     · body 非空
│   │     · body.length ≤ 4MB (MESSAGE_MAX_SIZE)
│   │
│   ├─ ② commitLogManager.appendMessage(message)
│   │     │
│   │     │  ┌──────────────────────────────────────────────────────────┐
│   │     │  │         CommitLog 中的消息二进制格式                       │
│   │     │  │                                                          │
│   │     │  │   4B     TotalSize           消息总长度                    │
│   │     │  │   4B     MagicCode           魔数 0xAABBCCDD              │
│   │     │  │   4B     BodyCRC             Body 的 CRC32 校验值         │
│   │     │  │   4B     QueueId             目标队列 ID                   │
│   │     │  │   4B     Flag                消息标志位                    │
│   │     │  │   8B     BornTimestamp       生产者创建时间戳               │
│   │     │  │   8B     StoreTimestamp      Broker 存储时间戳             │
│   │     │  │   4B     BodyLength          Body 字节长度                 │
│   │     │  │  ────────────────────────  固定头部 40 字节 ─────────      │
│   │     │  │   2B     TopicLength         Topic 字符串长度              │
│   │     │  │   var    Topic(UTF-8)        Topic 名称                    │
│   │     │  │   2B     TagsLength          Tags 字符串长度               │
│   │     │  │   var    Tags(UTF-8)         Tags 标签                     │
│   │     │  │   2B     KeysLength          Keys 字符串长度               │
│   │     │  │   var    Keys(UTF-8)         消息 Key                      │
│   │     │  │   2B     PropertiesLength    属性字符串长度                 │
│   │     │  │   var    Properties(UTF-8)   业务扩展属性                   │
│   │     │  │   var    Body                消息体 (带 CRC32 校验)        │
│   │     │  │  ────────────────────────────────────────────             │
│   │     │  │  最大总长度: 4MB                                          │
│   │     │  └──────────────────────────────────────────────────────────┘
│   │     │
│   │     ├─ getOrCreateMappedFile(requiredSize)
│   │     │   │
│   │     │   ├─ 有空闲文件? ──▶ 复用 (读锁)
│   │     │   │
│   │     │   └─ 文件已满? ──▶ createNewMappedFile() (写锁, 带 5s 超时)
│   │     │       │
│   │     │       ├─ 正常路径: new MappedFile(path, 1MB)
│   │     │       │   ├─ 文件预分配: RandomAccessFile.setLength(fileSize)
│   │     │       │   ├─ mmap: FileChannel.map(READ_WRITE, 0, fileSize)
│   │     │       │   ├─ 重试: 3次, 递增等待 100ms/200ms/300ms
│   │     │       │   ├─ 预热: warmupMappedBuffer() 逐页 get() 加载
│   │     │       │   └─ 加入: mappedFiles.put(offset, mappedFile)
│   │     │       │
│   │     │       └─ 降级路径: new SimpleMappedFile(path, 64KB, offset)
│   │     │           ├─ 文件预分配: setLength(64KB)
│   │     │           └─ 传统 IO: RandomAccessFile.seek() + write()
│   │     │
│   │     └─ mappedFile.appendMessage(bytes)  ← synchronized, 纯 mmap 内存操作
│   │         │
│   │         │  写入前:                             写入后:
│   │         │  ┌──────────────────────┐    ┌──────────────────────┐
│   │         │  │  wrotePosition=5000   │    │  wrotePosition=5240  │
│   │         │  │  committedPosition    │    │  committedPosition   │
│   │         │  │    = 5000             │    │    = 5000            │
│   │         │  │  flushedPosition      │    │  flushedPosition     │
│   │         │  │    = 4000             │    │    = 4000            │
│   │         │  │                      │    │                      │
│   │         │  │ wrote: 0B 未提交       │    │ wrote: 240B 未提交    │
│   │         │  │ committed: 1000B 脏页  │    │ committed: 1240B 脏页 │
│   │         │  │ flushed: 4000B 已落盘  │    │ flushed: 4000B 已落盘 │
│   │         │  └──────────────────────┘    └──────────────────────┘
│   │         │
│   │         └─ 返回 AppendMessageResult(SUCCESS, offset=5000, size=240)
│   │
│   └─ ③ consumeQueueManager.putMessageIndex(topic="order", queueId=2,
│             commitLogOffset=5000, size=240, tagsHashCode=0xABCD)
│         │
│         ├─ getOrCreateConsumeQueue(topic="order", queueId=2)
│         │   └─ key = "order-2" → ConsumeQueue 实例 (每个 topic+queueId 唯一)
│         │       └─ ConcurrentHashMap 管理
│         │
│         └─ consumeQueue.putMessageIndex(5000, 240, 0xABCD)
│               │
│               ├─ 创建 ConsumeQueueUnit (固定 20 字节):
│               │   ┌─────────────────────────────────────┐
│               │   │    ConsumeQueueUnit (固定 20 字节)   │
│               │   │    ┌─────────────────────────────┐  │
│               │   │    │ Offset 0..7  │ commitLogOffset │ 8B │
│               │   │    │ Offset 8..11 │ size            │ 4B │
│               │   │    │ Offset 12..19│ tagsHashCode   │ 8B │
│               │   │    └─────────────────────────────┘  │
│               │   └─────────────────────────────────────┘
│               │
│               ├─ unit.serialize() → ByteBuffer.allocate(20).putLong().putInt().putLong()
│               │
│               └─ mappedFile.appendMessage(byte[20])
│                     └─ 写入 ConsumeQueue 的 MappedFile
│                        文件大小: 64KB = 可存 3276 条索引
│                        每条固定 20B → O(1) 随机定位
│
├─ getMessage(topic, queueId, offset, maxCount) ──── 消息消费入口
│   │
│   ├─ ① consumeQueueManager.getConsumeQueueUnits(topic, queueId, offset, maxCount)
│   │     │
│   │     └─ consumeQueue.getConsumeQueueUnits(offset=1024, maxCount=32)
│   │           │
│   │           ├─ for i = 0..31:
│   │           │     ├─ 计算文件索引: fileIndex = (1024+i) / 3276
│   │           │     ├─ 计算文件内偏移: relativePos = ((1024+i) % 3276) × 20
│   │           │     ├─ mappedFile.readBytes(relativePos, 20)
│   │           │     └─ ConsumeQueueUnit.deserialize(byte[20])
│   │           │         → 得到 (commitLogOffset, size, tagsHashCode)
│   │           │
│   │           └─ 返回 List<ConsumeQueueUnit> (最多 32 条)
│   │
│   └─ ② for each unit in units:
│         commitLogManager.getMessage(unit.commitLogOffset, unit.size)
│           │
│           ├─ mappedFile = findMappedFileByOffset(offset)
│           │     └─ ConcurrentSkipListMap.floorKey(offset) → O(log N)
│           │        文件名就是起始偏移量, floorKey 找到 ≤offset 的最大 key
│           │
│           ├─ relativePos = offset - mappedFile.getFileFromOffset()
│           └─ messageBytes = mappedFile.readBytes(relativePos, size)
│                 └─ MessageSerializer.deserialize(messageBytes)
│                       ① 验证 MagicCode = 0xAABBCCDD
│                       ② 验证 CRC32(Body) = BodyCRC
│                       ③ 按字段反序列化 → Message 对象
```

---

## 五、磁盘文件物理布局

```
~/flare-mq-store/                           ← DEFAULT_STORE_PATH (用户主目录)
│
├── commitlog/                               ← COMMIT_LOG_DIR
│   │
│   ├── 00000000000000000000                 ← 第 1 个 CommitLog 文件
│   │   ┌──────────────────────────────────────────────────────────────┐
│   │   │  文件名 "00000000000000000000" 表示该文件的消息起始物理偏移量 = 0 │
│   │   │                                                              │
│   │   │  Offset 0..239:   [Msg#1] TotalSize=240 MagicCode BodyCRC   │
│   │   │                   QueueId Flag BornTimestamp StoreTimestamp   │
│   │   │                   BodyLen TopicLen Topic TagsLen Tags        │
│   │   │                   KeysLen Keys PropLen Prop Body             │
│   │   │                                                              │
│   │   │  Offset 240..427: [Msg#2] 188B                               │
│   │   │  Offset 428..939: [Msg#3] 512B                               │
│   │   │  Offset 940..1059:[Msg#4] 120B                               │
│   │   │  ...                                                        │
│   │   │  Offset ~980240:  [Msg#N] ← wrotePosition                    │
│   │   │  Offset 980240..1MB: 空闲区域                                 │
│   │   └──────────────────────────────────────────────────────────────┘
│   │   文件大小: 1MB = COMMIT_LOG_FILE_SIZE (测试用, 生产可调如 1GB)
│   │
│   ├── 00000000000001048576                 ← 第 2 个 CommitLog 文件
│   │   ┌──────────────────────────────────────────────────────────────┐
│   │   │  文件名 = 1048576 = 1MB = 上一个文件写满了继续                   │
│   │   │                                                              │
│   │   │  Offset 0..319:   [Msg#N+1] 320B                             │
│   │   │  Offset 320..1343:[Msg#N+2] 1024B                            │
│   │   │  ...                                                        │
│   │   └──────────────────────────────────────────────────────────────┘
│   │
│   └── 00000000000002097152                 ← 第 3 个 CommitLog 文件
│       ...
│
├── consumequeue/                            ← CONSUME_QUEUE_DIR
│   │
│   ├── order/                               ← topic = "order"
│   │   │
│   │   ├── 0/                               ← queueId = 0
│   │   │   │
│   │   │   ├── 00000000000000000000         ← 第 1 个 ConsumeQueue 文件
│   │   │   │   ┌───────────────────────────────────────────────────┐
│   │   │   │   │  文件名 = 0 = 该文件第一条索引的逻辑偏移量            │
│   │   │   │   │  文件大小: 64KB = 最多 3276 条索引                  │
│   │   │   │   │                                                   │
│   │   │   │   │  逻辑偏移 0 (文件偏移 0..19):                     │
│   │   │   │   │    [commitLogOffset=5000] [size=240] [tags=0xABCD]│
│   │   │   │   │                          ↘                        │
│   │   │   │   │                指向 CommitLog offset 5000          │
│   │   │   │   │                                                   │
│   │   │   │   │  逻辑偏移 1 (文件偏移 20..39):                    │
│   │   │   │   │    [commitLogOffset=5240] [size=188] [tags=0x1234]│
│   │   │   │   │                          ↘                        │
│   │   │   │   │                指向 CommitLog offset 5240          │
│   │   │   │   │                                                   │
│   │   │   │   │  逻辑偏移 2 (文件偏移 40..59):                    │
│   │   │   │   │    [commitLogOffset=5428] [size=512] [tags=0xBEEF]│
│   │   │   │   │                          ↘                        │
│   │   │   │   │                指向 CommitLog offset 5428          │
│   │   │   │   │                                                   │
│   │   │   │   │  ...                                             │
│   │   │   │   └───────────────────────────────────────────────────┘
│   │   │   │
│   │   │   └── 00000000000000003276           ← 第 2 个 ConsumeQueue 文件
│   │   │       ┌───────────────────────────────────────────────────┐
│   │   │       │  文件名 = 3276 = 从第 3276 条索引开始               │
│   │   │       │  (上一个文件装满了 3276 条固定 20B 的索引)           │
│   │   │       └───────────────────────────────────────────────────┘
│   │   │
│   │   ├── 1/                               ← queueId = 1
│   │   │   └── 00000000000000000000
│   │   │
│   │   ├── 2/                               ← queueId = 2
│   │   │   └── 00000000000000000000
│   │   │
│   │   └── 3/                               ← queueId = 3 (默认 4 个队列)
│   │       └── 00000000000000000000
│   │
│   └── payment/                             ← topic = "payment"
│       ├── 0/
│       ├── 1/
│       └── ...
│
└── index/                                   ← INDEX_DIR (预留, 尚未实现)
```

### 文件命名规则

| 文件类型 | 文件名格式 | 含义 |
|----------|-----------|------|
| CommitLog | `00000000000000000000` (20位数字) | 该文件中首条消息的全局物理偏移量 |
| ConsumeQueue | `00000000000000000000` (20位数字) | 该文件中首条索引的逻辑偏移量 (消息序号) |

`CommitLogManager` 通过 `ConcurrentSkipListMap.floorKey(offset)` 二分查找定位文件，`ConsumeQueue` 通过 `(offset / 3276)` 计算文件索引实现 O(1) 定位。

---

## 六、MappedFile 内部状态机与并发模型

### 6.1 内存布局与三阶段位置追踪

```
                        MappedFile (1MB)
                        ═══════════════════
                        
    磁盘物理地址           mmap 映射的虚拟内存空间
  ┌──────────────┐      ┌──────────────────────────────────────────┐
  │  0 ── 1MB    │◀════▶│  MappedByteBuffer (READ_WRITE, 1MB)      │
  │              │ mmap │  position/limit = 读写窗口                 │
  │              │      │                                          │
  │              │      │  ┌────── 已刷盘区域 ───────────────────┐  │  flushedPosition = 4000
  │              │      │  │                                     │  │  ← 安全: 已落盘, 崩溃可恢复
  │              │      │  │  Offset 0 → 4000                    │  │
  │              │      │  │  (MappedByteBuffer.force() 完成)     │  │
  │              │      │  ├─────────────────────────────────────┤  │
  │              │      │  │  脏页区域 (OS 页缓存中, 未落盘)       │  │  committedPosition = 5000
  │              │      │  │                                     │  │
  │              │      │  │  Offset 4000 → 5000                 │  │
  │              │      │  │  (已 commit, OS 负责异步刷盘)         │  │
  │              │      │  │  OS 崩溃则丢失                       │  │
  │              │      │  ├─────────────────────────────────────┤  │
  │              │      │  │  wrote 区域 (未 commit)              │  │  wrotePosition = 5240
  │              │      │  │                                     │  │
  │              │      │  │  Offset 5000 → 5240                 │  │
  │              │      │  │  (仅应用程序可见, JVM 崩溃必丢)        │  │
  │              │      │  ├─────────────────────────────────────┤  │
  │              │      │  │  空闲区域                             │  │
  │              │      │  │                                     │  │
  │              │      │  │  Offset 5240 → 1MB                  │  │  ← 可写入
  │              │      │  └─────────────────────────────────────┘  │
  │              │      │                                          │
  └──────────────┘      └──────────────────────────────────────────┘
  
  三个位置的含义:
  
    wrotePosition ─────────────▶ 应用程序已写入的最远偏移量
                                   appendMessage() 后 +length (synchronized)
                                   每个 MappedFile 一个 AtomicInteger
    
    committedPosition ─────────▶ 已提交到 OS 文件系统缓存的边界
                                   commit() 方法: committedPosition = wrotePosition
                                   标记这些数据"可以落盘", OS 可见
    
    flushedPosition ───────────▶ 已通过 force() 落盘到物理磁盘的边界
                                   flush() 方法: MappedByteBuffer.force()
                                   完成后 flushedPosition = committedPosition
                                   标识这些数据已安全持久化

  状态转换图:
  
  ┌──────────┐   appendMessage()   ┌──────────┐   commit()   ┌──────────┐   flush()    ┌──────────┐
  │  空闲区域  │ ─────────────────▶ │  wrote   │ ───────────▶ │ commit   │ ───────────▶ │  flush   │
  │          │    (mmap put)       │  (未提交) │              │  (脏页)   │  (force())   │  (安全)   │
  └──────────┘                     └──────────┘              └──────────┘              └──────────┘
```

### 6.2 并发模型

```
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                      CommitLog 写入并发模型 (读写锁分离)                       │
  │                                                                             │
  │  线程 A: appendMessage(msg1)  ──▶  readLock.lock()                          │
  │     └─ 找到 MappedFile #1, 写入 offset 5000  ──▶  readLock.unlock()        │
  │                                                                             │
  │  线程 B: appendMessage(msg2)  ──▶  readLock.lock()                          │
  │     └─ 找到 MappedFile #1, 写入 offset 5240  ──▶  readLock.unlock()        │
  │         (synchronized 保证同一文件内串行写入)                                   │
  │                                                                             │
  │  线程 C: MappedFile #1 满了, 需要创建新文件                                     │
  │     └─ 尝试 writeLock.lock() ──▶ 等待 A、B 释放读锁                            │
  │        └─ 获得写锁 ──▶ new MappedFile(#2) ──▶ writeLock.unlock()            │
  │                                                                             │
  │  线程 D: getMessage(offset=3000)  ──▶  readLock.lock()                      │
  │     └─ 查找 MappedFile #1, readBytes()  ──▶  readLock.unlock()              │
  │         (读和写可并发, synchronized 保护同一文件内的读写)                         │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────┬──────────────────────────────────────────────────┐
  │  操作                      │  锁策略                                           │
  ├──────────────────────────┼──────────────────────────────────────────────────┤
  │  MappedFile.appendMessage │  synchronized 方法 (同一文件内串行)                  │
  │  CommitLog.appendMessage  │  ReadWriteLock.readLock (多文件并行写入)           │
  │  CommitLog.createNewFile  │  ReadWriteLock.writeLock (互斥创建, 5s超时)        │
  │  CommitLog.getMessage     │  ReadWriteLock.readLock (并发读取)                 │
  │  MappedFile.commit()      │  无锁 (仅读取 volatile wrotePosition 值)           │
  │  MappedFile.flush()       │  无锁 (ByteBuffer.force() 由 OS 保证线程安全)      │
  │  ConsumeQueue.putIndex    │  ReadWriteLock.writeLock (同一队列串行写索引)       │
  │  ConsumeQueue.getUnit     │  ReadWriteLock.readLock (并发读索引)               │
  └──────────────────────────┴──────────────────────────────────────────────────┘
```

### 6.3 三级降级策略

```
  appendMessage 需要新文件时:
  
  ┌───────────────────────────────────────────────────────────────────┐
  │  第一级: new MappedFile(path, fileSize)                            │
  │    ├─ RandomAccessFile.setLength(fileSize)  ← 预分配磁盘空间       │
  │    ├─ FileChannel.map(READ_WRITE, 0, fileSize) ← mmap             │
  │    ├─ 重试 3 次, 递增等待 100ms / 200ms / 300ms                     │
  │    └─ 成功 → 预热(warmupMappedBuffer) → 返回                       │
  │                                                                   │
  │  ↓ 失败 (文件太大 / 系统 mmap 数量耗尽 / OOM)                       │
  │                                                                   │
  │  第二级: new MappedFile(path, 64KB)                                │
  │    ├─ 改用更小的文件大小 (64KB) 再次尝试 mmap                        │
  │    ├─ setLength(64KB) + Channel.map(0, 64KB)                      │
  │    └─ 成功 → useMmap=true → 返回                                   │
  │                                                                   │
  │  ↓ 失败                                                           │
  │                                                                   │
  │  第三级: new SimpleMappedFile(path, 64KB, offset)                   │
  │    ├─ 不使用 mmap, 直接用 RandomAccessFile 做传统 IO                │
  │    ├─ 写入: file.seek(pos) + file.write(bytes)                    │
  │    ├─ 读取: file.seek(pos) + file.readFully(bytes)                 │
  │    ├─ 刷盘: file.getFD().sync()                                    │
  │    └─ 实现 MappedFileInterface 接口, 对上层完全透明                   │
  │                                                                   │
  │  性能会下降, 但服务不中断                                             │
  └───────────────────────────────────────────────────────────────────┘
```

---

## 七、从 ConsumeQueue 索引到 CommitLog 数据的寻址关系

```
ConsumeQueue (topic="order", queueId=0)
═══════════════════════════════════════════════════════════════════════════════

  逻辑偏移量    ConsumeQueue 索引条目 (每条固定 20B)            CommitLog 物理位置
  ─────────    ─────────────────────────────────         ──────────────────────────

    1024 :  [ commitLogOffset=5000  ] ────────────────────────▶  CommitLog: offset 5000
            [ size=240              ]                           ┌────────────────────┐
            [ tagsHashCode=0xABCD   ]                           │ Msg #5  240 bytes   │
                                                                │ 包含完整消息数据      │
    1025 :  [ commitLogOffset=5240  ] ──────┐                  └────────────────────┘
            [ size=188              ]       │                   CommitLog: offset 5240
            [ tagsHashCode=0x1234   ]       └─────────────────▶ ┌────────────────────┐
                                                                │ Msg #8  188 bytes   │
    1026 :  [ commitLogOffset=5428  ] ──────┐                  └────────────────────┘
            [ size=512              ]       │                   CommitLog: offset 5428
            [ tagsHashCode=0xBEEF   ]       └─────────────────▶ ┌────────────────────┐
                                                                │ Msg #11 512 bytes   │
    1027 :  [ commitLogOffset=5940  ] ──────┐                  └────────────────────┘
            [ size=120              ]       │                   CommitLog: offset 5940
            [ tagsHashCode=0x5678   ]       └─────────────────▶ ┌────────────────────┐
                                                                │ Msg #15 120 bytes   │
    1028 :  ...                                                  └────────────────────┘

  ─────────────────────────────────────────────────────────────────────────────────

  消费者请求: topic="order", queueId=0, offset=1024, maxCount=4

  步骤分解:
  ═════════

  ① O(1) 定位 ConsumeQueue 文件:
     文件索引   = 1024 / 3276 = 0  → 文件 "00000000000000000000"
     文件内偏移 = (1024 % 3276) × 20 = 1024 × 20 = 20480

  ② 读取 4 条 ConsumeQueueUnit (4 × 20B = 80B):
     mappedFile.readBytes(20480, 80) → byte[80]
     纯 mmap 内存读, 无需系统调用

  ③ 反序列化为 ConsumeQueueUnit 对象:
     for i = 0..3:
       unit = ConsumeQueueUnit.deserialize(buffer[offset+i*20 .. offset+(i+1)*20])

  ④ Tags 过滤 (在索引层):
     if (tags != null && !tags.isEmpty() && !unit.matchTags(tags)):
         跳过该索引, 继续下一个
     else:
         通过

  ⑤ 根据 commitLogOffset 回查 CommitLog:
     for each passed unit:
       mappedFile = commitLogMappedFiles.floorKey(unit.commitLogOffset)
       relativePos = unit.commitLogOffset - mappedFile.getFileFromOffset()
       messageBytes = mappedFile.readBytes(relativePos, unit.size)
       message = MessageSerializer.deserialize(messageBytes)
         ├─ 验证 MagicCode = 0xAABBCCDD
         ├─ 验证 CRC32(Body) = BodyCRC
         └─ 构造 Message 对象

  ⑥ 返回 GetMessageResult:
     - messages: List<Message> (实际拉到的消息列表)
     - nextBeginOffset: 1024 + 4 = 1028 (下次从哪开始拉)
     - minOffset: 1024 (该队列最小可拉偏移)
     - maxOffset: 当前最大偏移
```

### 寻址性能分析

| 步骤 | 操作 | 数据结构 | 时间复杂度 | IO 类型 |
|------|------|----------|-----------|--------|
| 定位 ConsumeQueue 文件 | `(offset / 3276)` 计算 | 固定文件大小 | O(1) | 无 IO |
| 定位 ConsumeQueue 索引 | `(offset % 3276) × 20` 计算 | 固定记录大小 | O(1) | mmap 内存读 |
| Tags 过滤 | `tagsHashCode` 比对 | ConsumeQueueUnit | O(1)/条 | 无额外 IO |
| 定位 CommitLog 文件 | `floorKey(offset)` 二分查找 | ConcurrentSkipListMap | O(log N) | 无 IO |
| 定位 CommitLog 位置 | `offset - fileFromOffset` | 文件内偏移计算 | O(1) | mmap 内存读 |
| 反序列化 | ByteBuffer 顺序读取 | 固定二进制格式 | O(bodySize) | 无 IO |
| CRC32 校验 | `CRC32.update(body)` | 校验和 | O(bodySize) | 无 IO |

---

## 八、各模块核心类关系图

### 8.1 flare-mq-protocol — 网络通信层

```
┌─────────────────────────────────────────────────────────────────────┐
│                     flare-mq-protocol                               │
│                                                                     │
│  ┌─────────────────┐     ┌──────────────────┐                      │
│  │ ProtocolMessage  │     │ MessageType (枚举)│                      │
│  │  - length: int   │     │  REQUEST/RESPONSE│                      │
│  │  - type: MsgType │     │  SEND_MESSAGE    │                      │
│  │  - requestId:int │     │  PULL_MESSAGE    │                      │
│  │  - status: Code  │     │  ACK_MESSAGE     │                      │
│  │  - body: byte[]  │     │  CREATE_TOPIC    │                      │
│  │  (12B 固定头)     │     │  HEARTBEAT       │                      │
│  └────────┬────────┘     │  ...             │                      │
│           │              └──────────────────┘                      │
│           ▼                                                         │
│  ┌─────────────────┐     ┌──────────────────┐                      │
│  │ ProtocolEncoder  │     │ ProtocolDecoder  │                      │
│  │ (Netty编码器)    │     │ (Netty解码器)    │                      │
│  └────────┬────────┘     └────────┬─────────┘                      │
│           │                       │                                 │
│           ▼                       ▼                                 │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Netty Pipeline                             │   │
│  │  IdleStateHandler → ProtocolDecoder → ProtocolEncoder         │   │
│  │                     → ServerHandler / ClientHandler           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │ NettyServer       │    │ NettyClient       │                     │
│  │ - port: 9876/10911│    │ - host+port       │                     │
│  │ - bossGroup(1)    │    │ - workerGroup     │                     │
│  │ - workerGroup(N)  │    │ - Channel         │                     │
│  │ - TCP_NODELAY     │    │ - ResponseFuture  │                     │
│  │ - SO_KEEPALIVE    │    │ - sendSync()      │                     │
│  │ - SO_SNDBUF: 64KB │    │ - sendAsync()     │                     │
│  └────────┬─────────┘    └────────┬─────────┘                     │
│           │                       │                                 │
│           ▼                       ▼                                 │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │ServerRequestHandler│   │ ClientHandler     │                     │
│  │ (SPI 接口)        │    │ ResponseFuture    │                     │
│  │ handleRequest()   │    │ ResponseCallback  │                     │
│  │ onChannelActive() │    │ 超时清理           │                     │
│  │ onChannelInactive()│   └──────────────────┘                      │
│  └──────────────────┘                                              │
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │ZeroCopyTransfer  │    │ DirectBufferPool │                      │
│  │ MappedBuffer传输  │    │ 1K/4K/16K/64K池  │                      │
│  │ FileRegion传输    │    │ 预分配+命中率统计  │                      │
│  │ Buffer降级传输    │    │ 减少GC压力       │                      │
│  └──────────────────┘    └──────────────────┘                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 flare-mq-store — 消息持久化

```
┌─────────────────────────────────────────────────────────────────────┐
│                       flare-mq-store                                │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  DefaultMessageStore (门面类)                                  │  │
│  │   - start() / shutdown()                                      │  │
│  │   - putMessage(Message) → PutMessageResult                     │  │
│  │   - getMessage(topic, queueId, offset, maxCount)              │  │
│  │   - 定时刷盘 (1s) + 统计 (60s)                                 │  │
│  └──────────┬───────────────────┬───────────────────────────────┘  │
│             │                   │                                    │
│             ▼                   ▼                                    │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐    │
│  │ CommitLogManager │  │ ConsumeQueueManager                   │    │
│  │                  │  │                                      │    │
│  │ mappedFiles:     │  │ consumeQueueTable:                    │    │
│  │ ConcurrentSkip   │  │ ConcurrentHashMap<String,ConsumeQueue>│    │
│  │ ListMap<Long,    │  │                                      │    │
│  │  MappedFileI>    │  │ key = "topic-queueId"                │    │
│  │                  │  │                                      │    │
│  │ appendMessage()  │  │ putMessageIndex()                    │    │
│  │ getMessage()     │  │ getConsumeQueueUnits()               │    │
│  │ flush()          │  │ getMaxOffset() / getMinOffset()      │    │
│  │ 读写锁            │  │ flushAll() / shutdown()               │    │
│  └────────┬─────────┘  └──────────────┬───────────────────────┘    │
│           │                           │                              │
│           ▼                           ▼                              │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐    │
│  │ MappedFile       │  │ ConsumeQueue                         │    │
│  │ (实现 MappedFileI)│  │                                     │    │
│  │                  │  │ topic, queueId                       │    │
│  │ fileName,fileSize│  │ mappedFiles:                         │    │
│  │ fileFromOffset   │  │ ConcurrentSkipListMap<Long,MappedFile>│   │
│  │                  │  │                                     │    │
│  │ mappedByteBuffer │  │ putMessageIndex()                    │    │
│  │ (MappedByteBuffer)│ │ getConsumeQueueUnit(offset)          │    │
│  │                  │  │ getConsumeQueueUnits(start,count)    │    │
│  │ wrotePosition    │  │ getConsumeQueueUnitsByTags()         │    │
│  │ committedPosition│  │ flush()                              │    │
│  │ flushedPosition  │  │ 读写锁                                │    │
│  │                  │  └──────────────┬───────────────────────┘    │
│  │ appendMessage()  │                 │                            │
│  │ readBytes()      │                 ▼                            │
│  │ commit()/flush() │  ┌──────────────────────────────────────┐    │
│  │ warmup()         │  │ ConsumeQueueUnit                     │    │
│  │ isFull()/cleanup()│ │  - commitLogOffset: long (8B)        │    │
│  └────────┬─────────┘  │  - size: int (4B)                    │    │
│           │            │  - tagsHashCode: long (8B)           │    │
│           ▼            │  serialize()/deserialize()            │    │
│  ┌──────────────────┐  │  matchTags()/isValid()               │    │
│  │ SimpleMappedFile  │  └──────────────────────────────────────┘    │
│  │ (降级方案)        │                                              │
│  │                  │                                              │
│  │ RandomAccessFile │                                              │
│  │ seek()+write()   │                                              │
│  │ seek()+read()    │                                              │
│  │ getFD().sync()   │                                              │
│  └──────────────────┘                                              │
│                                                                     │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐    │
│  │MessageSerializer │  │ IntelligentStorageManager             │    │
│  │ serialize()      │  │  - MessageHeatAnalyzer               │    │
│  │ deserialize()    │  │  - AdaptiveStorageStrategy            │    │
│  │ CRC32 校验       │  │  - PerformanceMonitor                │    │
│  │ MagicCode 验证   │  │  热/温/冷/归档 四级存储分级             │    │
│  └──────────────────┘  └──────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.3 flare-mq-nameserver — 注册与路由中心

```
┌─────────────────────────────────────────────────────────────────────┐
│                     flare-mq-nameserver                              │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  NameServerController (主控制器)                               │  │
│  │   - NettyServer (9876端口)                                    │  │
│  │   - ScheduledExecutorService (4线程)                          │  │
│  │   - 定时: 扫描不活跃Broker(10s) / 清理过期路由(30s) / 统计(60s)  │  │
│  └──────────┬───────────────────────────────────────────────────┘  │
│             │                                                       │
│   ┌─────────┼─────────┬──────────────────┐                         │
│   ▼         ▼         ▼                  ▼                         │
│  ┌──────┐ ┌──────┐ ┌──────────┐ ┌──────────────────┐              │
│  │Service│ │Service│ │Health    │ │RouteInfoManager  │              │
│  │Registry│ │Discov-│ │Checker   │ │                  │              │
│  │       │ │ery    │ │          │ │ TopicRouteData   │              │
│  │ Broker│ │Topic→ │ │ 扫描不活跃│ │ Topic→Queue→     │              │
│  │ 注册/ │ │Broker │ │ Broker并 │ │ Broker 映射表    │              │
│  │ 心跳  │ │ 路由查询│ │ 剔除     │ │ 定期清理过期路由  │              │
│  └──────┘ └──────┘ └──────────┘ └────────┬─────────┘              │
│                                           │                         │
│                                           ▼                         │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  SmartRoutingEngine (智能路由引擎)                             │  │
│  │                                                                │  │
│  │  第一层: GlobalRouter ──▶ ConsistentHashRouter                 │  │
│  │           跨集群路由        160虚拟节点 + MD5哈希               │  │
│  │                                                                │  │
│  │  第二层: ClusterRouter ──▶ BrokerSelector                      │  │
│  │           集群内路由        ROUND_ROBIN | RANDOM |             │  │
│  │                           LEAST_ACTIVE | WEIGHTED_RR |        │  │
│  │                           CONSISTENT_HASH (5种策略)            │  │
│  │                                                                │  │
│  │  第三层: LocalRouter ──▶ QueueSelector + LocalLoadBalancer     │  │
│  │           队列选择          轮询 | 哈希 | 最少负载               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.4 flare-mq-broker — 消息代理服务器

```
┌─────────────────────────────────────────────────────────────────────┐
│                       flare-mq-broker                                │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  BrokerStartup                                               │  │
│  │   - parseArgs (-n -a -c -b -i -r)                           │  │
│  │   - 创建 ClusterManager                                      │  │
│  │   - 注册 ShutdownHook                                        │  │
│  └──────────────────────┬───────────────────────────────────────┘  │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  ClusterManager                                               │  │
│  │   - clusterName / brokerName / clusterConfig                  │  │
│  │   - NettyServer (10911端口)                                    │  │
│  │   - BrokerRegistration → NameServer                            │  │
│  │   - Master 选举 (brokerId 最小优先)                            │  │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │  │
│  │   │ReplicationMgr│  │ FailoverMgr  │  │ LoadBalancer │       │  │
│  │   │ 主从复制      │  │ 故障转移      │  │ 集群负载均衡   │       │  │
│  │   └──────────────┘  └──────────────┘  └──────────────┘       │  │
│  └──────────────────────┬───────────────────────────────────────┘  │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  BrokerRequestHandler implements ServerRequestHandler         │  │
│  │                                                                │  │
│  │  handleRequest() → switch(messageType):                       │  │
│  │   ├─ SEND_MESSAGE_REQUEST → handleSendMessage()               │  │
│  │   │   └─ TopicManager.ensureTopic()                           │  │
│  │   │      QueueManager.selectLeastLoadedQueue()                │  │
│  │   │      DefaultMessageStore.putMessage()                     │  │
│  │   ├─ PULL_MESSAGE_REQUEST → handlePullMessage()               │  │
│  │   │   └─ DefaultMessageStore.getMessage()                     │  │
│  │   ├─ ACK_MESSAGE_REQUEST → handleAckMessage()                 │  │
│  │   ├─ CREATE_TOPIC_REQUEST → handleCreateTopic()               │  │
│  │   └─ QUERY_TOPIC_REQUEST → handleQueryTopic()                 │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ TopicManager │  │ QueueManager │  │ AckManager               │  │
│  │ createTopic  │  │ createQueues │  │ 消费确认追踪              │  │
│  │ topicExists  │  │ selectLeast  │  │ 重试记录 (RetryRecord)    │  │
│  │ getTopicCfg  │  │  LoadedQueue │  │ 死信队列 (DeadLetter)     │  │
│  │ initDefault  │  │              │  │                          │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.5 flare-mq-client — 客户端 SDK

```
┌─────────────────────────────────────────────────────────────────────┐
│                       flare-mq-client                                │
│                                                                     │
│  ┌───────────────────────────────────┐  ┌─────────────────────────┐ │
│  │  ProducerImpl implements Producer │  │  ConsumerImpl impl Consumer│
│  │                                   │  │                          │ │
│  │  config: ProducerConfig           │  │  config: ConsumerConfig  │ │
│  │   · producerGroup                 │  │   · consumerGroup        │ │
│  │   · nameServerAddr (支持多个)      │  │   · consumeMode          │ │
│  │   · sendMsgTimeout=3000ms         │  │   · consumeType (PULL)   │ │
│  │   · retryTimes=2                  │  │   · pullBatchSize=32     │ │
│  │   · maxBatchSize=32               │  │   · consumeThreads=20    │ │
│  │   · compressThreshold=4KB         │  │   · pullInterval=0ms     │ │
│  │                                   │  │   · maxRetryTimes=16     │ │
│  ├───────────────────────────────────┤  ├─────────────────────────┤ │
│  │                                   │  │                          │ │
│  │  nameServerClient (NettyClient)   │  │  nameServerClient        │ │
│  │  brokerClients                    │  │  brokerClients           │ │
│  │   Map<String, NettyClient>        │  │   Map<String, NettyClient>│ │
│  │                                   │  │                          │ │
│  │  topicRouteCache                  │  │  topicRouteCache         │ │
│  │   Map<String, TopicRouteInfo>     │  │   Map<String, TRInfo>    │ │
│  │   5分钟过期                        │  │   5分钟过期               │ │
│  │                                   │  │                          │ │
│  │  callbackExecutor                 │  │  subscriptions           │ │
│  │   ThreadPool(cores, 10000 queue)  │  │   Map<String, SubData>   │ │
│  │                                   │  │                          │ │
│  ├───────────────────────────────────┤  │  consumeExecutor         │ │
│  │  send(Message) → SendResult       │  │   ThreadPool(20~64,1000) │ │
│  │  sendAsync(Message, Callback)     │  │  pullScheduler           │ │
│  │  sendOneway(Message)              │  │   ScheduledPool          │ │
│  │                                   │  │  consumeProgress         │ │
│  │  TopicRouteInfo                   │  │   Map<String, Long>      │ │
│  │   · queueInfos                    │  ├─────────────────────────┤ │
│  │   · brokerInfos                   │  │  subscribe(topic, tags,  │ │
│  │   · selectQueue() 轮询            │  │    listener)             │ │
│  │   · isExpired() 5分钟             │  │  unsubscribe(topic)      │ │
│  │   · BrokerInfo.getMasterAddr()    │  │  pullMessage()           │ │
│  │                                   │  │  ackMessage()            │ │
│  └───────────────────────────────────┘  └─────────────────────────┘ │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Client 通用流程                                               │  │
│  │                                                                │  │
│  │  ① 解析 NameServer 地址列表 (分号分隔)                          │  │
│  │  ② 轮询连接 NameServer (故障转移)                               │  │
│  │  ③ 请求 GET_ROUTEINFO_BY_TOPIC                               │  │
│  │  ④ 解析 TopicRouteInfo → 缓存 5 分钟                           │  │
│  │  ⑤ 根据 BrokerName 获取/创建 NettyClient (连接池复用)           │  │
│  │  ⑥ 发送业务请求 (SEND / PULL / ACK)                           │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

> 本文档对应的源代码位于 `E:\面试讲解\自研消息中间件代码最新\flare-mq`，Java 8 + Maven + Netty + JUnit 5。详细构建和测试命令见 [CLAUDE.md](CLAUDE.md)。
