# Console 管理 UI 设计

2026-07-04 | MQ 管理控制台 | 阶段一：概览 + Broker + Topic

## 背景

Console 模块当前只有后端 API：
- `MonitorController` 返回纯文本报告（`toString()` 格式），无法被前端消费
- `TopicApiHandler` 提供 Topic CRUD REST API（JSON 格式），但无前端
- `ConsoleApplication` 使用 `com.sun.net.httpserver` 内置 HTTP 服务
- 缺少任何形式的管理页面

## 目标

- 用 Spring Boot 全量重写 Console 模块，替换现有实现
- 提供 3 个管理页面（Tab 切换）：系统概览、Broker 状态、Topic 统计
- 后端返回 JSON，前端单 HTML 页面 + Vue 3 CDN + 5 秒轮询
- 支持 Topic 新建和删除

## 架构

```
浏览器 (index.html + Vue 3 CDN)
  │ fetch /api/* JSON, 5s 轮询
  ▼
Spring Boot (port 8080)
  ├── /api/overview      → SystemOverview JSON
  ├── /api/brokers       → List<BrokerStatus> JSON
  ├── /api/topics        → GET: List<TopicStats> JSON, POST: create topic
  ├── /api/topics/{name} → DELETE: delete topic
  └── /api/health        → ClusterHealth JSON
  │
  ▼
NameServer (port 9876)  ← 通过 NettyClient 查询路由和 Broker 信息
```

### 依赖

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
    <version>2.7.18</version>
</dependency>
```

Spring Boot 2.7.x 兼容 Java 8，是最后一个支持 Java 8 的大版本。

## 后端

### 项目结构

```
flare-mq-console/src/main/java/com/flare/mq/console/
├── ConsoleApplication.java        # Spring Boot 启动类（重写）
├── controller/
│   └── MonitorController.java     # REST JSON API（重写）
├── service/
│   ├── MonitorService.java        # 接口（保留）
│   └── impl/
│       └── MonitorServiceImpl.java # 实现（重写，通过 NettyClient 查 NameServer）
└── model/
    ├── SystemOverview.java        # 保留，新增 JSON 序列化
    ├── BrokerStatus.java          # 保留
    ├── TopicStats.java            # 保留
    ├── ConsumerGroupStatus.java   # 保留
    ├── ClusterHealth.java         # 保留
    ├── PerformanceMetrics.java    # 保留
    ├── TpsStatistics.java         # 保留
    ├── StorageStatistics.java     # 保留
    └── SystemAlert.java           # 保留
```

### API 设计

所有响应 `Content-Type: application/json; charset=utf-8`。

**GET /api/overview**
```json
{
  "systemName": "FlareMQ",
  "version": "1.0.0",
  "uptime": 453200000,
  "totalBrokers": 3,
  "healthyBrokers": 3,
  "totalTopics": 12,
  "totalMessages": 1200000,
  "currentTps": 1523.5,
  "healthStatus": "GREEN",
  "cpuUsage": 0.23,
  "memoryUsage": 0.45,
  "diskUsage": 0.32
}
```

**GET /api/brokers**
```json
[
  {
    "brokerName": "broker-a",
    "brokerAddr": "10.0.0.1:10911",
    "clusterName": "default",
    "role": "Master",
    "status": "ONLINE",
    "healthy": true,
    "cpuUsage": 0.23,
    "memoryUsage": 0.45,
    "diskUsage": 0.32,
    "activeConnections": 128,
    "totalMessages": 45000,
    "currentTps": 850.0
  }
]
```

**GET /api/topics**
```json
[
  {
    "topicName": "order-topic",
    "queueCount": 8,
    "totalMessages": 500000,
    "currentTps": 320.0,
    "totalSize": 13107200,
    "lastUpdateTime": "2026-07-04T10:30:15"
  }
]
```

**POST /api/topics** (body: `{"name":"xxx","queueCount":4}`)
```json
{"success":true,"topic":"xxx","queueCount":4}
```

**DELETE /api/topics/{name}**
```json
{"success":true,"topic":"xxx"}
```

**GET /api/health**
```json
{
  "overallStatus": "GREEN",
  "totalNodes": 3,
  "healthyNodes": 3,
  "masterBroker": "broker-a",
  "activeConnections": 256,
  "lastCheckTime": "2026-07-04T10:30:10"
}
```

## 前端

### 技术选型

- Vue 3 CDN（`unpkg.com/vue@3`），无需构建
- 纯 CSS（内联 `<style>`），无 UI 框架依赖
- 5 秒自动轮询 `setInterval(fetch, 5000)`

### 页面结构

```
┌──────────────────────────────────────────────┐
│  FlareMQ Console    [概览] [Broker] [Topic] │
├──────────────────────────────────────────────┤
│                                              │
│         Tab 内容区（v-if 控制显示）            │
│                                              │
└──────────────────────────────────────────────┘
```

### Tab 1: 概览

4 个集群状态卡片 + 3 个资源卡片 + 底部信息条。

数据源：`GET /api/overview`，`GET /api/health`

### Tab 2: Broker

Broker 列表表格 + 点击展开详情（连接数、消息数、TPS、集群名）。

数据源：`GET /api/brokers`

### Tab 3: Topic

Topic 列表表格 + 搜索过滤 + 新建按钮（弹出对话框）+ 点击展开 Queue 分布。

数据源：`GET /api/topics`，`POST /api/topics`，`DELETE /api/topics/{name}`

### 刷新策略

每个 Tab 独立在 `onMounted` 时初次加载 + `setInterval(5000)` 轮询。切换 Tab 时立即拉取一次最新数据。离开 Tab 时 `clearInterval` 停止轮询，减少不必要的请求。

## 不在此次范围

- 消费者组、性能指标、TPS 统计、存储统计、系统告警页面（阶段二/三）
- WebSocket 实时推送
- 图表（ECharts）
- 用户认证/登录
- 国际化

## 文件清单

| 文件 | 动作 | 说明 |
|------|------|------|
| `pom.xml` | 重写 | 添加 spring-boot-starter-web 依赖 |
| `ConsoleApplication.java` | 重写 | Spring Boot 启动类 + 静态资源配置 |
| `MonitorController.java` | 重写 | REST JSON API，替换纯文本返回 |
| `MonitorServiceImpl.java` | 重写 | 通过 NettyClient 从 NameServer 查询真实数据 |
| `model/*.java` | 保留+调整 | 确保 Jackson 序列化正确 |
| `src/main/resources/static/index.html` | 新增 | 前端单页面 |
| 删除 `ConsoleApplication.java` 原实现 | 删除 | 被 Spring Boot 版本替换 |
| 删除 `api/TopicApiHandler.java` | 删除 | Topic API 合并到 MonitorController |
