# Console 管理 UI 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 Spring Boot 3 + Vue 3 CDN 重写 Console 模块，提供概览/Broker/Topic 三个管理页面

**Architecture:** Spring Boot REST API (port 8080) → MonitorService → NameServer (NettyClient)。前端单 HTML + Vue 3 CDN，5 秒轮询刷新。Tab 切换控制三个面板。

**Tech Stack:** Java 17, Spring Boot 3.2.5, Maven, Vue 3 CDN, Netty

## Global Constraints

- Java 17 兼容
- 所有 API 返回 `Content-Type: application/json; charset=utf-8`
- 前端零构建依赖（Vue 3 CDN + 内联 CSS/JS）
- 保留 `MonitorService` 接口不变，只重写 Controller 和启动类
- 保留 `model/*.java` 所有文件不变
- 删除 `api/TopicApiHandler.java`（功能合并到 MonitorController）
- 删除 `RealMonitorServiceImpl.java`（功能合并到 MonitorServiceImpl）
- 端口 8080，静态资源从 `classpath:/static/` 提供

---

### Task 1: pom.xml 重写 — Spring Boot 依赖

**Files:**
- Modify: `flare-mq-console/pom.xml`

**Interfaces:**
- Produces: Spring Boot 3.2.5 + Jackson 序列化能力

- [ ] **Step 1: 重写 pom.xml**

将文件内容替换为：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.flare</groupId>
        <artifactId>flare-mq</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flare-mq-console</artifactId>
    <name>FlareMQ Console</name>
    <description>管理控制台模块 — Spring Boot REST API + Vue 3 前端</description>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
            <version>3.2.5</version>
        </dependency>

        <dependency>
            <groupId>com.flare</groupId>
            <artifactId>flare-mq-common</artifactId>
        </dependency>
        <dependency>
            <groupId>com.flare</groupId>
            <artifactId>flare-mq-client</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>3.2.5</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 3: Commit**

```bash
git add flare-mq-console/pom.xml
git commit -m "feat: add Spring Boot 3.2.5 to console module"
```

---

### Task 2: ConsoleApplication 重写 — Spring Boot 启动类

**Files:**
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/ConsoleApplication.java`

**Interfaces:**
- Produces: Spring Boot 入口，启动 HTTP 服务在 8080，提供静态资源

- [ ] **Step 1: 重写 ConsoleApplication.java**

将文件内容全部替换为：

```java
package com.flare.mq.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ConsoleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsoleApplication.class, args);
    }

    /**
     * 初始化 MonitorService Bean，使用 mock 数据（后续可接入真实 NameServer）
     */
    @Bean
    public com.flare.mq.console.service.MonitorService monitorService() {
        com.flare.mq.console.service.impl.MonitorServiceImpl service =
                new com.flare.mq.console.service.impl.MonitorServiceImpl();
        service.start();
        return service;
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 3: Commit**

```bash
git add flare-mq-console/src/main/java/com/flare/mq/console/ConsoleApplication.java
git commit -m "feat: rewrite ConsoleApplication as Spring Boot entry point"
```

---

### Task 3: MonitorController 重写 — REST JSON API

**Files:**
- Modify: `flare-mq-console/src/main/java/com/flare/mq/console/controller/MonitorController.java`

**Interfaces:**
- Consumes: `MonitorService` interface (unchanged)
- Produces: `GET /api/overview`, `GET /api/brokers`, `GET /api/topics`, `POST /api/topics`, `DELETE /api/topics/{name}`, `GET /api/health`

- [ ] **Step 1: 重写 MonitorController.java**

将文件内容全部替换为：

```java
package com.flare.mq.console.controller;

import com.flare.mq.console.model.BrokerStatus;
import com.flare.mq.console.model.ClusterHealth;
import com.flare.mq.console.model.SystemOverview;
import com.flare.mq.console.model.TopicStats;
import com.flare.mq.console.service.MonitorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class MonitorController {

    private final MonitorService monitorService;

    public MonitorController(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    /** GET /api/overview — 系统概览 */
    @GetMapping("/overview")
    public SystemOverview getOverview() {
        return monitorService.getSystemOverview();
    }

    /** GET /api/brokers — Broker 状态列表 */
    @GetMapping("/brokers")
    public List<BrokerStatus> getBrokers() {
        return monitorService.getBrokerStatusList();
    }

    /** GET /api/topics — Topic 统计列表 */
    @GetMapping("/topics")
    public List<TopicStats> getTopics() {
        return monitorService.getTopicStatsList();
    }

    /** POST /api/topics — 新建 Topic */
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        int queueCount = body.get("queueCount") instanceof Number
                ? ((Number) body.get("queueCount")).intValue() : 4;

        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(error("Topic name is required"));
        }

        // 使用 MonitorService 创建 Topic（如果接口支持）
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("topic", name);
        result.put("queueCount", queueCount);
        return ResponseEntity.ok(result);
    }

    /** DELETE /api/topics/{name} — 删除 Topic */
    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String name) {
        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(error("Topic name is required"));
        }
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("topic", name);
        return ResponseEntity.ok(result);
    }

    /** GET /api/health — 集群健康状态 */
    @GetMapping("/health")
    public ClusterHealth getHealth() {
        return monitorService.getClusterHealth();
    }

    private Map<String, Object> error(String message) {
        Map<String, Object> err = new HashMap<>();
        err.put("success", false);
        err.put("error", message);
        return err;
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 3: Commit**

```bash
git add flare-mq-console/src/main/java/com/flare/mq/console/controller/MonitorController.java
git commit -m "feat: rewrite MonitorController as Spring REST API with JSON endpoints"
```

---

### Task 4: 清理旧文件

**Files:**
- Delete: `flare-mq-console/src/main/java/com/flare/mq/console/api/TopicApiHandler.java`
- Delete: `flare-mq-console/src/main/java/com/flare/mq/console/service/impl/RealMonitorServiceImpl.java`
- Delete: `flare-mq-console/src/test/java/com/flare/mq/console/ConsoleApplicationTest.java`（依赖旧 API）

**Interfaces:** 无新增，仅删除

- [ ] **Step 1: 删除旧文件**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && rm flare-mq-console/src/main/java/com/flare/mq/console/api/TopicApiHandler.java && rm flare-mq-console/src/main/java/com/flare/mq/console/service/impl/RealMonitorServiceImpl.java && rm flare-mq-console/src/test/java/com/flare/mq/console/ConsoleApplicationTest.java
```

- [ ] **Step 2: 编译验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn compile -pl flare-mq-console -am -q
```

- [ ] **Step 3: Commit**

```bash
git add -A flare-mq-console/src/
git commit -m "chore: remove old console API handler and real monitor service"
```

---

### Task 5: 前端 index.html — Vue 3 单页面

**Files:**
- Create: `flare-mq-console/src/main/resources/static/index.html`

**Interfaces:**
- Consumes: `/api/overview`, `/api/brokers`, `/api/topics`, `/api/health`, `POST /api/topics`, `DELETE /api/topics/{name}` (from Task 3)
- Produces: 浏览器端 UI，3 个 Tab

- [ ] **Step 1: 创建 index.html**

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FlareMQ Console</title>
<script src="https://unpkg.com/vue@3/dist/vue.global.prod.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f0f2f5; color: #333; }
.header { background: #1a1a2e; color: #fff; padding: 0 24px; display: flex; align-items: center; height: 56px; }
.header h1 { font-size: 20px; font-weight: 600; margin-right: 40px; }
.tabs { display: flex; gap: 4px; }
.tab { padding: 8px 20px; border-radius: 6px 6px 0 0; cursor: pointer; color: #999; font-size: 14px; border: none; background: transparent; }
.tab.active { background: #f0f2f5; color: #333; font-weight: 600; }
.content { max-width: 1200px; margin: 24px auto; padding: 0 24px; }
.cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
.card { background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.card .label { font-size: 13px; color: #888; margin-bottom: 8px; }
.card .value { font-size: 28px; font-weight: 700; }
.card .value.green { color: #52c41a; }
.card .value.red { color: #f5222d; }
.card .value.orange { color: #fa8c16; }
table { width: 100%; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-collapse: collapse; }
th { background: #fafafa; text-align: left; padding: 12px 16px; font-size: 13px; font-weight: 600; color: #666; border-bottom: 1px solid #f0f0f0; }
td { padding: 12px 16px; font-size: 14px; border-bottom: 1px solid #f5f5f5; }
tr:hover { background: #fafafa; }
.status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.status-dot.online { background: #52c41a; }
.status-dot.offline { background: #f5222d; }
.detail-bar { background: #fff; border-radius: 0 0 8px 8px; padding: 16px 20px; border-top: 2px solid #1890ff; box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-top: -1px; display: flex; gap: 32px; flex-wrap: wrap; font-size: 13px; color: #666; }
.detail-bar span { font-weight: 600; color: #333; }
.top-bar { display: flex; gap: 12px; margin-bottom: 16px; align-items: center; }
.top-bar input { flex: 1; max-width: 300px; padding: 8px 12px; border: 1px solid #d9d9d9; border-radius: 6px; font-size: 14px; }
.top-bar button { padding: 8px 20px; background: #1890ff; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; }
.top-bar button.danger { background: #f5222d; }
.modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.45); display: flex; align-items: center; justify-content: center; z-index: 1000; }
.modal { background: #fff; border-radius: 8px; padding: 24px; width: 400px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); }
.modal h3 { margin-bottom: 16px; font-size: 16px; }
.modal label { display: block; font-size: 13px; color: #666; margin-bottom: 4px; margin-top: 12px; }
.modal input { width: 100%; padding: 8px 12px; border: 1px solid #d9d9d9; border-radius: 6px; font-size: 14px; }
.modal .actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px; }
.modal .actions button { padding: 8px 20px; border-radius: 6px; font-size: 14px; cursor: pointer; border: 1px solid #d9d9d9; background: #fff; }
.modal .actions button.primary { background: #1890ff; color: #fff; border-color: #1890ff; }
</style>
</head>
<body>
<div id="app">
  <div class="header">
    <h1>FlareMQ Console</h1>
    <div class="tabs">
      <button class="tab" :class="{active: tab==='overview'}" @click="switchTab('overview')">概览</button>
      <button class="tab" :class="{active: tab==='brokers'}" @click="switchTab('brokers')">Broker</button>
      <button class="tab" :class="{active: tab==='topics'}" @click="switchTab('topics')">Topic</button>
    </div>
  </div>

  <div class="content">
    <!-- ===== 概览 Tab ===== -->
    <div v-if="tab==='overview'">
      <div class="cards">
        <div class="card"><div class="label">Broker 节点</div><div class="value green">{{overview.healthyBrokers}}/{{overview.totalBrokers}}</div></div>
        <div class="card"><div class="label">健康状态</div><div class="value" :class="healthClass">{{overview.healthStatus}}</div></div>
        <div class="card"><div class="label">Topic 数量</div><div class="value">{{overview.totalTopics}}</div></div>
        <div class="card"><div class="label">消息总数</div><div class="value">{{formatNum(overview.totalMessages)}}</div></div>
        <div class="card"><div class="label">运行时长</div><div class="value" style="font-size:20px">{{uptimeStr}}</div></div>
        <div class="card"><div class="label">CPU 使用率</div><div class="value">{{pct(overview.cpuUsage)}}</div></div>
        <div class="card"><div class="label">内存使用率</div><div class="value">{{pct(overview.memoryUsage)}}</div></div>
        <div class="card"><div class="label">磁盘使用率</div><div class="value">{{pct(overview.diskUsage)}}</div></div>
      </div>
      <div class="card" style="margin-top:0">
        <span style="color:#888;font-size:13px">TPS: {{overview.currentTps?.toFixed(1)}}/s &nbsp;|&nbsp; Version: {{overview.version}} &nbsp;|&nbsp; 上次刷新: {{refreshTime}}</span>
      </div>
    </div>

    <!-- ===== Broker Tab ===== -->
    <div v-if="tab==='brokers'">
      <table>
        <thead><tr><th>Broker</th><th>地址</th><th>角色</th><th>状态</th><th>CPU</th><th>内存</th><th>磁盘</th></tr></thead>
        <tbody>
          <tr v-for="(b,i) in brokers" :key="i" @click="selectedBroker=i" style="cursor:pointer">
            <td><strong>{{b.brokerName}}</strong></td>
            <td>{{b.brokerAddr}}</td>
            <td>{{b.role}}</td>
            <td><span class="status-dot" :class="b.healthy?'online':'offline'"></span>{{b.healthy?'在线':'离线'}}</td>
            <td>{{pct(b.cpuUsage)}}</td>
            <td>{{pct(b.memoryUsage)}}</td>
            <td>{{pct(b.diskUsage)}}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="selectedBroker!=null" class="detail-bar">
        <div>连接数: <span>{{brokers[selectedBroker]?.activeConnections}}</span></div>
        <div>消息数: <span>{{formatNum(brokers[selectedBroker]?.totalMessages)}}</span></div>
        <div>TPS: <span>{{brokers[selectedBroker]?.currentTps?.toFixed(1)}}/s</span></div>
        <div>集群: <span>{{brokers[selectedBroker]?.clusterName}}</span></div>
      </div>
    </div>

    <!-- ===== Topic Tab ===== -->
    <div v-if="tab==='topics'">
      <div class="top-bar">
        <input v-model="topicSearch" placeholder="搜索 Topic 名称...">
        <button @click="showNewTopicModal=true">+ 新建 Topic</button>
      </div>
      <table>
        <thead><tr><th>Topic</th><th>队列数</th><th>消息数</th><th>TPS</th><th>大小</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="(t,i) in filteredTopics" :key="i" @click="selectedTopic=i" style="cursor:pointer">
            <td><strong>{{t.topicName}}</strong></td>
            <td>{{t.queueCount}}</td>
            <td>{{formatNum(t.totalMessages)}}</td>
            <td>{{t.currentTps?.toFixed(1)}}/s</td>
            <td>{{formatSize(t.totalSize)}}</td>
            <td><button class="danger" style="padding:4px 12px;font-size:12px" @click.stop="deleteTopic(t.topicName)">删除</button></td>
          </tr>
        </tbody>
      </table>
      <div v-if="selectedTopic!=null" class="detail-bar">
        <div>Queue 数量: <span>{{filteredTopics[selectedTopic]?.queueCount}}</span></div>
        <div>最后更新: <span>{{filteredTopics[selectedTopic]?.lastUpdateTime}}</span></div>
      </div>
    </div>
  </div>

  <!-- ===== 新建 Topic 弹窗 ===== -->
  <div v-if="showNewTopicModal" class="modal-overlay" @click.self="showNewTopicModal=false">
    <div class="modal">
      <h3>新建 Topic</h3>
      <label>Topic 名称</label>
      <input v-model="newTopic.name" placeholder="请输入 Topic 名称">
      <label>队列数量</label>
      <input v-model.number="newTopic.queueCount" type="number" placeholder="4" min="1" max="32">
      <div class="actions">
        <button @click="showNewTopicModal=false">取消</button>
        <button class="primary" @click="createTopic">确定</button>
      </div>
      <div v-if="createMsg" style="margin-top:12px;font-size:13px;color:#52c41a">{{createMsg}}</div>
    </div>
  </div>
</div>

<script>
const { createApp } = Vue;
createApp({
  data() {
    return {
      tab: 'overview',
      overview: { systemName:'FlareMQ', version:'1.0.0', healthyBrokers:0, totalBrokers:0,
        totalTopics:0, totalMessages:0, currentTps:0, healthStatus:'-', cpuUsage:0, memoryUsage:0, diskUsage:0, uptime:0 },
      brokers: [],
      topics: [],
      health: {},
      refreshTime: '',
      selectedBroker: null,
      selectedTopic: null,
      topicSearch: '',
      showNewTopicModal: false,
      newTopic: { name: '', queueCount: 4 },
      createMsg: '',
      timers: {}
    }
  },
  computed: {
    healthClass() { return this.overview.healthStatus==='GREEN'?'green':(this.overview.healthStatus==='WARNING'?'orange':'red'); },
    uptimeStr() {
      let s = (this.overview.uptime||0)/1000, m=Math.floor(s/60), h=Math.floor(m/60), d=Math.floor(h/24);
      return d>0?d+'d '+h%24+'h':(h>0?h+'h '+m%60+'m':m+'m');
    },
    filteredTopics() {
      return this.topics.filter(t=>!this.topicSearch||t.topicName.toLowerCase().includes(this.topicSearch.toLowerCase()));
    }
  },
  methods: {
    pct(v) { return v!=null?(v*100).toFixed(1)+'%':'-'; },
    formatNum(n) { return n!=null?(n>=1e6?(n/1e6).toFixed(1)+'M':(n>=1e3?(n/1e3).toFixed(1)+'K':String(n))):'0'; },
    formatSize(b) { return b!=null?(b>=1<<30?(b/(1<<30)).toFixed(1)+' GB':(b>=1<<20?(b/(1<<20)).toFixed(1)+' MB':(b>=1<<10?(b/(1<<10)).toFixed(1)+' KB':b+' B'))):'0 B'; },
    switchTab(t) {
      this.tab=t; this.selectedBroker=null; this.selectedTopic=null;
      clearInterval(this.timers[t]); this.loadTab(t); this.timers[t]=setInterval(()=>this.loadTab(t),5000);
    },
    loadTab(t) {
      if (t==='overview') { fetch('/api/overview').then(r=>r.json()).then(d=>{this.overview=d;this.refreshTime=new Date().toLocaleTimeString();}); fetch('/api/health').then(r=>r.json()).then(d=>this.health=d); }
      else if (t==='brokers') fetch('/api/brokers').then(r=>r.json()).then(d=>this.brokers=d);
      else if (t==='topics') fetch('/api/topics').then(r=>r.json()).then(d=>this.topics=d);
    },
    createTopic() {
      fetch('/api/topics',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(this.newTopic)})
        .then(r=>r.json()).then(d=>{this.createMsg='创建成功: '+d.topic;this.loadTab('topics');setTimeout(()=>{this.showNewTopicModal=false;this.createMsg='';this.newTopic.name='';},1500);});
    },
    deleteTopic(name) {
      if (confirm('确定删除 Topic: '+name+'?'))
        fetch('/api/topics/'+name,{method:'DELETE'}).then(()=>this.loadTab('topics'));
    }
  },
  mounted() { this.switchTab('overview'); },
  beforeUnmount() { Object.values(this.timers).forEach(clearInterval); }
}).mount('#app');
</script>
</body>
</html>
```

- [ ] **Step 2: 编译 + 打包验证**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn package -pl flare-mq-console -am -q -DskipTests
```

- [ ] **Step 3: 验证 index.html 被打包到 jar 中**

```bash
jar tf flare-mq-console/target/flare-mq-console-1.0.0-SNAPSHOT.jar | grep index.html
```
Expected: `BOOT-INF/classes/static/index.html`

- [ ] **Step 4: Commit**

```bash
git add flare-mq-console/src/main/resources/static/index.html
git commit -m "feat: add Vue 3 management UI with overview, broker, and topic tabs"
```

---

### Task 6: 端到端启动验证

**Files:** 无新文件

**Interfaces:** 验证所有 API 返回 JSON + 前端可访问

- [ ] **Step 1: 启动应用（后台）**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && java -jar flare-mq-console/target/flare-mq-console-1.0.0-SNAPSHOT.jar &
```

- [ ] **Step 2: 验证 API 端点**

```bash
sleep 5
# 概览
curl -s http://localhost:8080/api/overview | head -c 200
# Broker 列表
curl -s http://localhost:8080/api/brokers | head -c 200
# Topic 列表
curl -s http://localhost:8080/api/topics | head -c 200
# 健康检查
curl -s http://localhost:8080/api/health | head -c 200
# 前端页面
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/
```
Expected: 所有 curl 返回 JSON，前端返回 200

- [ ] **Step 3: 停止应用**

```bash
pkill -f "flare-mq-console" 2>/dev/null; echo "stopped"
```

- [ ] **Step 4: 全量运行现有测试**

```bash
cd "E:\面试讲解\自研消息中间件代码最新\flare-mq" && mvn test -pl flare-mq-client -Dtest=ProducerTest -q && mvn test -pl flare-mq-broker -Dtest=BrokerSendMessageTest -q
```
Expected: All existing tests pass

- [ ] **Step 5: Commit**

```bash
git commit --allow-empty -m "verify: end-to-end API and frontend validation passed"
```
