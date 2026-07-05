package com.flare.mq.console.controller;

import com.flare.mq.console.model.BrokerStatus;
import com.flare.mq.console.model.ClusterHealth;
import com.flare.mq.console.model.ConsumerGroupStatus;
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

        boolean ok = monitorService.createTopic(name, queueCount);
        Map<String, Object> result = new HashMap<>();
        result.put("success", ok);
        result.put("topic", name);
        result.put("queueCount", queueCount);
        if (!ok) {
            result.put("error", "Failed to create topic via NameServer");
        }
        return ResponseEntity.ok(result);
    }

    /** DELETE /api/topics/{name} — 删除 Topic */
    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String name) {
        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(error("Topic name is required"));
        }

        boolean ok = monitorService.deleteTopic(name);
        Map<String, Object> result = new HashMap<>();
        result.put("success", ok);
        result.put("topic", name);
        if (!ok) {
            result.put("error", "Failed to delete topic via NameServer");
        }
        return ResponseEntity.ok(result);
    }

    /** GET /api/health — 集群健康状态 */
    @GetMapping("/health")
    public ClusterHealth getHealth() {
        return monitorService.getClusterHealth();
    }

    /** GET /api/consumers — 消费者组状态列表 */
    @GetMapping("/consumers")
    public List<ConsumerGroupStatus> getConsumerGroups() {
        return monitorService.getConsumerGroupStatusList();
    }

    private Map<String, Object> error(String message) {
        Map<String, Object> err = new HashMap<>();
        err.put("success", false);
        err.put("error", message);
        return err;
    }
}
