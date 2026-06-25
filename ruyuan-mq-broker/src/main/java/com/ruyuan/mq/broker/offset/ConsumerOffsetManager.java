package com.ruyuan.mq.broker.offset;

import com.ruyuan.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;

/**
 * Consumer Offset 管理器 — 按 (consumerGroup, topic, queueId) 存储消费偏移量
 * 定时刷盘到 consumerOffset.json
 */
public class ConsumerOffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetManager.class);

    private final ConcurrentHashMap<String, Long> offsetTable;
    private final ScheduledExecutorService persistScheduler;
    private final File offsetFile;

    public ConsumerOffsetManager(String persistDir) {
        this.offsetTable = new ConcurrentHashMap<>();
        this.offsetFile = new File(persistDir, "consumerOffset.json");
        this.persistScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "OffsetPersist");
            t.setDaemon(true);
            return t;
        });

        loadOffsets();
        startPersistTask();
        logger.info("ConsumerOffsetManager initialized, offsetFile={}", offsetFile.getAbsolutePath());
    }

    /**
     * 更新 offset，只向前推进
     */
    public void updateOffset(String consumerGroup, String topic, int queueId, long offset) {
        String key = buildKey(consumerGroup, topic, queueId);
        offsetTable.merge(key, offset, Math::max);
    }

    /**
     * 查询 offset
     */
    public long getOffset(String consumerGroup, String topic, int queueId) {
        String key = buildKey(consumerGroup, topic, queueId);
        return offsetTable.getOrDefault(key, 0L);
    }

    /**
     * 获取所有 offset（供测试和监控使用）
     */
    public Map<String, Long> getAllOffsets() {
        return new ConcurrentHashMap<>(offsetTable);
    }

    // ===== 持久化 =====

    private void startPersistTask() {
        persistScheduler.scheduleWithFixedDelay(this::persistOffsets, 5, 5, TimeUnit.SECONDS);
    }

    private void persistOffsets() {
        try {
            if (offsetTable.isEmpty()) return;

            String json = JsonUtils.toJson(new ConcurrentHashMap<>(offsetTable));
            if (json == null) return;

            // 先写临时文件再 rename（原子写入）
            File tmpFile = new File(offsetFile.getParentFile(), "consumerOffset.json.tmp");
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(json.getBytes(StandardCharsets.UTF_8));
                fos.flush();
            }
            if (!tmpFile.renameTo(offsetFile)) {
                // rename 失败时直接 copy
                Files.move(tmpFile.toPath(), offsetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }
            logger.debug("Persisted {} offset entries", offsetTable.size());
        } catch (Exception e) {
            logger.error("Failed to persist offsets", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!offsetFile.exists()) {
            logger.info("No existing offset file, starting fresh");
            return;
        }
        try {
            String json = new String(Files.readAllBytes(offsetFile.toPath()), StandardCharsets.UTF_8);
            Map<String, Object> loaded = JsonUtils.fromJson(json, Map.class);
            if (loaded != null) {
                for (Map.Entry<String, Object> entry : loaded.entrySet()) {
                    long value = entry.getValue() instanceof Number
                            ? ((Number) entry.getValue()).longValue() : 0L;
                    offsetTable.put(entry.getKey(), value);
                }
                logger.info("Loaded {} offset entries from {}", offsetTable.size(), offsetFile.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("Failed to load offset file, starting fresh", e);
        }
    }

    // ===== 工具方法 =====

    private String buildKey(String consumerGroup, String topic, int queueId) {
        return consumerGroup + "@" + topic + "@" + queueId;
    }

    public void shutdown() {
        persistOffsets(); // 关闭前最后一次刷盘
        persistScheduler.shutdown();
        try {
            if (!persistScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                persistScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            persistScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("ConsumerOffsetManager shutdown complete");
    }
}
