package com.flare.mq.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CommitLogManager / MappedFile 并发写入测试
 *
 * 针对"多个线程同时向同一 MappedFile 追加消息"的场景：
 * 1. testConcurrentAppendOffsetsAreUniqueAndReadable
 *    - 并发向 CommitLogManager 追加消息，断言所有消息的 commitLogOffset 唯一（无重复）
 *    - 按 offset 读回，断言内容与写入时完全一致（无串号、无损坏）
 *    修复前，CommitLogManager 在锁外读取 wrotePosition 计算 offset，
 *    两个并发线程会记录到相同 offset，导致唯一性断言失败。
 * 2. testMappedFileConcurrentAppendReturnsUniquePositions
 *    - 直接验证 MappedFile.appendMessage 并发返回的写入位置唯一且连续，
 *      锁定新接口"成功返回实际写入位置"的契约。
 */
class CommitLogManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(CommitLogManagerTest.class);

    private String testStorePath;
    private CommitLogManager manager;

    @BeforeEach
    void setUp() throws Exception {
        testStorePath = System.getProperty("java.io.tmpdir") + File.separator
                + "commitlog-concurrent-" + System.currentTimeMillis();
        Files.createDirectories(Paths.get(testStorePath));
        manager = new CommitLogManager(testStorePath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager != null) {
            manager.shutdown();
        }
        deleteDirectory(Paths.get(testStorePath));
    }

    @Test
    void testConcurrentAppendOffsetsAreUniqueAndReadable() throws Exception {
        int threadCount = 8;
        int messagesPerThread = 200;
        int totalMessages = threadCount * messagesPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger duplicateCount = new AtomicInteger(0);

        List<AppendMessageResult> results = Collections.synchronizedList(new ArrayList<>());
        Set<Long> seenOffsets = Collections.synchronizedSet(new HashSet<>());
        Map<Long, byte[]> expectedBodies = Collections.synchronizedMap(new HashMap<>());

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < messagesPerThread; i++) {
                        byte[] body = ("body-" + threadId + "-" + i).getBytes(StandardCharsets.UTF_8);
                        Message message = new Message();
                        message.setTopic("topic-" + threadId);
                        message.setQueueId(0);
                        message.setTags("tag-" + threadId);
                        message.setKeys("key-" + threadId + "-" + i);
                        message.setBody(body);

                        AppendMessageResult result = manager.appendMessage(message);
                        if (result.getStatus() != AppendMessageStatus.SUCCESS) {
                            errorCount.incrementAndGet();
                            continue;
                        }
                        results.add(result);
                        expectedBodies.put(result.getWroteOffset(), body);
                        if (!seenOffsets.add(result.getWroteOffset())) {
                            duplicateCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    logger.error("线程 {} 执行异常", threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        assertEquals(0, errorCount.get(), "所有消息都应写入成功");
        assertEquals(0, duplicateCount.get(),
                "偏移量必须唯一：并发写入时两条消息被记录到同一 offset，即发生索引错位");
        assertEquals(totalMessages, results.size(), "消息总数应正确");
        // 全部消息可放入单个文件（1MB），冷启动并发不应各自新建文件（文件风暴）
        assertEquals(1, manager.getMappedFileCount(),
                "冷启动并发写入不应创建多个文件（文件风暴）：数据可放入单个文件时，应只创建一个 CommitLog 文件");

        // 每条消息按 offset 读回，内容必须与写入时完全一致（无串号、无损坏）
        for (AppendMessageResult result : results) {
            Message readMessage = manager.getMessage(result.getWroteOffset(), result.getWroteBytes());
            assertNotNull(readMessage, "消息应可读回: offset=" + result.getWroteOffset());
            assertArrayEquals(expectedBodies.get(result.getWroteOffset()), readMessage.getBody(),
                    "消息体应与写入一致: offset=" + result.getWroteOffset());
        }
    }

    @Test
    void testMappedFileConcurrentAppendReturnsUniquePositions() throws Exception {
        Path dir = Files.createTempDirectory("mappedfile-concurrent-");
        File file = dir.resolve("00000000000000000000").toFile();
        MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), 1024 * 1024);

        try {
            int threadCount = 8;
            int messagesPerThread = 200;
            int totalMessages = threadCount * messagesPerThread;

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            List<Long> positions = Collections.synchronizedList(new ArrayList<>());

            for (int t = 0; t < threadCount; t++) {
                final byte[] data = ("data-" + t).getBytes(StandardCharsets.UTF_8);
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < messagesPerThread; i++) {
                            long pos = mappedFile.appendMessage(data);
                            if (pos >= 0) {
                                positions.add(pos);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("MappedFile 并发写入异常", e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await();
            executor.shutdown();

            // 每个线程写入的数据长度相同，位置必须连续且唯一
            int dataLength = ("data-0").getBytes(StandardCharsets.UTF_8).length;
            assertEquals(totalMessages, positions.size(), "所有写入都应成功");
            assertEquals(totalMessages, new HashSet<>(positions).size(), "写入位置必须唯一，不允许重叠");

            List<Long> sorted = new ArrayList<>(positions);
            sorted.sort(Comparator.naturalOrder());
            long expected = sorted.get(0);
            for (long pos : sorted) {
                assertEquals(expected, pos, "写入位置应连续无重叠");
                expected += dataLength;
            }
        } finally {
            mappedFile.cleanup();
            deleteDirectory(dir);
        }
    }

    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                            logger.warn("删除文件失败: " + p, e);
                        }
                    });
        }
    }
}
