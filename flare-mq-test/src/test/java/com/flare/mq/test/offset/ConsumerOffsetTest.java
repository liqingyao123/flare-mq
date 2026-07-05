package com.flare.mq.test.offset;

import com.flare.mq.broker.offset.ConsumerOffsetManager;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;

public class ConsumerOffsetTest {

    private ConsumerOffsetManager manager;
    private File testDir;

    @BeforeEach
    public void setUp() throws Exception {
        testDir = new File(System.getProperty("java.io.tmpdir"), "offset-test-" + System.nanoTime());
        testDir.mkdirs();
        manager = new ConsumerOffsetManager(testDir.getAbsolutePath());
    }

    @AfterEach
    public void tearDown() {
        manager.shutdown();
        for (File f : testDir.listFiles()) f.delete();
        testDir.delete();
    }

    @Test
    public void testUpdateAndGetOffset() {
        manager.updateOffset("group1", "topic1", 0, 100L);
        assertEquals(100L, manager.getOffset("group1", "topic1", 0));
    }

    @Test
    public void testOffsetOnlyMovesForward() {
        manager.updateOffset("group1", "topic1", 0, 100L);
        manager.updateOffset("group1", "topic1", 0, 50L);  // 后退
        assertEquals(100L, manager.getOffset("group1", "topic1", 0));
    }

    @Test
    public void testDefaultOffsetIsZero() {
        assertEquals(0L, manager.getOffset("nonexistent", "topic", 0));
    }

    @Test
    public void testMultipleQueues() {
        manager.updateOffset("g1", "t1", 0, 100L);
        manager.updateOffset("g1", "t1", 1, 200L);
        manager.updateOffset("g2", "t1", 0, 50L);
        assertEquals(100L, manager.getOffset("g1", "t1", 0));
        assertEquals(200L, manager.getOffset("g1", "t1", 1));
        assertEquals(50L, manager.getOffset("g2", "t1", 0));
    }

    @Test
    public void testPersistAndLoad() throws Exception {
        manager.updateOffset("g1", "t1", 0, 42L);
        manager.persistOffsets(); // 刷盘
        manager.shutdown();

        // 重新加载
        ConsumerOffsetManager manager2 = new ConsumerOffsetManager(testDir.getAbsolutePath());
        assertEquals(42L, manager2.getOffset("g1", "t1", 0));
        manager2.shutdown();
    }
}
