package com.ruyuan.mq.test.rebalance;

import com.ruyuan.mq.client.consumer.QueueAllocationManager;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;

public class RebalanceTest {

    @Test
    public void testEqualDistribution() {
        // 8 queues, 2 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> queues = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B"));
        assertEquals(Arrays.asList(0, 1, 2, 3), queues);
    }

    @Test
    public void testUnequalDistribution() {
        // 8 queues, 3 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> a = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(0, 1, 2), a); // 8/3 = 2余2, A拿3个

        mgr = new QueueAllocationManager("localhost", 9876, "g", "B", Arrays.asList("t"));
        List<Integer> b = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(3, 4, 5), b); // B拿3个

        mgr = new QueueAllocationManager("localhost", 9876, "g", "C", Arrays.asList("t"));
        List<Integer> c = mgr.calculateMyAllocation("t", 8, Arrays.asList("A", "B", "C"));
        assertEquals(Arrays.asList(6, 7), c); // C拿2个
    }

    @Test
    public void testConsumerMoreThanQueues() {
        // 2 queues, 5 consumers
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "E", Arrays.asList("t"));
        List<Integer> e = mgr.calculateMyAllocation("t", 2, Arrays.asList("A", "B", "C", "D", "E"));
        assertTrue(e.isEmpty()); // 多出来的 consumer 拿不到 queue
    }

    @Test
    public void testSingleConsumer() {
        QueueAllocationManager mgr = new QueueAllocationManager("localhost", 9876, "g", "A", Arrays.asList("t"));
        List<Integer> queues = mgr.calculateMyAllocation("t", 8, Arrays.asList("A"));
        assertEquals(8, queues.size());
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7), queues);
    }
}
