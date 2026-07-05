package com.flare.mq.protocol.zerocopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DirectBuffer pool manager
 *
 * Implements efficient DirectBuffer pooling mechanism to reduce GC pressure and improve network transmission performance
 *
 * @author FlareMQ Team
 */
public class DirectBufferPool {

    private static final Logger logger = LoggerFactory.getLogger(DirectBufferPool.class);

    /**
     * Buffer pools of different sizes
     */
    private final ConcurrentLinkedQueue<ByteBuffer>[] pools;

    /**
     * Buffer size configuration
     */
    private final int[] sizes = {1024, 4096, 16384, 65536}; // 1KB, 4KB, 16KB, 64KB

    /**
     * Maximum capacity of each pool
     */
    private final int maxPoolSize;

    /**
     * Statistics
     */
    private final AtomicLong totalAllocated = new AtomicLong(0);
    private final AtomicLong totalReleased = new AtomicLong(0);
    private final AtomicLong poolHits = new AtomicLong(0);
    private final AtomicLong poolMisses = new AtomicLong(0);

    /**
     * Singleton instance
     */
    private static volatile DirectBufferPool instance;

    /**
     * Private constructor
     */
    @SuppressWarnings("unchecked")
    private DirectBufferPool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        this.pools = new ConcurrentLinkedQueue[sizes.length];
        
        // Initialize buffer pools of different sizes
        for (int i = 0; i < sizes.length; i++) {
            pools[i] = new ConcurrentLinkedQueue<>();

            // Pre-allocate some buffers
            int preAllocateCount = Math.min(maxPoolSize / 4, 10);
            for (int j = 0; j < preAllocateCount; j++) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(sizes[i]);
                pools[i].offer(buffer);
            }

            logger.info("Initialized DirectBuffer pool: size={}KB, preAllocated={}",
                       sizes[i] / 1024, preAllocateCount);
        }
    }

    /**
     * Get singleton instance
     */
    public static DirectBufferPool getInstance() {
        if (instance == null) {
            synchronized (DirectBufferPool.class) {
                if (instance == null) {
                    instance = new DirectBufferPool(100); // Default max 100 buffers per pool
                }
            }
        }
        return instance;
    }

    /**
     * Get DirectBuffer of specified size
     */
    public ByteBuffer acquire(int size) {
        int poolIndex = findPoolIndex(size);
        
        if (poolIndex >= 0) {
            // Get from corresponding pool
            ByteBuffer buffer = pools[poolIndex].poll();
            if (buffer != null) {
                buffer.clear(); // Reset position and limit
                poolHits.incrementAndGet();
                logger.debug("Got DirectBuffer from pool: size={}, poolIndex={}", sizes[poolIndex], poolIndex);
                return buffer;
            }
        }

        // No available buffer in pool, create new one
        int actualSize = poolIndex >= 0 ? sizes[poolIndex] : size;
        ByteBuffer buffer = ByteBuffer.allocateDirect(actualSize);
        totalAllocated.incrementAndGet();
        poolMisses.incrementAndGet();

        logger.debug("Created new DirectBuffer: requestedSize={}, actualSize={}", size, actualSize);
        return buffer;
    }

    /**
     * Release DirectBuffer back to pool
     */
    public void release(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        
        int capacity = buffer.capacity();
        int poolIndex = findExactPoolIndex(capacity);
        
        if (poolIndex >= 0 && pools[poolIndex].size() < maxPoolSize) {
            buffer.clear(); // Reset state
            pools[poolIndex].offer(buffer);
            totalReleased.incrementAndGet();
            logger.debug("Released DirectBuffer back to pool: size={}, poolIndex={}", capacity, poolIndex);
        } else {
            // Pool is full or size mismatch, let GC reclaim
            logger.debug("DirectBuffer not returned to pool: size={}, poolIndex={}, poolSize={}",
                        capacity, poolIndex, poolIndex >= 0 ? pools[poolIndex].size() : -1);
        }
    }

    /**
     * Find suitable pool index (round up)
     */
    private int findPoolIndex(int size) {
        for (int i = 0; i < sizes.length; i++) {
            if (size <= sizes[i]) {
                return i;
            }
        }
        return -1; // Exceeds maximum pool size
    }

    /**
     * Find exact matching pool index
     */
    private int findExactPoolIndex(int size) {
        for (int i = 0; i < sizes.length; i++) {
            if (size == sizes[i]) {
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Get pool statistics
     */
    public PoolStatistics getStatistics() {
        PoolStatistics stats = new PoolStatistics();
        stats.totalAllocated = totalAllocated.get();
        stats.totalReleased = totalReleased.get();
        stats.poolHits = poolHits.get();
        stats.poolMisses = poolMisses.get();
        stats.hitRate = stats.poolHits + stats.poolMisses > 0 ?
                       (double) stats.poolHits / (stats.poolHits + stats.poolMisses) : 0.0;

        // Count current size of each pool
        stats.poolSizes = new int[sizes.length];
        for (int i = 0; i < sizes.length; i++) {
            stats.poolSizes[i] = pools[i].size();
        }

        return stats;
    }

    /**
     * Clean up all pools
     */
    public void cleanup() {
        logger.info("Starting to cleanup DirectBuffer pools...");

        for (int i = 0; i < pools.length; i++) {
            ConcurrentLinkedQueue<ByteBuffer> pool = pools[i];
            int cleared = 0;

            ByteBuffer buffer;
            while ((buffer = pool.poll()) != null) {
                // DirectBuffer will be reclaimed by GC automatically
                cleared++;
            }

            logger.info("Cleaned DirectBuffer pool: size={}KB, cleared={}", sizes[i] / 1024, cleared);
        }

        // Reset statistics
        totalAllocated.set(0);
        totalReleased.set(0);
        poolHits.set(0);
        poolMisses.set(0);

        logger.info("DirectBuffer pool cleanup completed");
    }
    
    /**
     * Pool statistics
     */
    public static class PoolStatistics {
        public long totalAllocated;
        public long totalReleased;
        public long poolHits;
        public long poolMisses;
        public double hitRate;
        public int[] poolSizes;
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DirectBufferPool Statistics:\n");
            sb.append("  Total Allocated: ").append(totalAllocated).append("\n");
            sb.append("  Total Released: ").append(totalReleased).append("\n");
            sb.append("  Pool Hits: ").append(poolHits).append("\n");
            sb.append("  Pool Misses: ").append(poolMisses).append("\n");
            sb.append("  Hit Rate: ").append(String.format("%.2f%%", hitRate * 100)).append("\n");
            sb.append("  Pool Sizes: ");
            if (poolSizes != null) {
                for (int i = 0; i < poolSizes.length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(poolSizes[i]);
                }
            }
            return sb.toString();
        }
    }
}
