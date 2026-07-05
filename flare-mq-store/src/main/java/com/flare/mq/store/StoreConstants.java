package com.flare.mq.store;

/**
 * 存储常量定义
 * 
 * @author FlareMQ Team
 */
public class StoreConstants {
    
    // ========== 文件相关常量 ==========
    
    /**
     * CommitLog文件大小：1MB (测试环境使用最小值)
     */
    public static final int COMMIT_LOG_FILE_SIZE = 1024 * 1024;
    
    /**
     * ConsumeQueue文件大小：64KB (测试环境使用最小值)
     */
    public static final int CONSUME_QUEUE_FILE_SIZE = 64 * 1024;
    
    /**
     * ConsumeQueue单条记录大小：20字节
     * CommitLogOffset(8) + Size(4) + TagsHashCode(8) = 20
     */
    public static final int CONSUME_QUEUE_UNIT_SIZE = 20;
    
    /**
     * ConsumeQueue单个文件最大记录数：30万条
     */
    public static final int CONSUME_QUEUE_MAX_RECORDS = CONSUME_QUEUE_FILE_SIZE / CONSUME_QUEUE_UNIT_SIZE;
    
    /**
     * 文件名长度：20位数字
     */
    public static final int FILE_NAME_LENGTH = 20;
    
    // ========== 消息相关常量 ==========
    
    /**
     * 消息最小长度：消息头部分
     * TotalSize(4) + MagicCode(4) + BodyCRC(4) + QueueId(4) + Flag(4) + 
     * BornTimestamp(8) + StoreTimestamp(8) + BodyLength(4) = 40字节
     */
    public static final int MESSAGE_MIN_SIZE = 40;
    
    /**
     * 消息最大长度：4MB
     */
    public static final int MESSAGE_MAX_SIZE = 4 * 1024 * 1024;
    
    /**
     * 消息魔数
     */
    public static final int MESSAGE_MAGIC_CODE = 0xAABBCCDD;
    
    // ========== 存储路径常量 ==========
    
    /**
     * 默认存储根目录
     */
    public static final String DEFAULT_STORE_PATH = System.getProperty("user.home") + "/flare-mq-store";
    
    /**
     * CommitLog目录名
     */
    public static final String COMMIT_LOG_DIR = "commitlog";
    
    /**
     * ConsumeQueue目录名
     */
    public static final String CONSUME_QUEUE_DIR = "consumequeue";
    
    /**
     * 索引目录名
     */
    public static final String INDEX_DIR = "index";
    
    // ========== 性能相关常量 ==========
    
    /**
     * 刷盘间隔：1秒
     */
    public static final int FLUSH_INTERVAL_MS = 1000;
    
    /**
     * 批量写入大小：64KB
     */
    public static final int BATCH_WRITE_SIZE = 64 * 1024;
    
    /**
     * 内存映射文件预热大小：1MB
     */
    public static final int MMAP_WARMUP_SIZE = 1024 * 1024;
    
    // ========== 清理相关常量 ==========
    
    /**
     * 文件保留时间：72小时
     */
    public static final long FILE_RESERVED_TIME = 72 * 60 * 60 * 1000L;
    
    /**
     * 磁盘空间警告阈值：85%
     */
    public static final double DISK_SPACE_WARNING_THRESHOLD = 0.85;
    
    /**
     * 磁盘空间清理阈值：90%
     */
    public static final double DISK_SPACE_CLEANUP_THRESHOLD = 0.90;
    
    // ========== 智能存储相关常量 ==========
    
    /**
     * 热度评分更新间隔：5分钟
     */
    public static final long HEAT_SCORE_UPDATE_INTERVAL = 5 * 60 * 1000L;
    
    /**
     * 热存储阈值：热度评分 > 8.0
     */
    public static final double HOT_STORAGE_THRESHOLD = 8.0;
    
    /**
     * 温存储阈值：热度评分 4.0-8.0
     */
    public static final double WARM_STORAGE_THRESHOLD = 4.0;
    
    /**
     * 冷存储阈值：热度评分 < 4.0
     */
    public static final double COLD_STORAGE_THRESHOLD = 4.0;
    
    /**
     * 存储层级迁移检查间隔：30分钟
     */
    public static final long STORAGE_MIGRATION_CHECK_INTERVAL = 30 * 60 * 1000L;
}
