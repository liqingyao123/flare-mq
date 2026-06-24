package com.ruyuan.mq.broker.ack;

/**
 * 重试级别枚举
 * 
 * @author RuYuan MQ Team
 */
public enum RetryLevel {
    
    /**
     * 普通级别 - 重试次数 1-3
     */
    NORMAL("NORMAL", "普通", 1, 3),
    
    /**
     * 高级别 - 重试次数 4-8
     */
    HIGH("HIGH", "高", 4, 8),
    
    /**
     * 严重级别 - 重试次数 9+
     */
    CRITICAL("CRITICAL", "严重", 9, Integer.MAX_VALUE);
    
    private final String code;
    private final String description;
    private final int minRetryCount;
    private final int maxRetryCount;
    
    RetryLevel(String code, String description, int minRetryCount, int maxRetryCount) {
        this.code = code;
        this.description = description;
        this.minRetryCount = minRetryCount;
        this.maxRetryCount = maxRetryCount;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    public int getMinRetryCount() {
        return minRetryCount;
    }
    
    public int getMaxRetryCount() {
        return maxRetryCount;
    }
    
    /**
     * 根据重试次数获取重试级别
     */
    public static RetryLevel fromRetryCount(int retryCount) {
        for (RetryLevel level : values()) {
            if (retryCount >= level.minRetryCount && retryCount <= level.maxRetryCount) {
                return level;
            }
        }
        return NORMAL;
    }
    
    /**
     * 根据代码获取重试级别
     */
    public static RetryLevel fromCode(String code) {
        if (code == null) {
            return NORMAL;
        }
        
        for (RetryLevel level : values()) {
            if (level.code.equalsIgnoreCase(code)) {
                return level;
            }
        }
        
        return NORMAL;
    }
    
    /**
     * 检查是否为普通级别
     */
    public boolean isNormal() {
        return this == NORMAL;
    }
    
    /**
     * 检查是否为高级别
     */
    public boolean isHigh() {
        return this == HIGH;
    }
    
    /**
     * 检查是否为严重级别
     */
    public boolean isCritical() {
        return this == CRITICAL;
    }
    
    /**
     * 获取重试延迟时间（毫秒）
     */
    public long getRetryDelay(int retryCount) {
        switch (this) {
            case NORMAL:
                // 普通级别：1s, 2s, 4s
                return Math.min(1000L * (1L << (retryCount - 1)), 4000L);
            case HIGH:
                // 高级别：8s, 16s, 32s, 60s, 60s
                return Math.min(8000L * (1L << (retryCount - 4)), 60000L);
            case CRITICAL:
                // 严重级别：固定60s
                return 60000L;
            default:
                return 1000L;
        }
    }
    
    /**
     * 获取优先级（数字越大优先级越高）
     */
    public int getPriority() {
        switch (this) {
            case NORMAL:
                return 1;
            case HIGH:
                return 2;
            case CRITICAL:
                return 3;
            default:
                return 0;
        }
    }
    
    @Override
    public String toString() {
        return code + "(" + description + ")";
    }
}
