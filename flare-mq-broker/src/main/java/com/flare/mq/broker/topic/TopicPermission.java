package com.flare.mq.broker.topic;

/**
 * Topic权限常量
 * 
 * @author FlareMQ Team
 */
public class TopicPermission {
    
    /**
     * 无权限
     */
    public static final int NONE = 0;
    
    /**
     * 读权限
     */
    public static final int READ = 1;
    
    /**
     * 写权限
     */
    public static final int WRITE = 2;
    
    /**
     * 读写权限
     */
    public static final int READ_WRITE = READ | WRITE;
    
    /**
     * 只读权限
     */
    public static final int READ_ONLY = READ;
    
    /**
     * 只写权限
     */
    public static final int WRITE_ONLY = WRITE;
    
    /**
     * 检查是否有读权限
     */
    public static boolean hasReadPermission(int permission) {
        return (permission & READ) != 0;
    }
    
    /**
     * 检查是否有写权限
     */
    public static boolean hasWritePermission(int permission) {
        return (permission & WRITE) != 0;
    }
    
    /**
     * 检查是否有读写权限
     */
    public static boolean hasReadWritePermission(int permission) {
        return hasReadPermission(permission) && hasWritePermission(permission);
    }
    
    /**
     * 获取权限描述
     */
    public static String getPermissionDescription(int permission) {
        if (permission == READ_WRITE) {
            return "READ_WRITE";
        } else if (permission == READ_ONLY) {
            return "READ_ONLY";
        } else if (permission == WRITE_ONLY) {
            return "WRITE_ONLY";
        } else {
            return "NONE";
        }
    }
    
    /**
     * 从字符串解析权限
     */
    public static int parsePermission(String permissionStr) {
        if (permissionStr == null || permissionStr.trim().isEmpty()) {
            return NONE;
        }
        
        switch (permissionStr.toUpperCase()) {
            case "READ_WRITE":
            case "RW":
                return READ_WRITE;
            case "READ_ONLY":
            case "R":
                return READ_ONLY;
            case "WRITE_ONLY":
            case "W":
                return WRITE_ONLY;
            case "NONE":
            default:
                return NONE;
        }
    }
    
    /**
     * 验证权限值是否有效
     */
    public static boolean isValidPermission(int permission) {
        return permission >= NONE && permission <= READ_WRITE;
    }
}
