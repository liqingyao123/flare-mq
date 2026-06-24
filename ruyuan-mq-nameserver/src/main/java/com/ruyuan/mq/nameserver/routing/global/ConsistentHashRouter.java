package com.ruyuan.mq.nameserver.routing.global;

import com.ruyuan.mq.nameserver.routing.model.ClusterInfo;
import com.ruyuan.mq.nameserver.routing.model.RoutableMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性哈希路由器
 * 
 * 用于在集群间进行一致性哈希路由
 * 
 * @author RuYuan MQ Team
 */
public class ConsistentHashRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashRouter.class);
    
    /**
     * 虚拟节点数量，用于提高负载均衡效果
     */
    private static final int VIRTUAL_NODES = 160;
    
    /**
     * 哈希环
     */
    private final SortedMap<Long, ClusterInfo> hashRing = new TreeMap<>();
    
    /**
     * MD5消息摘要
     */
    private MessageDigest md5;
    
    public ConsistentHashRouter() {
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("初始化MD5失败", e);
            throw new RuntimeException("无法初始化MD5", e);
        }
    }
    
    /**
     * 根据消息选择集群
     */
    public ClusterInfo selectCluster(List<ClusterInfo> clusters, RoutableMessage message) {
        if (clusters == null || clusters.isEmpty()) {
            return null;
        }
        
        if (clusters.size() == 1) {
            return clusters.get(0);
        }
        
        // 重建哈希环
        rebuildHashRing(clusters);
        
        // 计算消息的哈希值
        String routingKey = buildRoutingKey(message);
        long hash = hash(routingKey);
        
        // 在哈希环上查找
        ClusterInfo selectedCluster = getClusterFromHashRing(hash);
        
        logger.debug("一致性哈希路由: key={}, hash={}, cluster={}", 
                    routingKey, hash, selectedCluster != null ? selectedCluster.getClusterId() : "null");
        
        return selectedCluster;
    }
    
    /**
     * 重建哈希环
     */
    private void rebuildHashRing(List<ClusterInfo> clusters) {
        hashRing.clear();
        
        for (ClusterInfo cluster : clusters) {
            if (!cluster.isHealthy()) {
                continue;
            }
            
            // 为每个集群创建虚拟节点
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNodeKey = cluster.getClusterId() + "#" + i;
                long hash = hash(virtualNodeKey);
                hashRing.put(hash, cluster);
            }
        }
        
        logger.debug("重建哈希环完成，节点数量: {}", hashRing.size());
    }
    
    /**
     * 从哈希环中获取集群
     */
    private ClusterInfo getClusterFromHashRing(long hash) {
        if (hashRing.isEmpty()) {
            return null;
        }
        
        // 查找第一个大于等于hash值的节点
        SortedMap<Long, ClusterInfo> tailMap = hashRing.tailMap(hash);
        
        // 如果没有找到，则选择第一个节点（环形结构）
        Long targetHash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();
        
        return hashRing.get(targetHash);
    }
    
    /**
     * 构建路由键
     */
    private String buildRoutingKey(RoutableMessage message) {
        // 优先使用消息的Key
        String keys = message.getKeys();
        if (keys != null && !keys.isEmpty()) {
            return keys;
        }
        
        // 其次使用Topic + Tags
        String topic = message.getTopic();
        String tags = message.getTags();
        
        if (tags != null && !tags.isEmpty()) {
            return topic + "#" + tags;
        }
        
        // 最后使用Topic
        return topic;
    }
    
    /**
     * 计算字符串的哈希值
     */
    private long hash(String key) {
        synchronized (md5) {
            md5.reset();
            md5.update(key.getBytes());
            byte[] digest = md5.digest();
            
            // 取前8个字节构造long值
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            
            return hash;
        }
    }
    
    /**
     * 获取哈希环状态信息
     */
    public String getHashRingStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("HashRing Status:\n");
        sb.append("Total Nodes: ").append(hashRing.size()).append("\n");
        
        // 统计每个集群的虚拟节点数量
        TreeMap<String, Integer> clusterNodeCount = new TreeMap<>();
        for (ClusterInfo cluster : hashRing.values()) {
            String clusterId = cluster.getClusterId();
            clusterNodeCount.put(clusterId, clusterNodeCount.getOrDefault(clusterId, 0) + 1);
        }
        
        for (String clusterId : clusterNodeCount.keySet()) {
            sb.append("Cluster ").append(clusterId).append(": ")
              .append(clusterNodeCount.get(clusterId)).append(" virtual nodes\n");
        }
        
        return sb.toString();
    }
}
