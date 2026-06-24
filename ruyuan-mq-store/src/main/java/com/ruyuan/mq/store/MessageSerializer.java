package com.ruyuan.mq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 消息序列化工具类
 * 
 * 负责消息在CommitLog中的序列化和反序列化
 * 
 * @author RuYuan MQ Team
 */
public class MessageSerializer {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageSerializer.class);
    
    /**
     * 序列化消息到字节数组
     * 
     * 消息格式：
     * TotalSize(4) + MagicCode(4) + BodyCRC(4) + QueueId(4) + Flag(4) + 
     * BornTimestamp(8) + StoreTimestamp(8) + BodyLength(4) + 
     * TopicLength(2) + Topic + TagsLength(2) + Tags + KeysLength(2) + Keys + 
     * PropertiesLength(2) + Properties + Body
     */
    public static byte[] serialize(Message message) {
        if (message == null) {
            return null;
        }
        
        try {
            // 计算各部分的字节数组
            byte[] topicBytes = getStringBytes(message.getTopic());
            byte[] tagsBytes = getStringBytes(message.getTags());
            byte[] keysBytes = getStringBytes(message.getKeys());
            byte[] propertiesBytes = serializeProperties(message.getProperties());
            byte[] bodyBytes = message.getBody() != null ? message.getBody() : new byte[0];
            
            // 计算总长度
            int totalSize = StoreConstants.MESSAGE_MIN_SIZE + 
                           2 + topicBytes.length +    // TopicLength + Topic
                           2 + tagsBytes.length +     // TagsLength + Tags  
                           2 + keysBytes.length +     // KeysLength + Keys
                           2 + propertiesBytes.length + // PropertiesLength + Properties
                           bodyBytes.length;          // Body
            
            // 创建ByteBuffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // 写入消息头
            buffer.putInt(totalSize);                           // TotalSize
            buffer.putInt(StoreConstants.MESSAGE_MAGIC_CODE);   // MagicCode
            
            // 计算Body的CRC32
            CRC32 crc32 = new CRC32();
            crc32.update(bodyBytes);
            buffer.putInt((int) crc32.getValue());              // BodyCRC
            
            buffer.putInt(message.getQueueId());                // QueueId
            buffer.putInt(message.getFlag());                   // Flag
            buffer.putLong(message.getBornTimestamp());         // BornTimestamp
            buffer.putLong(System.currentTimeMillis());         // StoreTimestamp
            buffer.putInt(bodyBytes.length);                    // BodyLength
            
            // 写入Topic
            buffer.putShort((short) topicBytes.length);
            buffer.put(topicBytes);
            
            // 写入Tags
            buffer.putShort((short) tagsBytes.length);
            buffer.put(tagsBytes);
            
            // 写入Keys
            buffer.putShort((short) keysBytes.length);
            buffer.put(keysBytes);
            
            // 写入Properties
            buffer.putShort((short) propertiesBytes.length);
            buffer.put(propertiesBytes);
            
            // 写入Body
            buffer.put(bodyBytes);
            
            // 更新消息的存储信息
            message.setStoreTimestamp(System.currentTimeMillis());
            message.setStoreSize(totalSize);
            
            return buffer.array();
            
        } catch (Exception e) {
            logger.error("序列化消息失败: " + message, e);
            return null;
        }
    }
    
    /**
     * 从字节数组反序列化消息
     */
    public static Message deserialize(byte[] data) {
        return deserialize(data, 0);
    }
    
    /**
     * 从字节数组的指定位置反序列化消息
     */
    public static Message deserialize(byte[] data, int offset) {
        if (data == null || data.length < offset + StoreConstants.MESSAGE_MIN_SIZE) {
            return null;
        }
        
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data, offset, data.length - offset);
            
            // 读取消息头
            int totalSize = buffer.getInt();
            int magicCode = buffer.getInt();
            
            // 验证魔数
            if (magicCode != StoreConstants.MESSAGE_MAGIC_CODE) {
                logger.error("消息魔数不匹配: expected={}, actual={}", 
                           StoreConstants.MESSAGE_MAGIC_CODE, magicCode);
                return null;
            }
            
            int bodyCRC = buffer.getInt();
            int queueId = buffer.getInt();
            int flag = buffer.getInt();
            long bornTimestamp = buffer.getLong();
            long storeTimestamp = buffer.getLong();
            int bodyLength = buffer.getInt();
            
            // 读取Topic
            short topicLength = buffer.getShort();
            String topic = readString(buffer, topicLength);
            
            // 读取Tags
            short tagsLength = buffer.getShort();
            String tags = readString(buffer, tagsLength);
            
            // 读取Keys
            short keysLength = buffer.getShort();
            String keys = readString(buffer, keysLength);
            
            // 读取Properties
            short propertiesLength = buffer.getShort();
            Map<String, String> properties = deserializeProperties(buffer, propertiesLength);
            
            // 读取Body
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            
            // 验证Body的CRC32
            CRC32 crc32 = new CRC32();
            crc32.update(body);
            if (bodyCRC != (int) crc32.getValue()) {
                logger.error("消息Body CRC校验失败");
                return null;
            }
            
            // 创建消息对象
            Message message = new Message();
            message.setTopic(topic);
            message.setTags(tags);
            message.setKeys(keys);
            message.setQueueId(queueId);
            message.setFlag(flag);
            message.setBornTimestamp(bornTimestamp);
            message.setStoreTimestamp(storeTimestamp);
            message.setBody(body);
            message.setProperties(properties);
            message.setStoreSize(totalSize);
            
            return message;
            
        } catch (Exception e) {
            logger.error("反序列化消息失败", e);
            return null;
        }
    }
    
    /**
     * 获取字符串的UTF-8字节数组
     */
    private static byte[] getStringBytes(String str) {
        if (str == null || str.isEmpty()) {
            return new byte[0];
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 从ByteBuffer读取字符串
     */
    private static String readString(ByteBuffer buffer, int length) {
        if (length <= 0) {
            return "";
        }
        
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    /**
     * 序列化Properties
     */
    private static byte[] serializeProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return new byte[0];
        }
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (sb.length() > 0) {
                sb.append('\u0001'); // 使用\u0001作为分隔符
            }
            sb.append(entry.getKey()).append('\u0002').append(entry.getValue());
        }
        
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 反序列化Properties
     */
    private static Map<String, String> deserializeProperties(ByteBuffer buffer, int length) {
        Map<String, String> properties = new HashMap<>();
        
        if (length <= 0) {
            return properties;
        }
        
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String propertiesStr = new String(bytes, StandardCharsets.UTF_8);
        
        String[] pairs = propertiesStr.split("\u0001");
        for (String pair : pairs) {
            String[] kv = pair.split("\u0002");
            if (kv.length == 2) {
                properties.put(kv[0], kv[1]);
            }
        }
        
        return properties;
    }
}
