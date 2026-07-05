package com.flare.mq.protocol.serialize;

import com.flare.mq.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 序列化工具类
 * 
 * 提供对象与字节数组之间的序列化和反序列化
 * 
 * @author FlareMQ Team
 */
public class SerializeUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(SerializeUtils.class);
    
    /**
     * 序列化类型标识
     */
    public enum SerializeType {
        JSON((byte) 1),
        BINARY((byte) 2);
        
        private final byte code;
        
        SerializeType(byte code) {
            this.code = code;
        }
        
        public byte getCode() {
            return code;
        }
        
        public static SerializeType valueOf(byte code) {
            for (SerializeType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown serialize type: " + code);
        }
    }
    
    /**
     * 序列化对象为字节数组（使用JSON）
     */
    public static byte[] serialize(Object obj) {
        return serialize(obj, SerializeType.JSON);
    }
    
    /**
     * 序列化对象为字节数组
     */
    public static byte[] serialize(Object obj, SerializeType type) {
        if (obj == null) {
            return null;
        }
        
        try {
            switch (type) {
                case JSON:
                    String json = JsonUtils.toJson(obj);
                    if (json == null) {
                        return null;
                    }
                    // 格式：[序列化类型(1字节)] + [JSON字符串]
                    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
                    byte[] result = new byte[1 + jsonBytes.length];
                    result[0] = type.getCode();
                    System.arraycopy(jsonBytes, 0, result, 1, jsonBytes.length);
                    return result;
                    
                case BINARY:
                    // 二进制序列化约定：仅支持 byte[]，格式为 [type(1字节)] + [payload]
                    if (obj instanceof byte[]) {
                        byte[] bytes = (byte[]) obj;
                        byte[] resultBin = new byte[1 + bytes.length];
                        resultBin[0] = type.getCode();
                        System.arraycopy(bytes, 0, resultBin, 1, bytes.length);
                        return resultBin;
                    }
                    throw new IllegalArgumentException("Binary serialization only supports byte[] payload");

                default:
                    throw new IllegalArgumentException("Unsupported serialize type: " + type);
            }
        } catch (Exception e) {
            logger.error("序列化失败: " + obj, e);
            return null;
        }
    }

    /**
     * 反序列化字节数组为对象
     */
    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            // 读取序列化类型
            SerializeType type = SerializeType.valueOf(data[0]);

            switch (type) {
                case JSON:
                    // 提取JSON字符串
                    String json = new String(data, 1, data.length - 1, StandardCharsets.UTF_8);
                    return JsonUtils.fromJson(json, clazz);

                case BINARY:
                    // 二进制反序列化约定：仅支持目标类型为 byte[]
                    if (clazz == byte[].class) {
                        @SuppressWarnings("unchecked")
                        T resultBytes = (T) java.util.Arrays.copyOfRange(data, 1, data.length);
                        return resultBytes;
                    }
                    throw new IllegalArgumentException("Binary deserialization only supports byte[] target class");

                default:
                    throw new IllegalArgumentException("Unsupported serialize type: " + type);
            }
        } catch (Exception e) {
            logger.error("反序列化失败: " + clazz.getName(), e);
            return null;
        }
    }

    /**
     * 序列化字符串
     */
    public static byte[] serializeString(String str) {
        if (str == null) {
            return null;
        }

        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[1 + strBytes.length];
        result[0] = SerializeType.JSON.getCode(); // 字符串使用JSON类型标识
        System.arraycopy(strBytes, 0, result, 1, strBytes.length);
        return result;
    }

    /**
     * 反序列化字符串
     */
    public static String deserializeString(byte[] data) {
        if (data == null || data.length <= 1) {
            return null;
        }

        // 跳过序列化类型标识
        return new String(data, 1, data.length - 1, StandardCharsets.UTF_8);
    }
}
