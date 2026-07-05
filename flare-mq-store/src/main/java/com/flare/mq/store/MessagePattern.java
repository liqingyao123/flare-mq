package com.flare.mq.store;

import java.nio.charset.StandardCharsets;

/**
 * 消息模式分析
 * 
 * 用于分析消息的内容特征，以便选择最适合的压缩算法
 * 
 * @author FlareMQ Team
 */
public class MessagePattern {
    
    /**
     * 消息体
     */
    private final byte[] body;
    
    /**
     * 消息大小
     */
    private final int size;
    
    /**
     * 是否为文本数据
     */
    private Boolean isTextData;
    
    /**
     * 是否为JSON数据
     */
    private Boolean isJsonData;
    
    /**
     * 是否为二进制数据
     */
    private Boolean isBinaryData;
    
    /**
     * 文本重复率
     */
    private Double textRepetitionRate;
    
    /**
     * 构造函数
     */
    public MessagePattern(byte[] body) {
        this.body = body;
        this.size = body != null ? body.length : 0;
    }
    
    /**
     * 是否为文本重度数据
     */
    public boolean isTextHeavy() {
        if (isTextData == null) {
            analyzeTextData();
        }
        return isTextData && getTextRepetitionRate() > 0.3;
    }
    
    /**
     * 是否为JSON数据
     */
    public boolean isJsonData() {
        if (isJsonData == null) {
            analyzeJsonData();
        }
        return isJsonData;
    }
    
    /**
     * 是否为二进制数据
     */
    public boolean isBinaryData() {
        if (isBinaryData == null) {
            analyzeBinaryData();
        }
        return isBinaryData;
    }
    
    /**
     * 获取文本重复率
     */
    public double getTextRepetitionRate() {
        if (textRepetitionRate == null) {
            calculateTextRepetitionRate();
        }
        return textRepetitionRate;
    }
    
    /**
     * 分析是否为文本数据
     */
    private void analyzeTextData() {
        if (body == null || body.length == 0) {
            isTextData = false;
            return;
        }
        
        try {
            // 尝试转换为UTF-8字符串
            String text = new String(body, StandardCharsets.UTF_8);
            
            // 检查是否包含大量可打印字符
            int printableCount = 0;
            for (char c : text.toCharArray()) {
                if (isPrintableChar(c)) {
                    printableCount++;
                }
            }
            
            // 如果可打印字符占比超过80%，认为是文本数据
            double printableRatio = (double) printableCount / text.length();
            isTextData = printableRatio > 0.8;
            
        } catch (Exception e) {
            isTextData = false;
        }
    }
    
    /**
     * 分析是否为JSON数据
     */
    private void analyzeJsonData() {
        if (body == null || body.length == 0) {
            isJsonData = false;
            return;
        }
        
        try {
            String text = new String(body, StandardCharsets.UTF_8).trim();
            
            // 简单检查JSON格式
            boolean startsWithBrace = text.startsWith("{") && text.endsWith("}");
            boolean startsWithBracket = text.startsWith("[") && text.endsWith("]");
            
            if (startsWithBrace || startsWithBracket) {
                // 进一步检查JSON特征
                boolean hasJsonChars = text.contains("\"") && 
                                      (text.contains(":") || text.contains(","));
                isJsonData = hasJsonChars;
            } else {
                isJsonData = false;
            }
            
        } catch (Exception e) {
            isJsonData = false;
        }
    }
    
    /**
     * 分析是否为二进制数据
     */
    private void analyzeBinaryData() {
        if (body == null || body.length == 0) {
            isBinaryData = false;
            return;
        }
        
        // 如果不是文本数据，则认为是二进制数据
        if (isTextData == null) {
            analyzeTextData();
        }
        
        isBinaryData = !isTextData;
    }
    
    /**
     * 计算文本重复率
     */
    private void calculateTextRepetitionRate() {
        if (body == null || body.length == 0) {
            textRepetitionRate = 0.0;
            return;
        }
        
        try {
            String text = new String(body, StandardCharsets.UTF_8);
            
            // 计算字符重复率
            int[] charCount = new int[256];
            for (byte b : body) {
                charCount[b & 0xFF]++;
            }
            
            // 计算重复字符数
            int repeatedChars = 0;
            for (int count : charCount) {
                if (count > 1) {
                    repeatedChars += count - 1;
                }
            }
            
            textRepetitionRate = (double) repeatedChars / body.length;
            
        } catch (Exception e) {
            textRepetitionRate = 0.0;
        }
    }
    
    /**
     * 检查是否为可打印字符
     */
    private boolean isPrintableChar(char c) {
        // ASCII可打印字符范围：32-126
        // 加上常见的空白字符：\t, \n, \r
        return (c >= 32 && c <= 126) || c == '\t' || c == '\n' || c == '\r';
    }
    
    /**
     * 获取压缩建议
     */
    public CompressionRecommendation getCompressionRecommendation() {
        if (size < 1024) {
            // 小于1KB的消息不建议压缩
            return new CompressionRecommendation(CompressionType.NONE, "消息过小，不建议压缩");
        }
        
        if (isJsonData()) {
            return new CompressionRecommendation(CompressionType.SNAPPY, "JSON数据，建议使用Snappy快速压缩");
        } else if (isTextHeavy()) {
            return new CompressionRecommendation(CompressionType.GZIP, "文本数据重复率高，建议使用GZIP高压缩比");
        } else if (isBinaryData()) {
            return new CompressionRecommendation(CompressionType.LZ4, "二进制数据，建议使用LZ4快速压缩");
        } else {
            return new CompressionRecommendation(CompressionType.SNAPPY, "通用数据，建议使用Snappy平衡压缩");
        }
    }
    
    // ========== Getter方法 ==========
    
    public byte[] getBody() {
        return body;
    }
    
    public int getSize() {
        return size;
    }
    
    @Override
    public String toString() {
        return "MessagePattern{" +
                "size=" + size +
                ", isTextData=" + (isTextData != null ? isTextData : "unknown") +
                ", isJsonData=" + (isJsonData != null ? isJsonData : "unknown") +
                ", isBinaryData=" + (isBinaryData != null ? isBinaryData : "unknown") +
                ", textRepetitionRate=" + (textRepetitionRate != null ? String.format("%.2f", textRepetitionRate) : "unknown") +
                '}';
    }
}

/**
 * 压缩类型枚举
 */
enum CompressionType {
    /**
     * 不压缩
     */
    NONE,
    
    /**
     * GZIP压缩 - 高压缩比，适合文本数据
     */
    GZIP,
    
    /**
     * LZ4压缩 - 快速压缩，适合二进制数据
     */
    LZ4,
    
    /**
     * Snappy压缩 - 平衡压缩，适合JSON数据
     */
    SNAPPY
}

/**
 * 压缩建议
 */
class CompressionRecommendation {
    private final CompressionType type;
    private final String reason;
    
    public CompressionRecommendation(CompressionType type, String reason) {
        this.type = type;
        this.reason = reason;
    }
    
    public CompressionType getType() {
        return type;
    }
    
    public String getReason() {
        return reason;
    }
    
    @Override
    public String toString() {
        return "CompressionRecommendation{" +
                "type=" + type +
                ", reason='" + reason + '\'' +
                '}';
    }
}
