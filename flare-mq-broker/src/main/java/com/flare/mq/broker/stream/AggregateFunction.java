package com.flare.mq.broker.stream;

import com.flare.mq.store.Message;

import java.util.List;

/**
 * 聚合函数接口
 * 
 * @author FlareMQ Team
 */
public interface AggregateFunction {
    
    /**
     * 聚合窗口中的消息
     * 
     * @param messages 窗口中的消息列表
     * @return 聚合结果消息
     */
    Message aggregate(List<Message> messages);
    
    /**
     * 获取聚合函数名称
     */
    String getName();
}

/**
 * 计数聚合函数
 */
class CountAggregateFunction implements AggregateFunction {
    
    @Override
    public Message aggregate(List<Message> messages) {
        int count = messages.size();
        
        // 创建聚合结果消息
        Message result = new Message();
        result.setTopic("aggregated");
        result.setTags("count");
        result.setKeys("count_result");
        result.setBody(String.valueOf(count).getBytes());
        result.setBornTimestamp(System.currentTimeMillis());
        
        // 添加聚合元数据
        result.putProperty("aggregate_type", "count");
        result.putProperty("message_count", String.valueOf(count));
        result.putProperty("window_start", String.valueOf(getWindowStart(messages)));
        result.putProperty("window_end", String.valueOf(getWindowEnd(messages)));
        
        return result;
    }
    
    @Override
    public String getName() {
        return "count";
    }
    
    private long getWindowStart(List<Message> messages) {
        return messages.stream()
                .mapToLong(Message::getBornTimestamp)
                .min()
                .orElse(System.currentTimeMillis());
    }
    
    private long getWindowEnd(List<Message> messages) {
        return messages.stream()
                .mapToLong(Message::getBornTimestamp)
                .max()
                .orElse(System.currentTimeMillis());
    }
}

/**
 * 求和聚合函数
 */
class SumAggregateFunction implements AggregateFunction {
    
    private final String fieldName;
    
    public SumAggregateFunction(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public Message aggregate(List<Message> messages) {
        double sum = 0.0;
        int validCount = 0;
        
        for (Message message : messages) {
            String value = message.getProperty(fieldName);
            if (value != null) {
                try {
                    sum += Double.parseDouble(value);
                    validCount++;
                } catch (NumberFormatException e) {
                    // 忽略无法解析的值
                }
            }
        }
        
        // 创建聚合结果消息
        Message result = new Message();
        result.setTopic("aggregated");
        result.setTags("sum");
        result.setKeys("sum_result");
        result.setBody(String.valueOf(sum).getBytes());
        result.setBornTimestamp(System.currentTimeMillis());
        
        // 添加聚合元数据
        result.putProperty("aggregate_type", "sum");
        result.putProperty("field_name", fieldName);
        result.putProperty("sum_value", String.valueOf(sum));
        result.putProperty("valid_count", String.valueOf(validCount));
        result.putProperty("total_count", String.valueOf(messages.size()));
        
        return result;
    }
    
    @Override
    public String getName() {
        return "sum(" + fieldName + ")";
    }
}

/**
 * 平均值聚合函数
 */
class AvgAggregateFunction implements AggregateFunction {
    
    private final String fieldName;
    
    public AvgAggregateFunction(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public Message aggregate(List<Message> messages) {
        double sum = 0.0;
        int validCount = 0;
        
        for (Message message : messages) {
            String value = message.getProperty(fieldName);
            if (value != null) {
                try {
                    sum += Double.parseDouble(value);
                    validCount++;
                } catch (NumberFormatException e) {
                    // 忽略无法解析的值
                }
            }
        }
        
        double average = validCount > 0 ? sum / validCount : 0.0;
        
        // 创建聚合结果消息
        Message result = new Message();
        result.setTopic("aggregated");
        result.setTags("avg");
        result.setKeys("avg_result");
        result.setBody(String.valueOf(average).getBytes());
        result.setBornTimestamp(System.currentTimeMillis());
        
        // 添加聚合元数据
        result.putProperty("aggregate_type", "avg");
        result.putProperty("field_name", fieldName);
        result.putProperty("avg_value", String.valueOf(average));
        result.putProperty("sum_value", String.valueOf(sum));
        result.putProperty("valid_count", String.valueOf(validCount));
        result.putProperty("total_count", String.valueOf(messages.size()));
        
        return result;
    }
    
    @Override
    public String getName() {
        return "avg(" + fieldName + ")";
    }
}

/**
 * 最大值聚合函数
 */
class MaxAggregateFunction implements AggregateFunction {
    
    private final String fieldName;
    
    public MaxAggregateFunction(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public Message aggregate(List<Message> messages) {
        double max = Double.MIN_VALUE;
        boolean hasValue = false;
        
        for (Message message : messages) {
            String value = message.getProperty(fieldName);
            if (value != null) {
                try {
                    double numValue = Double.parseDouble(value);
                    if (!hasValue || numValue > max) {
                        max = numValue;
                        hasValue = true;
                    }
                } catch (NumberFormatException e) {
                    // 忽略无法解析的值
                }
            }
        }
        
        if (!hasValue) {
            max = 0.0;
        }
        
        // 创建聚合结果消息
        Message result = new Message();
        result.setTopic("aggregated");
        result.setTags("max");
        result.setKeys("max_result");
        result.setBody(String.valueOf(max).getBytes());
        result.setBornTimestamp(System.currentTimeMillis());
        
        // 添加聚合元数据
        result.putProperty("aggregate_type", "max");
        result.putProperty("field_name", fieldName);
        result.putProperty("max_value", String.valueOf(max));
        result.putProperty("has_value", String.valueOf(hasValue));
        result.putProperty("total_count", String.valueOf(messages.size()));
        
        return result;
    }
    
    @Override
    public String getName() {
        return "max(" + fieldName + ")";
    }
}

/**
 * 聚合函数工厂
 */
class AggregateFunctions {
    
    public static AggregateFunction count() {
        return new CountAggregateFunction();
    }
    
    public static AggregateFunction sum(String fieldName) {
        return new SumAggregateFunction(fieldName);
    }
    
    public static AggregateFunction avg(String fieldName) {
        return new AvgAggregateFunction(fieldName);
    }
    
    public static AggregateFunction max(String fieldName) {
        return new MaxAggregateFunction(fieldName);
    }
}
