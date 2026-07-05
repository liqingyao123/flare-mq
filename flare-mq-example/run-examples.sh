#!/bin/bash

echo "========================================"
echo "FlareMQ 示例运行脚本"
echo "========================================"

# 检查Java环境
if [ -z "$JAVA_HOME" ]; then
    echo "错误: 请设置JAVA_HOME环境变量"
    exit 1
fi

CLASSPATH="target/classes:target/dependency/*"

echo ""
echo "可用的示例:"
echo "1. 快速开始示例 (QuickStartExample)"
echo "2. Producer示例 (SimpleProducerExample)"
echo "3. Consumer示例 (SimpleConsumerExample)"
echo "4. 高级特性示例 (AdvancedFeaturesExample)"
echo "5. 性能测试示例 (PerformanceTestExample)"
echo "6. 集群模式示例 (ClusterExample)"
echo "0. 退出"
echo ""

read -p "请选择要运行的示例 (0-6): " choice

case $choice in
    1)
        echo "运行快速开始示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.quickstart.QuickStartExample
        ;;
    2)
        echo "运行Producer示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.producer.SimpleProducerExample
        ;;
    3)
        echo "运行Consumer示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.consumer.SimpleConsumerExample
        ;;
    4)
        echo "运行高级特性示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.advanced.AdvancedFeaturesExample
        ;;
    5)
        echo "运行性能测试示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.performance.PerformanceTestExample
        ;;
    6)
        echo "运行集群模式示例..."
        $JAVA_HOME/bin/java -cp $CLASSPATH com.flare.mq.example.cluster.ClusterExample
        ;;
    0)
        echo "退出..."
        exit 0
        ;;
    *)
        echo "无效选择，请重新运行脚本"
        exit 1
        ;;
esac

echo ""
echo "示例运行完成"
