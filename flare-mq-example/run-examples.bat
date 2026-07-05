@echo off
echo ========================================
echo FlareMQ 示例运行脚本
echo ========================================

set JAVA_HOME=%JAVA_HOME%
if "%JAVA_HOME%"=="" (
    echo 错误: 请设置JAVA_HOME环境变量
    pause
    exit /b 1
)

set CLASSPATH=target\classes;target\dependency\*

echo.
echo 可用的示例:
echo 1. 快速开始示例 (QuickStartExample)
echo 2. Producer示例 (SimpleProducerExample)
echo 3. Consumer示例 (SimpleConsumerExample)
echo 4. 高级特性示例 (AdvancedFeaturesExample)
echo 5. 性能测试示例 (PerformanceTestExample)
echo 6. 集群模式示例 (ClusterExample)
echo 0. 退出
echo.

set /p choice=请选择要运行的示例 (0-6): 

if "%choice%"=="1" (
    echo 运行快速开始示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.quickstart.QuickStartExample
) else if "%choice%"=="2" (
    echo 运行Producer示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.producer.SimpleProducerExample
) else if "%choice%"=="3" (
    echo 运行Consumer示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.consumer.SimpleConsumerExample
) else if "%choice%"=="4" (
    echo 运行高级特性示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.advanced.AdvancedFeaturesExample
) else if "%choice%"=="5" (
    echo 运行性能测试示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.performance.PerformanceTestExample
) else if "%choice%"=="6" (
    echo 运行集群模式示例...
    "%JAVA_HOME%\bin\java" -cp %CLASSPATH% com.flare.mq.example.cluster.ClusterExample
) else if "%choice%"=="0" (
    echo 退出...
    exit /b 0
) else (
    echo 无效选择，请重新运行脚本
)

echo.
echo 示例运行完成
pause
