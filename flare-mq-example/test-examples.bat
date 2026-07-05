@echo off
echo ========================================
echo FlareMQ 示例测试脚本
echo ========================================

set JAVA_HOME=%JAVA_HOME%
if "%JAVA_HOME%"=="" (
    echo 错误: 请设置JAVA_HOME环境变量
    pause
    exit /b 1
)

set CLASSPATH=target\classes;target\dependency\*

echo.
echo 测试编译结果...

echo 1. 测试QuickStartExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.quickstart.QuickStartExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] QuickStartExample类加载失败
) else (
    echo [成功] QuickStartExample类加载成功
)

echo 2. 测试SimpleProducerExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.producer.SimpleProducerExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] SimpleProducerExample类加载失败
) else (
    echo [成功] SimpleProducerExample类加载成功
)

echo 3. 测试SimpleConsumerExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.consumer.SimpleConsumerExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] SimpleConsumerExample类加载失败
) else (
    echo [成功] SimpleConsumerExample类加载成功
)

echo 4. 测试AdvancedFeaturesExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.advanced.AdvancedFeaturesExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] AdvancedFeaturesExample类加载失败
) else (
    echo [成功] AdvancedFeaturesExample类加载成功
)

echo 5. 测试PerformanceTestExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.performance.PerformanceTestExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] PerformanceTestExample类加载失败
) else (
    echo [成功] PerformanceTestExample类加载成功
)

echo 6. 测试ClusterExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.cluster.ClusterExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] ClusterExample类加载失败
) else (
    echo [成功] ClusterExample类加载成功
)

echo 7. 测试ComprehensiveExample类加载...
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Xmx256m com.flare.mq.example.comprehensive.ComprehensiveExample --help 2>nul
if %errorlevel% neq 0 (
    echo [失败] ComprehensiveExample类加载失败
) else (
    echo [成功] ComprehensiveExample类加载成功
)

echo.
echo ========================================
echo 所有示例类测试完成
echo ========================================
echo.
echo 注意: 要运行完整示例，需要先启动NameServer和Broker
echo 1. 启动NameServer: java -cp flare-mq-nameserver.jar com.flare.mq.nameserver.NameServerStartup
echo 2. 启动Broker: java -cp flare-mq-broker.jar com.flare.mq.broker.BrokerStartup
echo 3. 运行示例: run-examples.bat
echo.
pause
