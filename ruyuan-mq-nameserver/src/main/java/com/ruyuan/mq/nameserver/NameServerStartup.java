package com.ruyuan.mq.nameserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NameServer启动类
 * 
 * @author RuYuan MQ Team
 */
public class NameServerStartup {
    
    private static final Logger logger = LoggerFactory.getLogger(NameServerStartup.class);
    
    private static NameServerController nameServerController;
    
    public static void main(String[] args) {
        try {
            logger.info("=== RuYuan MQ NameServer is starting ===");
            
            // 解析命令行参数
            NameServerConfig config = parseArgs(args);
            
            // 创建并启动NameServer
            nameServerController = new NameServerController(config);
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down NameServer...");
                if (nameServerController != null) {
                    nameServerController.shutdown();
                }
            }));
            
            // 启动NameServer
            nameServerController.start();
            
            logger.info("=== RuYuan MQ NameServer started successfully ===");
            logger.info("NameServer address: {}", config.getNameServerAddress());
            logger.info("listen port: {}", config.getListenPort());
            logger.info("cluster name: {}", config.getClusterName());
            
            // 保持主线程运行
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("NameServer startup failed", e);
            System.exit(1);
        }
    }
    
    /**
     * 解析命令行参数
     */
    private static NameServerConfig parseArgs(String[] args) {
        NameServerConfig config = new NameServerConfig();
        
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            switch (arg) {
                case "-p":
                case "--port":
                    if (i + 1 < args.length) {
                        config.setListenPort(Integer.parseInt(args[++i]));
                    }
                    break;
                    
                case "-a":
                case "--address":
                    if (i + 1 < args.length) {
                        config.setNameServerAddress(args[++i]);
                    }
                    break;
                    
                case "-c":
                case "--cluster":
                    if (i + 1 < args.length) {
                        config.setClusterName(args[++i]);
                    }
                    break;
                    
                case "-h":
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                    
                default:
                    if (arg.startsWith("-")) {
                        logger.warn("unknown argument: {}", arg);
                    }
                    break;
            }
        }
        
        return config;
    }
    
    /**
     * 打印使用说明
     */
    private static void printUsage() {
        System.out.println("RuYuan MQ NameServer startup options:");
        System.out.println("  -p, --port <port>        listen port (default: 9876)");
        System.out.println("  -a, --address <address>  service address (default: 127.0.0.1:9876)");
        System.out.println("  -c, --cluster <name>     cluster name (default: DefaultCluster)");
        System.out.println("  -h, --help               show help");
        System.out.println();
        System.out.println("examples:");
        System.out.println("  java -jar ruyuan-mq-nameserver.jar");
        System.out.println("  java -jar ruyuan-mq-nameserver.jar -p 9876");
        System.out.println("  java -jar ruyuan-mq-nameserver.jar -a 192.168.1.100:9876 -c MyCluster");
    }
    
    /**
     * 获取NameServer控制器（用于测试）
     */
    public static NameServerController getNameServerController() {
        return nameServerController;
    }
}
