package com.ruyuan.mq.broker;

import com.ruyuan.mq.broker.cluster.ClusterConfig;
import com.ruyuan.mq.broker.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker启动类
 * 
 * @author RuYuan MQ Team
 */
public class BrokerStartup {
    
    private static final Logger logger = LoggerFactory.getLogger(BrokerStartup.class);
    
    private static ClusterManager clusterManager;
    
    public static void main(String[] args) {
        try {
            logger.info("=== RuYuan MQ Broker is starting ===");
            
            // 解析命令行参数
            BrokerConfig config = parseArgs(args);
            
            // 创建集群配置
            ClusterConfig clusterConfig = createClusterConfig(config);
            
            // 创建并启动Broker
            clusterManager = new ClusterManager(
                config.getClusterName(), 
                config.getBrokerName(), 
                clusterConfig
            );
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Broker...");
                if (clusterManager != null) {
                    clusterManager.shutdown();
                }
            }));
            
            // 启动Broker
            clusterManager.start();
            
            logger.info("=== RuYuan MQ Broker started successfully ===");
            logger.info("Broker name: {}", config.getBrokerName());
            logger.info("Broker address: {}", config.getBrokerAddr());
            logger.info("cluster name: {}", config.getClusterName());
            logger.info("NameServer address: {}", config.getNameServerAddr());
            
            // 保持主线程运行
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Broker startup failed", e);
            System.exit(1);
        }
    }
    
    /**
     * 解析命令行参数
     */
    private static BrokerConfig parseArgs(String[] args) {
        BrokerConfig config = new BrokerConfig();
        
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            switch (arg) {
                case "-n":
                case "--nameserver":
                    if (i + 1 < args.length) {
                        config.setNameServerAddr(args[++i]);
                    }
                    break;
                    
                case "-a":
                case "--address":
                    if (i + 1 < args.length) {
                        config.setBrokerAddr(args[++i]);
                    }
                    break;
                    
                case "-c":
                case "--cluster":
                    if (i + 1 < args.length) {
                        config.setClusterName(args[++i]);
                    }
                    break;
                    
                case "-b":
                case "--broker":
                    if (i + 1 < args.length) {
                        config.setBrokerName(args[++i]);
                    }
                    break;
                    
                case "-i":
                case "--id":
                    if (i + 1 < args.length) {
                        config.setBrokerId(Long.parseLong(args[++i]));
                    }
                    break;
                    
                case "-r":
                case "--role":
                    if (i + 1 < args.length) {
                        String role = args[++i];
                        config.setMaster("master".equalsIgnoreCase(role));
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
     * 创建集群配置
     */
    private static ClusterConfig createClusterConfig(BrokerConfig brokerConfig) {
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setBrokerAddr(brokerConfig.getBrokerAddr());
        clusterConfig.setBrokerId(brokerConfig.getBrokerId());
        clusterConfig.setMasterCandidate(brokerConfig.isMaster());
        
        // 设置集群相关配置
        clusterConfig.setEnableReplication(true);
        clusterConfig.setEnableFailover(true);
        clusterConfig.setEnableDynamicBalance(true);
        
        return clusterConfig;
    }
    
    /**
     * 打印使用说明
     */
    private static void printUsage() {
        System.out.println("RuYuan MQ Broker startup options:");
        System.out.println("  -n, --nameserver <addr>  NameServer address (default: localhost:9876)");
        System.out.println("  -a, --address <address>  Broker address (default: 127.0.0.1:10911)");
        System.out.println("  -c, --cluster <name>     cluster name (default: DefaultCluster)");
        System.out.println("  -b, --broker <name>      Broker name (default: broker-a)");
        System.out.println("  -i, --id <id>            Broker ID (default: 0)");
        System.out.println("  -r, --role <role>        Broker role (default: master)");
        System.out.println("  -h, --help               show help");
        System.out.println();
        System.out.println("examples:");
        System.out.println("  java -jar ruyuan-mq-broker.jar");
        System.out.println("  java -jar ruyuan-mq-broker.jar -n localhost:9876");
        System.out.println("  java -jar ruyuan-mq-broker.jar -a 192.168.1.100:10911 -c MyCluster");
        System.out.println("  java -jar ruyuan-mq-broker.jar -b broker-1 -i 1 -r slave");
    }
    
    /**
     * 获取集群管理器（用于测试）
     */
    public static ClusterManager getClusterManager() {
        return clusterManager;
    }
    
    /**
     * Broker配置类
     */
    public static class BrokerConfig {
        private String nameServerAddr = "localhost:9876";
        private String brokerAddr = "127.0.0.1:10911";
        private String clusterName = "DefaultCluster";
        private String brokerName = "broker-a";
        private long brokerId = 0L;
        private boolean master = true;
        
        // Getters and Setters
        public String getNameServerAddr() { return nameServerAddr; }
        public void setNameServerAddr(String nameServerAddr) { this.nameServerAddr = nameServerAddr; }
        
        public String getBrokerAddr() { return brokerAddr; }
        public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
        
        public String getClusterName() { return clusterName; }
        public void setClusterName(String clusterName) { this.clusterName = clusterName; }
        
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        
        public long getBrokerId() { return brokerId; }
        public void setBrokerId(long brokerId) { this.brokerId = brokerId; }
        
        public boolean isMaster() { return master; }
        public void setMaster(boolean master) { this.master = master; }
    }
}
