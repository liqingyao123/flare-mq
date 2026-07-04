package com.ruyuan.mq.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ConsoleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsoleApplication.class, args);
    }

    /**
     * 初始化 MonitorService Bean，使用 mock 数据（后续可接入真实 NameServer）
     */
    @Bean
    public com.ruyuan.mq.console.service.MonitorService monitorService() {
        com.ruyuan.mq.console.service.impl.MonitorServiceImpl service =
                new com.ruyuan.mq.console.service.impl.MonitorServiceImpl();
        service.start();
        return service;
    }
}
