package com.ruyuan.mq.console;

import com.ruyuan.mq.console.service.MonitorService;
import com.ruyuan.mq.console.service.impl.MonitorServiceImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class ConsoleApplication {

    private MonitorService monitorService;

    public static void main(String[] args) {
        SpringApplication.run(ConsoleApplication.class, args);
    }

    @Bean
    public MonitorService monitorService() {
        MonitorServiceImpl service = new MonitorServiceImpl();
        service.start();
        this.monitorService = service;
        return service;
    }

    /** 每 30 秒刷新一次系统指标，模拟数据持续变化 */
    @Scheduled(fixedRate = 30000)
    public void refreshMetrics() {
        if (monitorService != null && monitorService.isRunning()) {
            monitorService.refreshSystemMetrics();
        }
    }
}
