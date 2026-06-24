package com.ruyuan.mq.console.model;

import java.time.LocalDateTime;

/**
 * 系统告警
 * 
 * @author RuYuan
 * @version 1.0.0
 */
public class SystemAlert {
    private String alertId;
    private String alertType; // ERROR, WARNING, INFO
    private String title;
    private String message;
    private String source;
    private LocalDateTime createTime;
    private boolean resolved;
    
    public SystemAlert() {
        this.createTime = LocalDateTime.now();
        this.resolved = false;
    }
    
    public SystemAlert(String alertType, String title, String message, String source) {
        this();
        this.alertType = alertType;
        this.title = title;
        this.message = message;
        this.source = source;
        this.alertId = generateAlertId();
    }
    
    private String generateAlertId() {
        return "ALERT-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 1000);
    }
    
    // Getters and Setters
    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }
    
    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }
    
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    
    public LocalDateTime getCreateTime() { return createTime; }
    public void setCreateTime(LocalDateTime createTime) { this.createTime = createTime; }
    
    public boolean isResolved() { return resolved; }
    public void setResolved(boolean resolved) { this.resolved = resolved; }
    
    @Override
    public String toString() {
        return String.format("SystemAlert{type='%s', title='%s', source='%s', resolved=%s}",
                alertType, title, source, resolved);
    }
}
