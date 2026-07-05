package com.flare.mq.nameserver.registry;

/**
 * Broker注册结果
 * 
 * @author FlareMQ Team
 */
public class RegisterBrokerResult {
    
    private String haServerAddr;
    private String masterAddr;
    private boolean success;
    private String message;
    
    public RegisterBrokerResult() {
        this.success = true;
    }
    
    public RegisterBrokerResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }
    
    // Getters and Setters
    public String getHaServerAddr() {
        return haServerAddr;
    }
    
    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }
    
    public String getMasterAddr() {
        return masterAddr;
    }
    
    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    @Override
    public String toString() {
        return "RegisterBrokerResult{" +
                "haServerAddr='" + haServerAddr + '\'' +
                ", masterAddr='" + masterAddr + '\'' +
                ", success=" + success +
                ", message='" + message + '\'' +
                '}';
    }
}
