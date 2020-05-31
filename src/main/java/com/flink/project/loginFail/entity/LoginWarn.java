package com.flink.project.loginFail.entity;

public class LoginWarn {

    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warnMsg;

    public LoginWarn(Long userId, Long firstFailTime, Long lastFailTime, String warnMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warnMsg = warnMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(Long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public Long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(Long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarnMsg() {
        return warnMsg;
    }

    public void setWarnMsg(String warnMsg) {
        this.warnMsg = warnMsg;
    }

    @Override
    public String toString() {
        return "LoginWarn{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warnMsg='" + warnMsg + '\'' +
                '}';
    }
}
