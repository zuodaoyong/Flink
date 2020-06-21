package com.flink.project.order.entity;


public class OrderResult {

    private String orderId;
    private String msg;

    public OrderResult(String orderId, String msg) {
        this.orderId = orderId;
        this.msg = msg;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId='" + orderId + '\'' +
                ", msg='" + msg + '\'' +
                '}';
    }
}
