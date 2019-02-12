package com.flink.applogs;

/**
 * 启动日志
 */
public class AppStartupLog extends AppBaseLog {
    private String country;                 
    private String province;                
    private String ipAddress;               

    private String network;                
    private String carrier;                

    private String brand;               
    private String deviceStyle;            
    private String screenSize;           
    private String osType;                

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    @Override
    public String getDeviceStyle() {
        return deviceStyle;
    }

    @Override
    public void setDeviceStyle(String deviceStyle) {
        this.deviceStyle = deviceStyle;
    }

    public String getScreenSize() {
        return screenSize;
    }

    public void setScreenSize(String screenSize) {
        this.screenSize = screenSize;
    }

    @Override
    public String getOsType() {
        return osType;
    }

    @Override
    public void setOsType(String osType) {
        this.osType = osType;
    }
}
