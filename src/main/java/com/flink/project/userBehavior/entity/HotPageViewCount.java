package com.flink.project.userBehavior.entity;


public class HotPageViewCount implements Comparable<HotPageViewCount> {

    private String url;
    private Integer count;
    private Long windowEnd;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }


    @Override
    public String toString() {
        return "HotPageViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }

    @Override
    public int compareTo(HotPageViewCount o) {
        return o.getCount().compareTo(this.getCount());
    }
}
