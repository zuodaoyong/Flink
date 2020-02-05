package com.flink.project.userBehavior.entity;

public class ItemViewCount implements Comparable<ItemViewCount>{
    private Long itemId;
    private Long windowEnd;
    private Integer count;
    private String windowEndStr;

    public String getWindowEndStr() {
        return windowEndStr;
    }

    public void setWindowEndStr(String windowEndStr) {
        this.windowEndStr = windowEndStr;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                ", windowEndStr='" + windowEndStr + '\'' +
                '}';
    }

    @Override
    public int compareTo(ItemViewCount o) {
        return o.getCount().compareTo(this.getCount());
    }
}
