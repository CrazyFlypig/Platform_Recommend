package com.xiyou.platform.UserRecommend;

import java.util.Comparator;

/**
 * 商品类，便于按评分排序
 *
 * @author cc
 * @create 2017-08-08-2:48
 */

public class Item implements Comparable<Item>{
    private String item;
    private Double platform;
    public Item(String i, Double d){
        item = i;
        platform = d;
    }
    public String getItem() {
        return item;
    }
    public Double getPlatform() {
        return platform;
    }
    public void setItem(String item) {
        this.item = item;
    }
    public void setPlatform(Double platform) {
        this.platform = platform;
    }
    public int compareTo(Item other){
        return other.getPlatform().compareTo(this.platform);
    }

}
