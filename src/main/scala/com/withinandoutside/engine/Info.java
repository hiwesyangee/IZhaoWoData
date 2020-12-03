package com.withinandoutside.engine;

/**
 * WanWan MCTSTree数据结构中属性Info:
 * 包含ttype，section，hotel_id
 */
public class Info {
    private String ttpe;
    private int section;
    private String hotel_id;

    public Info(String ttpe, int section, String hotel_id) {
        this.ttpe = ttpe;
        this.section = section;
        this.hotel_id = hotel_id;
    }

    public String getTtpe() {
        return ttpe;
    }

    public void setTtpe(String ttpe) {
        this.ttpe = ttpe;
    }

    public int getSection() {
        return section;
    }

    public void setSection(int section) {
        this.section = section;
    }

    public String getHotel_id() {
        return hotel_id;
    }

    public void setHotel_id(String hotel_id) {
        this.hotel_id = hotel_id;
    }
}
