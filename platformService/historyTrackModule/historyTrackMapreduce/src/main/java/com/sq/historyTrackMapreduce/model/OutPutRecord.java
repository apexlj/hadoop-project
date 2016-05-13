package com.sq.historyTrackMapreduce.model;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/11 12:15
 */
public class OutPutRecord implements Cloneable {
    private static final String FIELD_SEPARATOR = ",";
    // 经度
    private double longitude;
    // 纬度
    private double latitude;
    // 时间
    private long time;
    // 是否转向点
    private boolean isTurning;

    public OutPutRecord(double longitude, double latitude, long time) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.time = time;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setTurning(boolean turning) {
        isTurning = turning;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return longitude + FIELD_SEPARATOR + latitude + FIELD_SEPARATOR + time + FIELD_SEPARATOR + isTurning;
    }
}
