package com.sq.historyTrackMapreduce.service;

import com.sq.historyTrackMapreduce.model.OutPutRecord;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 11:07
 */
public class GPSParser {
    private static final String FIELD_SEPARATOR = ",";
    private static final String RECORD_REG = "^(\\d{1,3}\\.\\d+),(\\d{1,2}\\.\\d+),(\\d{14})(,.*|)";
    // 经度
    private double longitude;
    // 纬度
    private double latitude;
    // 时间
    private long time;
    // 格式是否非法
    private boolean formatInvalid;

    private void parse(String record) {
        Pattern pattern = Pattern.compile(RECORD_REG);
        Matcher matcher = pattern.matcher(record);
        if (!matcher.find()) {
            formatInvalid = true;
        } else {
            formatInvalid = false;
            longitude = Double.parseDouble(matcher.group(1));
            latitude = Double.parseDouble(matcher.group(2));
            time = Long.parseLong(matcher.group(3));
        }
    }

    public long getTime() {
        return time;
    }

    /**
     * 检查记录是否有效：经纬度在正常范围内；时间是14位数字
     *
     * @return
     */
    public boolean isValidRecord() {
        return (!formatInvalid && longitude > 0.0 && longitude < 180.0)
                && (latitude > 0.0 && latitude < 90.0) && (String.valueOf(time).length() == 14);
    }

    /**
     * 解析记录
     *
     * @param record
     */
    public void parse(Text record) {
        parse(record.toString());
    }

    /**
     * 生成输出记录对象
     *
     * @param record
     * @return
     */
    public OutPutRecord parseToOutPutRecord(Text record) {
        parse(record);
        return new OutPutRecord(this.longitude, this.latitude, this.time);
    }

    @Override
    public String toString() {
        return longitude + FIELD_SEPARATOR + latitude + FIELD_SEPARATOR + time;
    }
}
