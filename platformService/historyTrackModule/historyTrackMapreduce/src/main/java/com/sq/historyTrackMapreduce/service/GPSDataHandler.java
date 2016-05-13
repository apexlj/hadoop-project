package com.sq.historyTrackMapreduce.service;

import com.sq.historyTrackMapreduce.model.LngLat;
import com.sq.historyTrackMapreduce.model.OutPutRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 数据处理对象
 *
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 19:28
 */
public class GPSDataHandler {
    private static final int MAX_RECORD = 30;
    private static final int MAX_ANGLE = 30;
    private static final Logger logger = LoggerFactory.getLogger(GPSDataHandler.class);
    private long sim;
    // 连续记录数
    private int recordCount;
    // 当前经纬度
    private LngLat currentLngLat = new LngLat();
    // 上一条记录的经纬度
    private LngLat previousLngLat = new LngLat();
    // 上次计算的方向角
    private Double previousAngle;
    // 上一条记录：当进入弯道时，不仅要输出弯道点也要输出弯道点的前一点，让路线更加完整
    private OutPutRecord previousRecord;
    // 数据解析器
    private GPSParser gpsParser = new GPSParser();
    // 数据输出
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    public GPSDataHandler(MultipleOutputs<NullWritable, Text> multipleOutputs) {
        this.multipleOutputs = multipleOutputs;
    }

    /**
     * 处理记录
     *
     * @param sim   sim卡号
     * @param value 记录
     * @throws IOException
     * @throws InterruptedException
     */
    public void execute(long sim, Text value) throws IOException, InterruptedException {
        init(sim);
        OutPutRecord currentRecord = gpsParser.parseToOutPutRecord(value);
        currentLngLat.set(currentRecord.getLongitude(), currentRecord.getLatitude());
        // 第一条数据
        if (previousRecord == null) {
            logger.info("输出首条记录");
            outputRecord(sim, currentRecord);
        } else {
            double currentAngle = currentLngLat.getAngle(previousLngLat);
            if (previousAngle == null) {
                // 第二条数据，此时previousAngle还没有值，直接输出记录，并给previousAngle赋值
                outputRecord(sim, currentRecord);
            } else {
                // 第二条数据以后
                handleRecord(sim, currentRecord, currentAngle);
            }

            previousAngle = currentAngle;
        }

        // 缓存值
        try {
            previousLngLat = (LngLat) currentLngLat.clone();
            previousRecord = (OutPutRecord) currentRecord.clone();
        } catch (CloneNotSupportedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 处理记录
     *
     * @param sim           sim卡号
     * @param currentRecord 当前记录
     * @param currentAngle  当前方向角
     * @throws IOException
     * @throws InterruptedException
     */
    private void handleRecord(long sim, OutPutRecord currentRecord, double currentAngle) throws IOException, InterruptedException {
        // 当前方向角与上次方向角之差大于阈值或者超过最大未记录数，则加入结果集
        double angle = Math.abs(currentAngle - previousAngle);
        if (recordCount == MAX_RECORD) {
            logger.info("输出记录[记录条数:{}]", recordCount);
            outputRecord(sim, currentRecord);
        } else if (angle >= MAX_ANGLE) {
            logger.info("输出记录[方向角之差:{}]", angle);
            // 当上一条记录未被输出过时输出上一条记录
            if (!isPreviousRecordOutput()) {
                previousRecord.setTurning(true);
                outputRecord(sim, previousRecord);
            }
            currentRecord.setTurning(true);
            outputRecord(sim, currentRecord);
        } else {
            recordCount++;
            logger.info("计数器递增:{}", recordCount);
        }
    }

    /**
     * 输出记录
     *
     * @param sim   sim卡号
     * @param value 记录
     * @throws IOException
     * @throws InterruptedException
     */
    private void outputRecord(long sim, OutPutRecord value) throws IOException, InterruptedException {
        multipleOutputs.write(NullWritable.get(), new Text(value.toString()), String.valueOf(sim));
        resetRecordCount();
    }

    /**
     * 重置计数器
     */
    private void resetRecordCount() {
        logger.info("计数器归零");
        recordCount = 0;
    }

    /**
     * 初始化任务数据
     *
     * @param sim sim卡号
     * @throws IOException
     * @throws InterruptedException
     */
    private void init(long sim) throws IOException, InterruptedException {
        if (this.sim != sim) {
            // 如果前一个sim卡号的最后一条数据未被输出过，则输出
            outputLastRecord();
            resetAllField(sim);
        }
    }

    /**
     * 前一条数据是否被输出过
     *
     * @return true：已输出；false：未输出
     */
    private boolean isPreviousRecordOutput() {
        return previousRecord != null && recordCount == 0;
    }

    /**
     * 重置所有属性
     *
     * @param sim sim卡号
     */
    private void resetAllField(long sim) {
        logger.info("开始处理终端: {}", sim);
        this.sim = sim;
        previousAngle = null;
        recordCount = 0;
        previousRecord = null;
    }

    /**
     * 如果前一个sim卡号的最后一条数据未被输出过，则输出
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void outputLastRecord() throws IOException, InterruptedException {
        if (previousRecord == null)
            return;
        if (!isPreviousRecordOutput()) {
            outputRecord(sim, previousRecord);
        }
    }
}
