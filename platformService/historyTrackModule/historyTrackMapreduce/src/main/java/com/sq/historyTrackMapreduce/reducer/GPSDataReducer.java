package com.sq.historyTrackMapreduce.reducer;

import com.sq.historyTrackMapreduce.model.PairKey;
import com.sq.historyTrackMapreduce.service.GPSDataHandler;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 12:14
 */
public class GPSDataReducer extends Reducer<PairKey, Text, NullWritable, Text> {
    /**
     * 根据分组把结果输出为多个文件
     */
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    private GPSDataHandler gpsDataHandler;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
        gpsDataHandler = new GPSDataHandler(multipleOutputs);
    }

    @Override
    protected void reduce(PairKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            gpsDataHandler.execute(key.getSim(), value);
        }
        // 如果所有数据集中的最后一条数据未被输出，则输出
        gpsDataHandler.outputLastRecord();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
