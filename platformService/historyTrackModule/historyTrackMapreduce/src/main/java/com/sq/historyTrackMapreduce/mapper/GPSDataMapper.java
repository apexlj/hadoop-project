package com.sq.historyTrackMapreduce.mapper;

import com.sq.historyTrackMapreduce.model.PairKey;
import com.sq.historyTrackMapreduce.service.GPSParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 11:03
 */
public class GPSDataMapper extends Mapper<Text, Text, PairKey, Text> {

    private GPSParser gpsParser = new GPSParser();
    private PairKey pairKey = new PairKey();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        gpsParser.parse(value);
        if (gpsParser.isValidRecord()) {
            pairKey.set(Long.parseLong(key.toString()), gpsParser.getTime());
            context.write(pairKey, new Text(gpsParser.toString()));
        }
    }
}
