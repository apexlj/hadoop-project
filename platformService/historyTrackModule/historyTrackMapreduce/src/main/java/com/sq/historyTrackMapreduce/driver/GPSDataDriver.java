package com.sq.historyTrackMapreduce.driver;

import com.sq.historyTrackMapreduce.comparator.GroupingComparator;
import com.sq.historyTrackMapreduce.mapper.GPSDataMapper;
import com.sq.historyTrackMapreduce.model.PairKey;
import com.sq.historyTrackMapreduce.reducer.GPSDataReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 12:18
 */
public class GPSDataDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [options] <input> <output> \n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "gpsData");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 键值对文件输入格式：键值对之间以制表符分隔
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置分组比较器，以sim卡号分组
        job.setGroupingComparatorClass(GroupingComparator.class);

        // 当mapOutPutKey和outPutKey类型不同时，需要指明mapOutPutKeyClass
        job.setMapOutputKeyClass(PairKey.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(GPSDataMapper.class);
        job.setReducerClass(GPSDataReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new GPSDataDriver(), args);
        System.exit(code);
    }
}
