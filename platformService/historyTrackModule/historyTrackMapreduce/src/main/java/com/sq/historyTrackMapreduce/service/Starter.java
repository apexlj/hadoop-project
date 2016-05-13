package com.sq.historyTrackMapreduce.service;

import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.stereotype.Component;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/12 19:20
 */
@Component
public class Starter implements InitializingBean {
    @Autowired
    private ApplicationContext context;

    @Autowired
    private JobRunner jobRunner;

    @Override
    public void afterPropertiesSet() throws Exception {
        System.out.println("开始运行");
        Job job = context.getBean("testGPS", Job.class);
        jobRunner.setJob(job);
        jobRunner.call();
//        System.out.println("开始运行2");
//        job = context.getBean("testGPS", Job.class);
//        jobRunner.setJob(job);
//        jobRunner.call();
    }
}
