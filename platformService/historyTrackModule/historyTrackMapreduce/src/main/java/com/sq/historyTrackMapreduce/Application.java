package com.sq.historyTrackMapreduce;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @author lijiang
 * @version 1.0
 * @since 2016/5/12 14:21
 */
@SpringBootApplication
@ImportResource("classpath:hadoopContext.xml")
public class Application implements CommandLineRunner {
    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Thread.currentThread().join();
    }
}
