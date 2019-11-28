package com.sxt.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MainClass {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration(true);
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MainClass.class);
        job.setJobName("hdfs  to hbase2121");

        FileInputFormat.addInputPath(job,new Path("/log/20191126/events-.1574769239563"));

        job.setMapperClass(AnalyserLogDataMapperDemo.class);

        TableMapReduceUtil.initTableReducerJob(
                "eventlog",
                null,
                job,
                null,
                null,
                null,
                null,
                false
        );

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        boolean flag = job.waitForCompletion(true);
        System.out.println(flag ? 0 : 1);

    }
}
