package com.bjxst.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyAPP implements Tool {

    private Configuration configuration = null;
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "hdfs to hbase");


        job.setJarByClass(MyAPP.class);
        job.setMapperClass(MyMapper.class);
        FileInputFormat.addInputPath(job, new Path("/hello.txt"));
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(
                "tb_hello",
                null,
                job,
                null,
                null,
                null,
                null,
                false
        );


        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);
        return b ? 0:1;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("hbase.zookeeper.quorum", "node2:2181,node3:2181,node4:2181");
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {
       ToolRunner.run(new Configuration(true),new MyAPP(), args);
    }
}
