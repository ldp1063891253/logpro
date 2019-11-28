package com.bjxst.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();
        String[] split = s.split(" ");


        Put put = new Put(Bytes.toBytes(key.get()));


            put.add("cf".getBytes(),"w".getBytes(),split[0].getBytes());
            put.add("cf".getBytes(),"e".getBytes(),split[1].getBytes());
            put.add("cf".getBytes(),"t".getBytes(),split[2].getBytes());

            context.write(NullWritable.get(),put);
    }
}
