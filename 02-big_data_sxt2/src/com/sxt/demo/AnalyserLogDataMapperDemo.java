package com.sxt.demo;

import com.sxt.common.EventLogConstants;
import com.sxt.etl.util.IPSeekerExt;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnalyserLogDataMapperDemo extends Mapper<LongWritable, Text, NullWritable, Put> {
    /**
     * 192.168.100.1^A1574736498.958^Anode1^A/log.gif?en=e_e&ca=event%E7%9A%84category%E5%90%8D%E7    %A7%B0&ac=event%E7%9A%84action%E5%90%8D%E7%A7%B0&kv_key1=value1&kv_key2=value2&du=1245&ver=    1&pl=website&sdk=js&u_ud=8D4F0D4B-7623-4DB2-A17B-83AD72C2CCB3&u_mid=zhangsan&u_sd=9C7C0951-    DCD3-47F9-AD8F-B937F023611B&c_time=1574736499827&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20N    T%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2    F78.0.3904.108%20Safari%2F537.36&b_rst=1360*768192.168.100.1^A1574736498.958^Anode1^A/log.gif?en=e_e&ca=event%E7%9A%84category%E5%90%8D%E7    %A7%B0&ac=event%E7%9A%84action%E5%90%8D%E7%A7%B0&kv_key1=value1&kv_key2=value2&du=1245&ver=    1&pl=website&sdk=js&u_ud=8D4F0D4B-7623-4DB2-A17B-83AD72C2CCB3&u_mid=zhangsan&u_sd=9C7C0951-    DCD3-47F9-AD8F-B937F023611B&c_time=1574736499827&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20N    T%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2    F78.0.3904.108%20Safari%2F537.36&b_rst=1360*768
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//      String rowKey = map.get("rowKry") + "_" + UUID.randomUUID();


        String str = value.toString();
        String[] strS = str.split("\\^A");

        String rowKey =strS[1] + "_" + UUID.randomUUID();
        Put put = new Put(rowKey.getBytes());
        //处理ip  192.168.100.1
        put = delIP(put, strS[0]);

        //处理时间  1574736498.958
        put.add("log".getBytes(), "date".getBytes(), (Integer.parseInt(strS[1])*1000+"").getBytes());
        //处理服务器名称 node1
        put.add("log".getBytes(),"hostname".getBytes(), strS[2].getBytes());

        //请求参数处理
        int index = str.indexOf("?");
        String[] strings = str.substring(index + 1).split("&");
        Put newPut = putAdd(put,strings);
        context.write(NullWritable.get(),newPut);
    }

    private Put delIP(Put put, String str) {
        IPSeekerExt ipSeekerExt = new IPSeekerExt();
        IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(str.trim());

        if (info.getCountry() != null || !info.getCountry().equals("")) {
            put.add("log".getBytes(),"Country".getBytes() , info.getCountry().getBytes());
        }
        if (info.getCity() != null || !info.getCity().equals("")) {
            put.add("log".getBytes(),"city".getBytes() , info.getCity().getBytes());
        }
        if (info.getProvince() != null || !info.getProvince().equals("")) {
            put.add("log".getBytes(),"Province".getBytes() , info.getProvince().getBytes());
        }
        return put;
    }

    private Put putAdd(Put put, String[] strings) {
        for (int i = 0; i < strings.length; i++) {
            String[] split1 = strings[i].split("=");
            if (split1[0].startsWith("b_iev")){
                String[] split2 = split1[1].split("%");
                System.out.println(split2[0].toString());
                put.add("log".getBytes(),split1[0].getBytes() ,split2[0].getBytes());
                continue;
            }
            put.add("log".getBytes(),split1[0].getBytes() ,split1[1].getBytes());
        }
        return  put;
    }

}
