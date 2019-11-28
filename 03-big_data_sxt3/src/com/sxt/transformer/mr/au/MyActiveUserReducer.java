package com.sxt.transformer.mr.au;

import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MyActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    //使用set集合进行去重
    private Set<String> uuids = new HashSet<>();

    @Override

    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清楚上一次统计的结果
        //用户活跃多次.进行去重
        this.uuids.clear();
        for (TimeOutputValue value : values) {
            uuids.add(value.getId());
        }

        MapWritable mapWritable = new MapWritable();
        //统计数量
        // -1不能随便设置
        // 从ActiveUserCollector的30行
        // 从ActiveUserBrowserCollector的24行
        mapWritable.put(new IntWritable(-1),new IntWritable(uuids.size()));
        MapWritableValue mapWritableValue = new MapWritableValue();
        // 设置好kpi，用于从TransformerRecordWriter的103行获取sql语句实例化preparedStatement的
        mapWritableValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
        mapWritableValue.setValue(mapWritable);
        context.write(key, mapWritableValue);
    }
}
