package com.sxt.transformer.mr.nu;

import com.sxt.common.EventLogConstants;
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

public class MyNewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    //使用set进行去重
    private Set<String> set = new HashSet<>();
    private MapWritableValue mapWritableValue = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //将上一次的分组统计结果进行清空
        this.set.clear();

        //使用set进行去重
        for (TimeOutputValue value : values) {
            set.add(value.getId());
        }

        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1),new IntWritable(set.size()));

        //设置kpi的类型
        mapWritableValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
        mapWritableValue.setValue(mapWritable);


        context.write(key, mapWritableValue);

    }
}
