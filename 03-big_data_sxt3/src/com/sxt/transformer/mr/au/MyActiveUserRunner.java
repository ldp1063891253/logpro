package com.sxt.transformer.mr.au;

import com.sxt.common.EventLogConstants;
import com.sxt.common.GlobalConstants;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import com.sxt.transformer.mr.TransformerOutputFormat;
import com.sxt.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

/**
 * yarn jar com.sxt.transformer.mr.au.MyActiveUserRunner -d 2019-11-27
 */
public class MyActiveUserRunner implements Tool {
    private Configuration configuration = null;
    //运行
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new MyActiveUserRunner(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        //输入参数进行设置
        this.processArgs(strings, configuration);
        Job job = new Job(configuration, "active user");

        job.setJarByClass(MyActiveUserRunner.class);

        //两种方式向Configuration赋值，同时让job可以使用该参数
        // 1、在创建Job之前，先给Configuration赋值
        // 2、在创建Job之后，通过job.getConfiguration().set("", "");进行赋值
        TableMapReduceUtil.initTableMapperJob(
                this.getScan(configuration),
                MyActiveUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job,
                false
        );


        //设置reduce
        job.setReducerClass(MyActiveUserReducer.class);

        //MR输出的key值类型
        job.setOutputKeyClass(StatsUserDimension.class);
        //MR输出的value值的类型
        job.setOutputValueClass(MapWritableValue.class);

        //将结果输出到mysql中
        job.setOutputFormatClass(TransformerOutputFormat.class);
        //运行job
        return job.waitForCompletion(true) ? 0 : 1;
    }
    //输入的参数进行检查
    private void processArgs(String[] strings, Configuration configuration) {
        String date = null;
        // 如果stings是null则不用处理，如果strings没有数据，也不用处理
        if (strings != null && strings.length > 0) {
            for (int i = 0; i < strings.length; i++) {
                if ("-d".equals(strings[i])) {
                    if (i + 1 < strings.length) {
                        // 查找到-d参数之后的日期：yyyy-MM-dd
                        date = strings[i + 1];
                        break;
                    }
                }
            }
        }
        //如果date是个空值 或者date不是一个正确的日期类型的字符串  yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            date = TimeUtil.getYesterday(); //默认为昨天的日期进行处理
        }
        //将参数放到configuration 去,可以在之后使用的时候方便获取
        configuration.set(GlobalConstants.RUNNING_DATE_PARAMES,date);
    }

    private List<Scan> getScan(Configuration configuration) {
        Scan scan = new Scan();
        //yyyy-MM-dd
        String dateStr = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES);
        //开始时间
        long time = TimeUtil.parseString2Long(dateStr, TimeUtil.DATE_FORMAT);
        //结束时间
        Long endTime = time + GlobalConstants.DAY_OF_MILLISECONDS;

        scan.setStartRow(("" + time).getBytes());
        scan.setStopRow(("" + endTime).getBytes());

        //单条数据进行判断列值是否相等
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(
                EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                EventLogConstants.EventEnum.PAGEVIEW.alias.getBytes()
        );

        // 定义要查询的列
        String[] columnStrs = {
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_UUID,
        };

        //查询过滤的列
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(
                getColumns(columnStrs)
        );

        //查询条件进行同时使用
        //封装多个filter到filterList
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(singleColumnValueExcludeFilter);
        filterList.addFilter(multipleColumnPrefixFilter);

        //指定扫描的表
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
            EventLogConstants.HBASE_NAME_EVENT_LOGS.getBytes()
                );

        return Arrays.asList(scan);
    }

    private byte[][] getColumns(String[] columnStrs) {
        byte[][] bytes = new byte[columnStrs.length][];
        //将列的字符串转换为字节数组的形式,由于史=是多个列名,需要使用二位数组进行存放
        for (int i = 0; i < columnStrs.length; i++) {
            bytes[i] = columnStrs[i].getBytes();
        }
        return bytes;
    }

    @Override
    public void setConf(Configuration configuration) {
        //本地执行
        configuration.set("mapreduce.framework.name", "local");
        //zookeeper地址
        configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");

        //将配置文件的内容加载到configuration中,方便后面的参数进行获取
        configuration.addResource("output-collector.xml");
        configuration.addResource("query-mapping.xml");
        configuration.addResource("transformer-env.xml");

        this.configuration = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
