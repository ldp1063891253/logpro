package com.sxt.transformer.mr.nu;

import com.sun.org.apache.regexp.internal.RE;
import com.sxt.common.EventLogConstants;
import com.sxt.common.GlobalConstants;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import com.sxt.transformer.mr.TransformerOutputFormat;
import com.sxt.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.BaseConfigurable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

/**
 * 运行方式
 */
public class MyNewInstallUserRunner implements Tool {
    private Configuration configuration = null;
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(true), new MyNewInstallUserRunner(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        this.processArgs(configuration, args);
        Job job = new Job(configuration, "new user | all user");


        TableMapReduceUtil.initTableMapperJob(
                this.getScans(job),
                MyNewInstallUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job,
                false
        );

        job.setReducerClass(MyNewInstallUserReducer.class);

        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //将数据处理到处到mysql中
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    /**
     * 1、设置起始key和终止key
     * 2、指定列值进行查询
     * 3、指定要查询的列
     * 4、设置表名
     * @param job
     * @return
     */
    private List<Scan> getScans(Job job) {
        Scan scan = new Scan();

        // 2019-03-22
        String runDateStr = job.getConfiguration().get(GlobalConstants.RUNNING_DATE_PARAMES);

        long runDateLong = TimeUtil.parseString2Long(runDateStr, TimeUtil.DATE_FORMAT);

        long endTime =runDateLong + GlobalConstants.DAY_OF_MILLISECONDS;
        //   99_556778667
        //   100
        //   100_4567899876

        // 设置起始和终止rowkey
        scan.setStartRow((""+runDateLong).getBytes());
        scan.setStopRow((""+endTime).getBytes());

        //要查询的数据中log列族指定事件字段的值必须是LAUNCH
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                EventLogConstants.EventEnum.LAUNCH.alias.getBytes()
        );
        String[] columnStrs = {
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
        };

        // 用于过滤多列，使用列名前缀进行过滤
        MultipleColumnPrefixFilter prefixFilter = new MultipleColumnPrefixFilter(this.getColumnBytes(columnStrs));

        //指定表名
        scan.setAttribute(
                Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                EventLogConstants.HBASE_NAME_EVENT_LOGS.getBytes()
        );

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        return Arrays.asList(scan);
    }

    /**
     * 将列名称字符串数组转换为列名称字节数组的数组
     * @param columnStrs
     * @return
     */
    private byte[][] getColumnBytes(String[] columnStrs) {
        byte[][] columnBytes = new byte[columnStrs.length][];
        for (int i = 0; i < columnStrs.length; i++) {
            columnBytes[i] = Bytes.toBytes(columnStrs[i]);
        }
        return columnBytes;
    }

    //参数处理
    private void processArgs(Configuration configuration, String[] strings) {
        String date = null;
        for (int i = 0; i < strings.length; i++) {
            if ("-d".equals(strings[i] )) {
                if (i+1 < strings.length){
                    date = strings[i + 1];
                    break;
                }
            }
        }

        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            date = TimeUtil.getYesterday();
        }
        configuration.set(GlobalConstants.RUNNING_DATE_PARAMES,date);
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("mapreduce.framework.name", "local");
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");

        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");

        this.configuration = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
