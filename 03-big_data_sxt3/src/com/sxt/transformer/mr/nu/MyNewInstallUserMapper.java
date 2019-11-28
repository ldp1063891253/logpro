package com.sxt.transformer.mr.nu;

import com.sxt.common.DateEnum;
import com.sxt.common.EventLogConstants;
import com.sxt.common.GlobalConstants;
import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsCommonDimension;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BrowserDimension;
import com.sxt.transformer.model.dim.base.DateDimension;
import com.sxt.transformer.model.dim.base.KpiDimension;
import com.sxt.transformer.model.dim.base.PlatformDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
                                //每个分析条件（由各个维度组成的）作为key，uuid作为value
public class MyNewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            //时间
            //浏览器
            //平台
            //模块

            String serverTime = getCellValue(
                    value,
                    EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                    EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME
            );

            String browserName = getCellValue(
                    value,
                    EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                    EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME
            );

            String browserVersion = getCellValue(
                    value,
                    EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                    EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION
            );
            //平台
            String platform = getCellValue(
                    value,
                    EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                    EventLogConstants.LOG_COLUMN_NAME_PLATFORM
            );

            String uuid = getCellValue(
                    value,
                    EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                    EventLogConstants.LOG_COLUMN_NAME_UUID
            );

            Long time = Long.valueOf(serverTime);
            //时间维度信息类
            DateDimension dateDimension = DateDimension.buildDate(time, DateEnum.DAY);

            //浏览器维度
            // 两个分支：某浏览器所有的新增用户信息，某浏览器某个版本的新增用户信息
            List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

            //平台维度类 app pc web
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

            //统计浏览器维度的新用户kpi
            KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);

            //统计新用户的kpi
            KpiDimension browserNewInstallUserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);
            timeOutputValue.setTime(time);
            timeOutputValue.setId(uuid);
            statsUserDimension.getStatsCommon().setDate(dateDimension);

            BrowserDimension emptyBrowserDimension = new BrowserDimension("", "");
            for (PlatformDimension platformDimension : platformDimensions) {
                // 清空上次循环留下的浏览器信息
                statsUserDimension.setBrowser(emptyBrowserDimension);
                // 此处的输出跟浏览器无关，数据应该发送到newInstallUser模块
                statsUserDimension.getStatsCommon().setKpi(newInstallUserKpi);
                statsUserDimension.getStatsCommon().setPlatform(platformDimension);

                context.write(statsUserDimension, timeOutputValue);
                for (BrowserDimension browserDimension : browserDimensions) {
                    // 此处的输出跟浏览器有关，数据应该发送到browserNewInstallUser模块
                    statsUserDimension.getStatsCommon().setKpi(browserNewInstallUserKpi);
                    statsUserDimension.setBrowser(browserDimension);

                    context.write(statsUserDimension,timeOutputValue);
                }
            }
        }



    public String getCellValue(Result value, String familyName, String columnName) {
        String strValue = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                familyName.getBytes(),
                columnName.getBytes()
        )));
        return strValue;
    }


    }
