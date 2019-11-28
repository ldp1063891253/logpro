package com.sxt.transformer.mr.au;

import com.sxt.common.DateEnum;
import com.sxt.common.EventLogConstants;
import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsCommonDimension;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BrowserDimension;
import com.sxt.transformer.model.dim.base.DateDimension;
import com.sxt.transformer.model.dim.base.KpiDimension;
import com.sxt.transformer.model.dim.base.PlatformDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.zone.ZoneOffsetTransitionRule;
import java.util.List;

public class MyActiveUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();
    private BrowserDimension defaultbBrowserDimension = new BrowserDimension("","");
    private KpiDimension aukpiDimension = new KpiDimension(KpiType.ACTIVE_USER.name);
    private KpiDimension browerKpiDimension = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //服务器时间
        String serverTime = getCellValue(
                value,
                EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME
        );

        //浏览器名称
        String browserName = getCellValue(value,
                EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME
        );
        //浏览器版本
        String browserVersion = getCellValue(
                value,
                EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION
        );
        //用户个人标识符
        String uuid = getCellValue(
                value,
                EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                EventLogConstants.LOG_COLUMN_NAME_UUID
        );
        //平台信息  app ios pc web
        String platform = getCellValue(
                value,
                EventLogConstants.EVENT_LOGS_FAMILY_NAME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM
        );

        //平台维度
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //浏览器维度
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        //时间维度
        DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(serverTime), DateEnum.DAY);

        timeOutputValue.setId(uuid);
        timeOutputValue.setTime(Long.parseLong(serverTime));

        //设置时间维度,减少循环
        statsUserDimension.getStatsCommon().setDate(dateDimension);

        for (PlatformDimension platformDimension : platformDimensions) {
            //与浏览器无关的维度,清空二次循环的浏览器信息
            statsUserDimension.setBrowser(defaultbBrowserDimension);
            statsUserDimension.getStatsCommon().setKpi(aukpiDimension);
            statsUserDimension.getStatsCommon().setPlatform(platformDimension);

            context.write(statsUserDimension, timeOutputValue);

            for (BrowserDimension browserDimension : browserDimensions) {
                //浏览器有关的维度
                statsUserDimension.setBrowser(browserDimension);
                statsUserDimension.getStatsCommon().setKpi(browerKpiDimension);
                context.write(statsUserDimension,timeOutputValue);
            }
        }

    }

    public String getCellValue(Result value, String familyName, String columnName) {
        String str = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                familyName.getBytes(),
                columnName.getBytes()
        )));
        return str;
    }
}
