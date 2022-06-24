// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.Constants;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testAllColumns() {
        String sql = "analyze table db.tbl";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertEquals(4, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testSelectedColumns() {
        String sql = "analyze table db.tbl (kk1, kk2)";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertTrue(analyzeStmt.isSample());
        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testProperties() {
        String sql = "analyze full table db.tbl properties('expire_sec' = '30')";
        analyzeFail(sql, "Property 'expire_sec' is not valid");
    }

    @Test
    public void testShow() throws MetaNotFoundException {
        String sql = "show analyze";
        ShowAnalyzeJobStmt showAnalyzeJobStmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);

        AnalyzeJob analyzeJob = new AnalyzeJob(10002, 10004, Lists.newArrayList(), Constants.AnalyzeType.FULL,
                Constants.ScheduleType.ONCE, Maps.newHashMap(), Constants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        Assert.assertEquals("[-1, test, t0, ALL, FULL, ONCE, {}, FINISH, None, ]",
                ShowAnalyzeJobStmt.showAnalyzeJobs(analyzeJob).toString());

        sql = "show analyze job";
        showAnalyzeJobStmt = (ShowAnalyzeJobStmt) analyzeSuccess(sql);

        sql = "show analyze status";
        ShowAnalyzeStatusStmt showAnalyzeStatusStatement = (ShowAnalyzeStatusStmt) analyzeSuccess(sql);

        AnalyzeStatus analyzeStatus = new AnalyzeStatus(-1, 10002, 10004, Lists.newArrayList(), Constants.AnalyzeType.FULL,
                Constants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setEndTime(LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setStatus(Constants.ScheduleStatus.FAILED);
        analyzeStatus.setReason("Test Failed");
        Assert.assertEquals("[-1, test, t0, ALL, FULL, ONCE, {}, FAILED, 2020-01-01 01:01:00, 2020-01-01 01:01:00, Test Failed]",
                ShowAnalyzeStatusStmt.showAnalyzeStatus(analyzeStatus).toString());

        sql = "show stats meta";
        ShowBasicStatsMetaStmt showAnalyzeMetaStmt = (ShowBasicStatsMetaStmt) analyzeSuccess(sql);

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(10002, 10004, Constants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1), Maps.newHashMap());
        basicStatsMeta.setHealthy(0.5);
        Assert.assertEquals("[test, t0, FULL, 2020-01-01 01:01:00, {}, 50%]",
                ShowBasicStatsMetaStmt.showBasicStatsMeta(basicStatsMeta).toString());

        sql = "show histogram meta";
        ShowHistogramStatsMetaStmt showHistogramStatsMetaStmt = (ShowHistogramStatsMetaStmt) analyzeSuccess(sql);
        HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(10002, 10004, "v1",
                Constants.AnalyzeType.HISTOGRAM, LocalDateTime.of(2020, 1, 1, 1, 1),
                Maps.newHashMap());
        Assert.assertEquals("[test, t0, v1, HISTOGRAM, 2020-01-01 01:01:00, {}]",
                ShowHistogramStatsMetaStmt.showHistogramStatsMeta(histogramStatsMeta).toString());
    }

    @Test
    public void testHistogram() {
        String sql = "analyze table t0 update histogram on v1,v2 with 256 buckets";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);
        Assert.assertTrue(analyzeStmt.getAnalyzeTypeDesc() instanceof AnalyzeHistogramDesc);
        Assert.assertEquals(((AnalyzeHistogramDesc) (analyzeStmt.getAnalyzeTypeDesc())).getBuckets(), 256);
    }
}
