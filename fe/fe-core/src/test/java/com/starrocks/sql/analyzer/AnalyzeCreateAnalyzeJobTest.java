// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.StatisticAutoCollector;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCreateAnalyzeJobTest {
    private static StarRocksAssert starRocksAssert;

    private static CreateAnalyzeJobStmt assertCreateAnalyzeStmt(String sql, StatsConstants.AnalyzeType expectedAnalyzeType,
                                                                String expectedSampleCollectRows) {
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);
        Assertions.assertEquals(expectedAnalyzeType, analyzeStmt.getAnalyzeType(), sql);
        if (expectedSampleCollectRows == null) {
            Assertions.assertTrue(
                    !analyzeStmt.getProperties().containsKey(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS), sql);
        } else {
            Assertions.assertEquals(expectedSampleCollectRows,
                    analyzeStmt.getProperties().get(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS), sql);
        }
        return analyzeStmt;
    }

    private static CreateAnalyzeJobStmt assertCreateHistogramAnalyzeStmt(String sql, long expectedBuckets,
                                                                         Boolean expectSampleRatioProperty) {
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);
        Assertions.assertEquals(StatsConstants.AnalyzeType.HISTOGRAM, analyzeStmt.getAnalyzeType(), sql);
        Assertions.assertTrue(analyzeStmt.getAnalyzeTypeDesc() instanceof AnalyzeHistogramDesc, sql);
        Assertions.assertEquals(expectedBuckets,
                ((AnalyzeHistogramDesc) analyzeStmt.getAnalyzeTypeDesc()).getBuckets(), sql);
        if (expectSampleRatioProperty != null) {
            Assertions.assertEquals(expectSampleRatioProperty,
                    analyzeStmt.getProperties().containsKey(StatsConstants.HISTOGRAM_SAMPLE_RATIO), sql);
        }
        return analyzeStmt;
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();
        ConnectorPlanTestBase.mockHiveCatalog(getConnectContext());

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(
                "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) " +
                        "AGGREGATE KEY(kk1, kk2,kk3,kk4) " +
                        "distributed by hash(kk1) buckets 3 " +
                        "properties('replication_num' = '1');");
        starRocksAssert.withTable("create table db.tbl1(c1 int, c2 int, c3 int)\n" +
                "partition by (c1)\n" +
                "properties('replication_num'='1') ");
        starRocksAssert.ddl("alter table db.tbl1 add partition p1 values in ('1')");
        starRocksAssert.ddl("alter table db.tbl1 add partition p2 values in ('2')");
    }

    @Test
    public void testAllDB() throws Exception {
        String sql = "create analyze all";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Assertions.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getDbId());
        Assertions.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assertions.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testAllTable() throws Exception {
        String sql = "create analyze full database db";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("db");
        Assertions.assertEquals(db.getId(), analyzeStmt.getDbId());
        Assertions.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assertions.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testColumn() throws Exception {
        String sql = "create analyze table db.tbl(kk1, kk2)";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("db");
        Assertions.assertEquals(db.getId(), analyzeStmt.getDbId());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl");
        Assertions.assertEquals(table.getId(), analyzeStmt.getTableId());
        Assertions.assertEquals(2, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testCreateAnalyzeJob() throws Exception {
        String sql = "create analyze table db.tbl";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        DDLStmtExecutor.execute(analyzeStmt, starRocksAssert.getCtx());
        Assertions.assertEquals(1,
                starRocksAssert.getCtx().getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList().size());
        sql = "create analyze sample table hive0.tpch.customer(C_NAME, C_PHONE)";
        analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);
        Assertions.assertEquals(2, analyzeStmt.getColumnNames().size());
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, analyzeStmt.getAnalyzeType());

        DDLStmtExecutor.execute(analyzeStmt, starRocksAssert.getCtx());
        Assertions.assertEquals(2,
                starRocksAssert.getCtx().getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList().size());
    }

    @Test
    public void testCreateAnalyzeSyntaxMatrix() {
        assertCreateAnalyzeStmt("create analyze table db.tbl",
                StatsConstants.AnalyzeType.FULL, null);
        assertCreateAnalyzeStmt("create analyze full table db.tbl",
                StatsConstants.AnalyzeType.FULL, null);
        assertCreateAnalyzeStmt("create analyze sample table db.tbl",
                StatsConstants.AnalyzeType.SAMPLE, null);
        assertCreateAnalyzeStmt("create analyze full table db.tbl " +
                        "properties(\"statistic_sample_collect_rows\"=\"100000\")",
                StatsConstants.AnalyzeType.FULL, "100000");
        assertCreateAnalyzeStmt("create analyze sample table db.tbl " +
                        "properties(\"statistic_sample_collect_rows\"=\"100000\")",
                StatsConstants.AnalyzeType.SAMPLE, "100000");
    }

    @Test
    public void testCreateAnalyzeHistogramSyntaxMatrix() {
        assertCreateHistogramAnalyzeStmt("create analyze table db.tbl1 update histogram on c1,c2",
                Config.histogram_buckets_size, null);
        assertCreateHistogramAnalyzeStmt("create analyze table db.tbl1 update histogram on c1,c2 with 128 buckets",
                128, null);
        assertCreateHistogramAnalyzeStmt(
                "create analyze table db.tbl1 update histogram on c1,c2 " +
                        "properties(\"histogram_sample_ratio\"=\"0.1\")",
                Config.histogram_buckets_size, true);
        assertCreateHistogramAnalyzeStmt(
                "create analyze table db.tbl1 update histogram on c1,c2 with 128 buckets " +
                        "properties(\"histogram_sample_ratio\"=\"0.1\")",
                128, true);

        analyzeFail("create analyze table db.tbl1 update histogram on c1,c2 with async mode",
                "Unexpected input 'async'");
        analyzeFail("create analyze table db.tbl1 update histogram on c1,c2 with async mode " +
                        "properties(\"histogram_sample_ratio\"=\"0.1\")",
                "Unexpected input 'async'");
        analyzeFail("create analyze table db.tbl1 update histogram on c1,c2 with sync mode with 128 buckets " +
                        "properties(\"histogram_sample_ratio\"=\"0.1\")",
                "Unexpected input 'sync'");
    }

    @Test
    public void testCreateHistogram() throws Exception {
        // mock execution
        UtFrameUtils.mockQueryExecute(() -> {
        });
        UtFrameUtils.mockDML();

        OlapTable table = (OlapTable) starRocksAssert.getTable("db", "tbl1");
        UtFrameUtils.setPartitionVersion(table.getPartition("p1"), 3);
        UtFrameUtils.setPartitionVersion(table.getPartition("p2"), 3);
        PlanTestBase.setTableStatistics(table, 1000);
        PlanTestBase.setPartitionStatistics(table, "p1", 500);
        PlanTestBase.setPartitionStatistics(table, "p2", 500);

        // create job
        starRocksAssert.ddl("create analyze table db.tbl1 update histogram on c1,c2 with 128 buckets ");
        List<List<String>> analyzeJobs = starRocksAssert.show("show analyze job where `Type` = 'HISTOGRAM'");
        List<String> jobDesc = analyzeJobs.get(0);
        String jobId = jobDesc.get(0);
        Assertions.assertEquals(
                List.of("default_catalog", "db", "tbl1", "c1,c2", "HISTOGRAM", "SCHEDULE",
                        "{histogram_sample_ratio=1, histogram_collect_bucket_ndv_mode=none, histogram_mcv_size=100, " +
                        "histogram_bucket_num=128}"),
                jobDesc.subList(1, jobDesc.size() - 3));

        // trigger the job
        StatisticAutoCollector statisticAutoCollector = GlobalStateMgr.getCurrentState().getStatisticAutoCollector();
        statisticAutoCollector.runJobs();
        {
            analyzeJobs = starRocksAssert.show("show analyze job where `Type` = 'HISTOGRAM'");
            jobDesc = analyzeJobs.get(0);
            jobId = jobDesc.get(0);
            Assertions.assertEquals(
                    List.of("default_catalog", "db", "tbl1", "c1,c2", "HISTOGRAM", "SCHEDULE",
                            "{histogram_sample_ratio=1, histogram_collect_bucket_ndv_mode=none, histogram_mcv_size=100, " +
                            "histogram_bucket_num=128}",
                            "FINISH"),
                    jobDesc.subList(1, jobDesc.size() - 2));
        }

        // drop analyze
        starRocksAssert.ddl("drop analyze " + jobId);
    }

    @Test
    public void testPrepareAnalyzeJob() {
        StatisticAutoCollector statisticAutoCollector = GlobalStateMgr.getCurrentState().getStatisticAutoCollector();
        statisticAutoCollector.prepareDefaultJob();
        List<NativeAnalyzeJob> jobs = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();

        Optional<NativeAnalyzeJob> defaultJob = jobs.stream().filter(NativeAnalyzeJob::isDefaultJob).findFirst();
        Assertions.assertTrue(defaultJob.isPresent());
        Assertions.assertSame(StatsConstants.AnalyzeType.FULL, defaultJob.get().getAnalyzeType());

        Config.enable_collect_full_statistic = false;
        statisticAutoCollector.prepareDefaultJob();
        jobs = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAllNativeAnalyzeJobList();
        defaultJob = jobs.stream().filter(NativeAnalyzeJob::isDefaultJob).findFirst();
        Assertions.assertTrue(defaultJob.isPresent());
        Assertions.assertSame(StatsConstants.AnalyzeType.SAMPLE, defaultJob.get().getAnalyzeType());
        Config.enable_collect_full_statistic = true;
    }
}
