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
import com.starrocks.catalog.Table;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCreateAnalyzeJobTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();
        ConnectorPlanTestBase.mockHiveCatalog(getConnectContext());

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testAllDB() throws Exception {
        String sql = "create analyze all";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Assert.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getDbId());
        Assert.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assert.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testAllTable() throws Exception {
        String sql = "create analyze full database db";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("db");
        Assert.assertEquals(db.getId(), analyzeStmt.getDbId());
        Assert.assertEquals(StatsConstants.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assert.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testColumn() throws Exception {
        String sql = "create analyze table db.tbl(kk1, kk2)";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("db");
        Assert.assertEquals(db.getId(), analyzeStmt.getDbId());
        Table table = db.getTable("tbl");
        Assert.assertEquals(table.getId(), analyzeStmt.getTableId());
        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testCreateAnalyzeJob() throws Exception {
        String sql = "create analyze table db.tbl";
        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);

        DDLStmtExecutor.execute(analyzeStmt, starRocksAssert.getCtx());
        Assert.assertEquals(1,
                starRocksAssert.getCtx().getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList().size());

        sql = "create analyze table hive0.tpch.customer(C_NAME, C_PHONE)";
        analyzeStmt = (CreateAnalyzeJobStmt) analyzeSuccess(sql);
        Assert.assertEquals("[c_name, c_phone]", analyzeStmt.getColumnNames().toString());
    }
}
