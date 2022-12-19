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

import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowAlterTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testShowAlter1() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess("SHOW ALTER TABLE COLUMN FROM db");
        Assert.assertEquals("db", statement.getDbName());
        Assert.assertEquals(ShowAlterStmt.AlterType.COLUMN, statement.getType());
    }

    @Test
    public void testShowAlter2() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `TableName` = \'abc\' LIMIT 1, 2");
        Assert.assertEquals("db", statement.getDbName());
        Assert.assertEquals(ShowAlterStmt.AlterType.COLUMN, statement.getType());
        Assert.assertTrue(statement.getFilterMap().containsKey("tablename"));
        Assert.assertEquals(2, statement.getLimitElement().getLimit());
        Assert.assertEquals(1, statement.getLimitElement().getOffset());
    }

    @Test
    public void testShowAlter3() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db ORDER BY CreateTime");
        Assert.assertTrue(statement.getOrderByElements().get(0).getExpr() instanceof SlotRef);
    }

    @Test
    public void testShowAlter4() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00'");
        Assert.assertTrue(statement.getFilterMap().containsKey("createtime"));
    }

    @Test
    public void normalTest() {
        analyzeSuccess("SHOW ALTER TABLE COLUMN ORDER BY CreateTime DESC LIMIT 1;");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM errordb",
                "Unknown database 'errordb'");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00' " +
                        "AND `bad_column` < '2010-12-04 00:00:00'",
                "The columns of TableName/CreateTime/FinishTime/State are supported");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00' " +
                        "OR `FinishTime` < '2022-12-04 00:00:00'",
                "Only allow compound predicate with operator AND");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW FROM db WHERE `TableName` = \'abc\' LIMIT 1, 2");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW FROM db ORDER BY CreateTime");        
    }
}
