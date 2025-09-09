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

import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowAlterTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
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
        Assertions.assertEquals("db", statement.getDbName());
        Assertions.assertEquals(ShowAlterStmt.AlterType.COLUMN, statement.getType());
    }

    @Test
    public void testShowAlter2() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `TableName` = \'abc\' LIMIT 1, 2");
        Assertions.assertEquals("db", statement.getDbName());
        Assertions.assertEquals(ShowAlterStmt.AlterType.COLUMN, statement.getType());
        Assertions.assertTrue(statement.getFilterMap().containsKey("tablename"));
        Assertions.assertEquals(2, statement.getLimitElement().getLimit());
        Assertions.assertEquals(1, statement.getLimitElement().getOffset());
    }

    @Test
    public void testShowAlter3() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db ORDER BY CreateTime");
        Assertions.assertTrue(statement.getOrderByElements().get(0).getExpr() instanceof SlotRef);
    }

    @Test
    public void testShowAlter4() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00'");
        Assertions.assertTrue(statement.getFilterMap().containsKey("createtime"));
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
