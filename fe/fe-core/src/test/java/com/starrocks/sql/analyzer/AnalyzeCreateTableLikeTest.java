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

import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCreateTableLikeTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();

        starRocksAssert = getStarRocksAssert();

        String createTblStmtStr = "create table db.tbl1(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testNormal() {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) analyzeSuccess(
                "CREATE TABLE tbl2 LIKE tbl1");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("tbl2", stmt.getTableName());
        Assert.assertEquals("test", stmt.getExistedDbName());
        Assert.assertEquals("tbl1", stmt.getExistedTableName());
    }

    @Test
    public void testAnalyzeSuccess() {
        analyzeSuccess("CREATE TABLE test1.table2 LIKE test1.table1;");
        analyzeSuccess("CREATE TABLE test2.table2 LIKE test1.table1;");
        analyzeSuccess("CREATE TABLE table2 LIKE table1;");
    }

}
