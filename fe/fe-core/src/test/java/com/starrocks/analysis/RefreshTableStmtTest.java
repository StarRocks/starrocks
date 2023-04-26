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


package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RefreshTableStmtTest {
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createTbl = "create table db1.tbl1(k1 varchar(32), catalog varchar(32), external varchar(32), k4 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTbl);
        String sql_1 = "CREATE EXTERNAL CATALOG catalog1 PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        starRocksAssert.withCatalog(sql_1);
    }

    @Test
    public void testRefreshTableParserAndAnalyzer(@Mocked MetadataMgr metadataMgr,
                                                  @Mocked Table table,
                                                  @Mocked Database database) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;

                metadataMgr.getTable(anyString, anyString, anyString);
                result = table;

                metadataMgr.getDb(anyString, anyString);
                result = database;
            }
        };
        String sql_1 = "REFRESH EXTERNAL TABLE db1.table1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1.test";
        AnalyzeTestUtil.analyzeFail(sql_1);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1 PARTITION(\"p1\", \"p2\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        Assert.assertEquals(((RefreshTableStmt) stmt).getPartitions().size(), 2);
        Assert.assertEquals(((RefreshTableStmt) stmt).getTableName().getTbl(), "table1");
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1 PARTITION(\"k1=0\\/k2=1\", \"k1=1\\/k2=2\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertEquals(((RefreshTableStmt) stmt).getPartitions().size(), 2);
    }
}
