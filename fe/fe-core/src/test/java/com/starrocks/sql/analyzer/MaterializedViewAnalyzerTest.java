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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class MaterializedViewAnalyzerTest {
    static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        Config.enable_experimental_mv = true;
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view mv\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        analyzeSuccess("refresh materialized view mv");
        Database testDb = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = testDb.getTable("mv");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        mv.setActive(false);
        analyzeFail("refresh materialized view mv");
    }

    @Test
    public void testMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `mv1` (a comment \"a1\", b comment \"b2\", c)\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "DISTRIBUTED BY HASH(a) BUCKETS 12\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 from test.tbl1");
        ShowExecutor showExecutor = new ShowExecutor(starRocksAssert.getCtx(),
                (ShowStmt) analyzeSuccess("show full columns from mv1"));
        ShowResultSet showResultSet = showExecutor.execute();
        Assert.assertEquals("[[a, date, , YES, YES, \\N, , , a1]," +
                        " [b, int, , YES, YES, \\N, , , b2]," +
                        " [c, int, , YES, YES, \\N, , , ]]",
                showResultSet.getResultRows().toString());
    }

    @Test
    public void testNondeterministicFunction() {
        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select  k1, k2, rand() from tbl1 group by k1, k2",
                "Materialized view query statement select item rand() not supported nondeterministic function.");

        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select k1, k2 from tbl1 group by k1, k2 union select k1, rand() from tbl1",
                "Materialized view query statement select item rand() not supported nondeterministic function.");

        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select  k1, k2 from tbl1 where rand() > 0.5",
                "Materialized view query statement select item rand() not supported nondeterministic function.");
    }
}
