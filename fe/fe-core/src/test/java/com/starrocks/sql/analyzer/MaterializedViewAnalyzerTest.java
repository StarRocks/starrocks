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

import com.google.common.base.Joiner;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.util.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class MaterializedViewAnalyzerTest {
    static StarRocksAssert starRocksAssert;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testMaterializedAnalyPaimonTable(@Mocked SlotRef slotRef, @Mocked PaimonTable table) {
        MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor materializedViewAnalyzerVisitor =
                new MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor();

        {
            // test check partition column can not be found
            boolean checkSuccess = false;
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = false;
                }
            };
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
                checkSuccess = true;
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Materialized view partition column in partition exp " +
                                "must be base table partition column"));
            }
            Assert.assertFalse(checkSuccess);
        }

        {
            // test check successfully
            boolean checkSuccess = false;
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = false;

                    table.getPartitionColumnNames();
                    result = Lists.newArrayList("dt");

                    slotRef.getColumnName();
                    result = "dt";

                    table.getColumn("dt");
                    result = new Column("dt", ScalarType.createType(PrimitiveType.DATE));
                }
            };
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
                checkSuccess = true;
            } catch (Exception e) {
            }
            Assert.assertTrue(checkSuccess);
        }

        {
            //test paimon table is unparitioned
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = true;
                }
            };

            boolean checkSuccess = false;
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Materialized view partition column in partition exp " +
                                "must be base table partition column"));
            }
            Assert.assertFalse(checkSuccess);
        }
    }

    @Test
    public void testReplacePaimonTableAlias(@Mocked SlotRef slotRef, @Mocked PaimonTable table) {
        MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor materializedViewAnalyzerVisitor =
                new MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor();
        BaseTableInfo baseTableInfo = new BaseTableInfo("test_catalog", "test_db", "test_tbl",
                "test_tbl:7920f06f-df49-472f-9662-97ac5c32da96(test_tbl) REFERENCES");
        {
            new Expectations() {
                {
                    table.getCatalogName();
                    result = "test_catalog";
                    table.getDbName();
                    result = "test_db";
                    table.getTableIdentifier();
                    result = "test_tbl:7920f06f-df49-472f-9662-97ac5c32da96(test_tbl) REFERENCES";
                }
            };

            Assert.assertTrue(materializedViewAnalyzerVisitor.replacePaimonTableAlias(slotRef, table, baseTableInfo));
        }

        {
            new Expectations() {
                {
                    table.getCatalogName();
                    result = "test_catalog2";

                }
            };
            Assert.assertFalse(materializedViewAnalyzerVisitor.replacePaimonTableAlias(slotRef, table, baseTableInfo));

        }

    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        Config.enable_experimental_mv = true;
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        ConnectorPlanTestBase.mockAllCatalogs(starRocksAssert.getCtx(), temp.newFolder().toURI().toString());
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
    public void testCreateIcebergTable() throws Exception {
        {
            String mvName = "iceberg_parttbl_mv1";
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                            "COMMENT \"MATERIALIZED_VIEW\"\n" +
                            "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");
            Table mv = starRocksAssert.getTable("test", mvName);
            Assert.assertTrue(mv != null);
            starRocksAssert.dropMaterializedView(mvName);
        }

        try {
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_bucket_mv1`\n" +
                            "COMMENT \"MATERIALIZED_VIEW\"\n" +
                            "PARTITION BY ts\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_bucket` as a;");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().
                    contains("Do not support create materialized view when base iceberg table partition transform " +
                            "has bucket or truncate."));
        }
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        analyzeSuccess("refresh materialized view mv");
        Database testDb = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = testDb.getTable("mv");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        mv.setInactiveAndReason(AlterJobMgr.MANUAL_INACTIVE_MV_REASON);
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
        Assert.assertEquals("[[a, date, , YES, YES, null, , , a1]," +
                        " [b, int, , YES, YES, null, , , b2]," +
                        " [c, int, , YES, YES, null, , , ]]",
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

    @Test
    public void testCreateMvWithNotExistResourceGroup() {
        String sql = "create materialized view mv\n" +
                "PARTITION BY k1\n" +
                "distributed by hash(k2) buckets 3\n" +
                "refresh async\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"resource_group\" = \"not_exist_rg\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;";
        Assert.assertThrows("resource_group not_exist_rg does not exist.",
                DdlException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql));
    }

    @Test
    public void testCreateMvWithWindowFunction() throws Exception {
        {
            String mvSql = "create materialized view window_mv_1\n" +
                    "partition by date_trunc('month', k1)\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (partition by date_trunc('month', k1) order by  k2)\n" +
                    "from tbl1 \n";
            starRocksAssert.useDatabase("test").withMaterializedView(mvSql);
        }

        {
            String mvSql = "create materialized view window_mv_2\n" +
                    "partition by k1\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (partition by k1 order by  k2)\n" +
                    "from tbl1 \n";
            starRocksAssert.useDatabase("test").withMaterializedView(mvSql);
        }

        {
            String mvSql = "create materialized view window_mv_3\n" +
                    "partition by k1\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (order by  k2)\n" +
                    "from tbl1 \n";
            analyzeFail(mvSql, "Detail message: window function row_number ’s partition expressions" +
                    " should contain the partition column k1 of materialized view");
        }
    }

    @Test
    public void testGetQueryOutputIndices() {
        checkQueryOutputIndices(Arrays.asList(1, 2, 0, 3), "2,0,1,3", true);
        checkQueryOutputIndices(Arrays.asList(0, 1, 2, 3), "0,1,2,3", false);
        checkQueryOutputIndices(Arrays.asList(3, 2, 1, 0), "3,2,1,0", true);
        checkQueryOutputIndices(Arrays.asList(1, 2, 3, 0), "3,0,1,2", true);
        checkQueryOutputIndices(Arrays.asList(0, 1), "0,1", false);
    }

    private void checkQueryOutputIndices(List<Integer> inputs, String expect, boolean isChanged) {
        List<Pair<Column, Integer>> mvColumnPairs = Lists.newArrayList();
        for (Integer i : inputs) {
            mvColumnPairs.add(Pair.create(new Column(), i));
        }
        List<Integer> queryOutputIndices = MaterializedViewAnalyzer.getQueryOutputIndices(mvColumnPairs);
        Assert.assertTrue(queryOutputIndices.size() == mvColumnPairs.size());
        Assert.assertEquals(Joiner.on(",").join(queryOutputIndices), expect);
        Assert.assertEquals(IntStream.range(0, queryOutputIndices.size()).anyMatch(i -> i != queryOutputIndices.get(i)),
                isChanged);

    }
}
