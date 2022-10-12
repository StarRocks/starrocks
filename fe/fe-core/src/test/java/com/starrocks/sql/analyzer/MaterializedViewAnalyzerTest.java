// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class MaterializedViewAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        Config.enable_experimental_mv = true;
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
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
                .withNewMaterializedView("create materialized view mv\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");

        analyzeSuccess("refresh materialized view mv");
        Database testDb = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = testDb.getTable("mv");
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        mv.setActive(false);
        analyzeFail("refresh materialized view mv");
    }
}
