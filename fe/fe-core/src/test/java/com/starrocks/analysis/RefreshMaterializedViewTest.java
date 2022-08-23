// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RefreshMaterializedViewTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testNormal() throws Exception {
        Config.enable_experimental_mv = true;
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.tbl_with_mv\n" +
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
                .withNewMaterializedView("create materialized view mv_to_refresh\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_with_mv group by k2;");
        String refreshMvSql = "refresh materialized view test.mv_to_refresh";
        RefreshMaterializedViewStatement alterMvStmt =
                (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(refreshMvSql, connectContext);
        String dbName = alterMvStmt.getMvName().getDb();
        String mvName = alterMvStmt.getMvName().getTbl();
        Assert.assertEquals("test", dbName);
        Assert.assertEquals("mv_to_refresh", mvName);
    }
}
