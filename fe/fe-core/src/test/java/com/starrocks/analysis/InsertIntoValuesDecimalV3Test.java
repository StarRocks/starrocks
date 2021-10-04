// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.UUID;

public class InsertIntoValuesDecimalV3Test {
    private static String runningDir = "fe/mocked/InsertIntoValuesDecimalV3/" + UUID.randomUUID().toString() + "/";
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static ConnectContext ctx;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanStarRocksFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        String createTblStmtStr =
                "CREATE TABLE if not exists test_table (\n" +
                        "\tcol_int INT NOT NULL, \n" +
                        "\tcol_decimal DECIMAL128(20, 9) NOT NULL \n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`col_int`) \n" +
                        "COMMENT \"OLAP\" \n" +
                        "DISTRIBUTED BY HASH(`col_int`) BUCKETS 1 \n" +
                        "PROPERTIES( \"replication_num\" = \"1\", \"in_memory\" = \"false\", \"storage_format\" = \"DEFAULT\" )";

        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);

        starRocksAssert.withTable("CREATE TABLE `tarray` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)>  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    @Test
    public void testInsertIntoValuesInvolvingDecimalV3() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql1 = "INSERT INTO db1.test_table\n" +
                "  (col_int, col_decimal)\n" +
                "VALUES\n" +
                "  (\"3\", \"9180620.681794072\"),\n" +
                "  (4, 9180620.681794072),\n" +
                "  (5, 9180620),\n" +
                "  (6, 0.681794072),\n" +
                "  (\"99\", \"1724.069658963\");";
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        SelectStmt selectStmt = (SelectStmt) stmt.getQueryStmt();
        for (ArrayList<Expr> exprs : selectStmt.getValueList().getRows()) {
            Assert.assertEquals(
                    exprs.get(1).getType(),
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 9));
        }
    }

    @Test
    public void testInsertArray() throws Exception {
        String sql = "insert into tarray values (1, 2, []) ";
        try {
            String plan = UtFrameUtils.getNewFragmentPlan(ctx, sql);
        } catch (NullPointerException ignored) {
        }
    }
}

