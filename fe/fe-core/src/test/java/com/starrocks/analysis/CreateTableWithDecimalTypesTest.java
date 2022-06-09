// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class CreateTableWithDecimalTypesTest {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
    }

    @Test
    public void testCreateTableWithDecimalV3() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Config.enable_decimal_v3 = true;
        String createTableSql = "" +
                "CREATE TABLE if not exists db1.decimalv3_table\n" +
                "(\n" +
                "key0 INT NOT NULL,\n" +
                "col_decimal_p9s2 DECIMAL(9, 2) NOT NULL,\n" +
                "col_decimal_p18s4 DECIMAL(18, 4) NOT NULL,\n" +
                "col_decimal_p19s6 DECIMAL(19, 6) NOT NULL,\n" +
                "col_decimal_p27s9 DECIMAL(27, 9) NOT NULL,\n" +
                "col_decimal_p38s38 DECIMAL(38, 38) NOT NULL,\n" +
                "col_nullable_decimal_p9s2 DECIMAL(9, 2) NULL,\n" +
                "col_nullable_decimal_p18s4 DECIMAL(18, 4) NULL,\n" +
                "col_nullable_decimal_p19s6 DECIMAL(19, 6) NULL,\n" +
                "col_nullable_decimal_p27s9 DECIMAL(27, 9) NULL,\n" +
                "col_nullable_decimal_p38s38 DECIMAL(38, 38) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql);

        String selectSql = "select " +
                "key0, \n" +
                "col_decimal_p9s2,\n" +
                "col_decimal_p18s4, \n" +
                "col_decimal_p19s6, \n" +
                "col_decimal_p27s9,\n" +
                "col_decimal_p38s38,\n" +
                "col_nullable_decimal_p9s2,\n" +
                "col_nullable_decimal_p18s4,\n" +
                "col_nullable_decimal_p19s6,\n" +
                "col_nullable_decimal_p27s9,\n" +
                "col_nullable_decimal_p38s38 \n" +
                " from decimalv3_table";

        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(selectSql, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getGlobalStateMgr(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 11);
        Assert.assertEquals(items.get(1).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 2));
        Assert.assertEquals(items.get(2).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4));
        Assert.assertEquals(items.get(3).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 6));
        Assert.assertEquals(items.get(4).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9));
        Assert.assertEquals(items.get(5).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 38));
        Assert.assertEquals(items.get(6).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 2));
        Assert.assertEquals(items.get(7).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4));
        Assert.assertEquals(items.get(8).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 6));
        Assert.assertEquals(items.get(9).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9));
        Assert.assertEquals(items.get(10).getExpr().type,
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 38));
    }

    @Test
    public void testCreateTableWithDecimalV2() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Config.enable_decimal_v3 = false;
        String createTableSql = "" +
                "CREATE TABLE if not exists db1.decimalv2_table\n" +
                "(\n" +
                "key0 INT NOT NULL,\n" +
                "col_decimal_p9s2 DECIMAL(9, 2) NOT NULL,\n" +
                "col_decimal_p18s4 DECIMAL(18, 4) NOT NULL,\n" +
                "col_decimal_p19s6 DECIMAL(19, 6) NOT NULL,\n" +
                "col_decimal_p27s9 DECIMAL(27, 9) NOT NULL,\n" +
                "col_nullable_decimal_p9s2 DECIMAL(9, 2) NULL,\n" +
                "col_nullable_decimal_p18s4 DECIMAL(18, 4) NULL,\n" +
                "col_nullable_decimal_p19s6 DECIMAL(19, 6) NULL,\n" +
                "col_nullable_decimal_p27s9 DECIMAL(27, 9) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql);

        String selectSql = "select " +
                "key0, \n" +
                "col_decimal_p9s2,\n" +
                "col_decimal_p18s4, \n" +
                "col_decimal_p19s6, \n" +
                "col_decimal_p27s9,\n" +
                "col_nullable_decimal_p9s2,\n" +
                "col_nullable_decimal_p18s4,\n" +
                "col_nullable_decimal_p19s6,\n" +
                "col_nullable_decimal_p27s9\n" +
                " from decimalv2_table";

        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(selectSql, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getGlobalStateMgr(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 9);
        Assert.assertEquals(items.get(1).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(2).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(3).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(4).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(5).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(6).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(7).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
        Assert.assertEquals(items.get(8).getExpr().type.getPrimitiveType(),
                PrimitiveType.DECIMALV2);
    }

    public void createTableFail(boolean enableDecimalV3, String columnType) throws Exception {
        if (enableDecimalV3) {
            Config.enable_decimal_v3 = true;
        } else {
            Config.enable_decimal_v3 = false;
        }
        String createTableSql = "" +
                "CREATE TABLE if not exists db1.decimalv3_table\n" +
                "(\n" +
                "key0 INT NOT NULL,\n" +
                "col_decimal " + columnType + " NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql);
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p39s12() throws Exception {
        createTableFail(true, "DECIMAL(39, 12)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p9s10() throws Exception {
        createTableFail(true, "DECIMAL(9, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p0s1() throws Exception {
        createTableFail(true, "DECIMAL(0, 1)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3ScaleAbsent() throws Exception {
        createTableFail(true, "DECIMAL(9)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p28s9() throws Exception {
        createTableFail(false, "DECIMAL(28, 9)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p27s10() throws Exception {
        createTableFail(false, "DECIMAL(27, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p9s10() throws Exception {
        createTableFail(false, "DECIMAL(9, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p0s1() throws Exception {
        createTableFail(false, "DECIMAL(0, 1)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2ScaleAbsent() throws Exception {
        createTableFail(false, "DECIMAL(9)");
        Assert.fail("should throw an exception");
    }
}

