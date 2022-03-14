// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class SelectStmtWithDecimalTypesTest {
    private static String runningDir = "fe/mocked/DecimalDemoTest/" + UUID.randomUUID().toString() + "/";
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanStarRocksFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        String createTblStmtStr = "" +
                "CREATE TABLE if not exists db1.decimal_table\n" +
                "(\n" +
                "key0 INT NOT NULL,\n" +
                "col0_smallint SMALLINT NOT NULL,\n" +
                "col1_smallint SMALLINT NOT NULL,\n" +
                "col_date DATE NOT NULL, \n" +
                "col_decimal DECIMALV2 NOT NULL,\n" +
                "col0_decimal_p9s2 DECIMAL32(9, 2) NOT NULL,\n" +
                "col1_decimal_p9s2 DECIMAL32(9, 2) NOT NULL,\n" +
                "col_decimal_p9s4 DECIMAL32(9, 4) NOT NULL,\n" +
                "col_decimal_p9s9 DECIMAL32(9, 9) NOT NULL,\n" +
                "col_nullable_decimal DECIMALV2 NOT NULL,\n" +
                "col0_nullable_decimal_p9s2 DECIMAL32(9, 2)  NULL,\n" +
                "col1_nullable_decimal_p9s2 DECIMAL32(9, 2)  NULL,\n" +
                "col_nullable_decimal_p15s10 DECIMAL64(15,10) NULL,\n" +
                "col_decimal_p15s10 DECIMAL64(15,10) NOT NULL,\n" +
                "col0_nullable_decimal_p38s3 DECIMAL128(38, 3)  NULL,\n" +
                "col1_nullable_decimal_p38s3 DECIMAL128(38, 3)  NULL,\n" +
                "col_decimal_p38s30 DECIMAL128(38, 30)  NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";

        ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testDecimal32AddConst() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select \n" +
                "   col0_decimal_p9s2 as a\n" +
                "  ,(cast(\"3.14\" as decimal32(9,2)) + col0_decimal_p9s2) as b\n" +
                "from db1.decimal_table\n" +
                "limit 10;";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 2 && items.get(1) != null && items.get(1).getExpr() != null);
        Type type = items.get(1).getExpr().type;
        Assert.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2));
    }

    @Test
    public void testDecimal32AddFloat() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select \n" +
                "   col0_decimal_p9s2 as a\n" +
                "  ,(cast(\"3.14\" as float) + col0_decimal_p9s2) as b\n" +
                "from db1.decimal_table\n" +
                "limit 10;";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 2 && items.get(1) != null && items.get(1).getExpr() != null);
        Type type = items.get(1).getExpr().type;
        Assert.assertEquals(type, ScalarType.DOUBLE);
    }

    @Test
    public void testDecimal32IfExpr() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   if(col0_nullable_decimal_p9s2 is NULL, 0, col0_nullable_decimal_p9s2) as res3\n" +
                "from db1.decimal_table\n" +
                "limit 10;\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertNotNull(stmt.selectList);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 1 && items.get(0) != null && items.get(0).getExpr() != null);
        Type type = items.get(0).getExpr().type;
        Assert.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2));
    }

    @Test
    public void testDecimal32IfNullExpr() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   ifnull(col0_nullable_decimal_p9s2, 3.14) as res3\n" +
                "from db1.decimal_table\n" +
                "limit 10;\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertNotNull(stmt.selectList);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.size() == 1 && items.get(0) != null && items.get(0).getExpr() != null);
        Type type = items.get(0).getExpr().type;
        Assert.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2));
    }

    @Test
    public void testAggregateMaxMin() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   max(col_decimal_p9s4) as decimal32_max,\n" +
                "   min(col_decimal_p9s4) as decimal32_min,\n" +

                "   max(col_decimal_p15s10) as decimal64_max,\n" +
                "   min(col_decimal_p15s10) as decimal64_min,\n" +

                "   max(col_decimal_p38s30) as decimal128_max,\n" +
                "   min(col_decimal_p38s30) as decimal128_min\n" +
                "from db1.decimal_table\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Type[] expectTypes = Arrays.asList(
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30)
        ).toArray(new Type[0]);
        Assert.assertTrue(items.size() == 6);
        Assert.assertTrue(expectTypes.length == 3);
        for (int i = 0; i < expectTypes.length; ++i) {
            Assert.assertTrue(items.get(i) != null && items.get(i).getExpr() != null);
            Type type = items.get(i).getExpr().type;
            Type expectType = expectTypes[i / 2];
            AggregateFunction fn = (AggregateFunction) items.get(i).getExpr().fn;
            Type returnType = fn.getReturnType();
            Type argType = fn.getArgs()[0];
            Type serdeType = fn.getIntermediateType();
            System.out.println("test#" + i + ":" + type.toString());
            Assert.assertEquals(type, expectType);
            Assert.assertEquals(returnType, expectType);
            Assert.assertEquals(argType, expectType);
            Assert.assertEquals(serdeType, null);
        }
    }

    @Test
    public void testAggregateSum() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   sum(col_decimal_p9s4) as decimal32_sum,\n" +
                "   sum_distinct(col_decimal_p9s4) as decimal32_sum_distinct,\n" +
                "   multi_distinct_sum(col_decimal_p9s4) as decimal32_multi_distinct_sum,\n" +

                "   sum(col_decimal_p15s10) as decimal64_sum,\n" +
                "   sum_distinct(col_decimal_p15s10) as decimal64_sum_distinct,\n" +
                "   multi_distinct_sum(col_decimal_p15s10) as decimal64_multi_distinct_sum,\n" +

                "   sum(col_decimal_p38s30) as decimal128_sum,\n" +
                "   sum_distinct(col_decimal_p38s30) as decimal128_sum_distinct,\n" +
                "   multi_distinct_sum(col_decimal_p38s30) as decimal128_multi_distinct_sum\n" +
                "from db1.decimal_table\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Type[] expectArgTypes = Arrays.asList(
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30)
        ).toArray(new Type[0]);

        Type[] expectReturnTypes = Arrays.asList(
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30)
        ).toArray(new Type[0]);
        Assert.assertTrue(items.size() == 9);
        Assert.assertTrue(expectArgTypes.length == 3);
        Assert.assertTrue(expectReturnTypes.length == 3);
        for (int i = 0; i < items.size(); ++i) {
            Assert.assertTrue(items.get(i) != null && items.get(i).getExpr() != null);
            Type type = items.get(i).getExpr().type;
            Type expectArgType = expectArgTypes[i / 3];
            Type expectReturnType = expectReturnTypes[i / 3];
            Assert.assertTrue(items.get(i).getExpr().fn instanceof AggregateFunction);
            AggregateFunction fn = (AggregateFunction) items.get(i).getExpr().fn;
            Type returnType = fn.getReturnType();
            Type argType = fn.getArgs()[0];
            Type serdeType = fn.getIntermediateType();
            System.out.println(
                    String.format("test#%d: type=%s, argType=%s, serdeType=%s, returnType=%s",
                            i, type, argType, serdeType, returnType));
            Assert.assertEquals(type, expectReturnType);
            Assert.assertEquals(argType, expectArgType);
            System.out.printf("%s: %s\n", fn.functionName(), serdeType);
            if (fn.functionName().equalsIgnoreCase("sum") ||
                    fn.functionName().equalsIgnoreCase("sum_distinct")) {
                Assert.assertEquals(serdeType, null);
            } else {
                Assert.assertEquals(serdeType, Type.VARCHAR);
            }
            Assert.assertEquals(returnType, expectReturnType);
        }
    }

    @Test
    public void testStrleftApplyToDecimalV2() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   strleft(variance_samp(col_decimal), 2) as a\n" +
                "from db1.decimal_table\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.get(0).getExpr().getType().isStringType());
    }

    @Test
    public void testStringFunctionsApplyToDecimalV3() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   strleft(variance_samp(col0_decimal_p9s2), 2) as a,\n" +
                "   strright(variance_samp(col_decimal_p15s10), 2) as b,\n" +
                "   substr(variance_samp(col_decimal_p38s30), 1, 2) as c\n" +
                "from db1.decimal_table\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.get(0).getExpr().getType().isStringType());
        Assert.assertTrue(items.get(1).getExpr().getType().isStringType());
        Assert.assertTrue(items.get(2).getExpr().getType().isStringType());
    }

    @Test
    public void testFloatFunctionsApplyToDecimalV3() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   cos(variance_samp(col0_decimal_p9s2)) as a,\n" +
                "   sin(variance_samp(col_decimal_p15s10)) as b,\n" +
                "   log10(variance_samp(col_decimal_p38s30)) as c\n" +
                "from db1.decimal_table\n";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.selectList != null);
        List<SelectListItem> items = stmt.selectList.getItems();
        Assert.assertTrue(items.get(0).getExpr().getType().isFloatingPointType());
        Assert.assertTrue(items.get(1).getExpr().getType().isFloatingPointType());
        Assert.assertTrue(items.get(2).getExpr().getType().isFloatingPointType());
    }

    @Test
    public void testSmallIntInPredicate() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   col0_smallint, \n" +
                "   col1_smallint\n" +
                "from db1.decimal_table\n" +
                "where col0_smallint in (\n" +
                "   select col1_smallint \n" +
                "   from db1.decimal_table \n" +
                "   group by col1_smallint \n" +
                "   having col1_smallint < 10)";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertTrue(stmt.whereClause.getType().isBoolean());
    }

    @Test
    public void testCastDecimal32ToDecimal32() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   cast(col0_decimal_p9s2 as decimal32(7, 4)), \n" +
                "   cast(col_nullable_decimal_p15s10 as decimal64(18, 16)), \n" +
                "   cast(col_decimal_p38s30 as decimal128(30, 7)) \n" +
                "from db1.decimal_table";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        ScalarType targetDecimal32Type = ScalarType.createDecimalV3Type(
                PrimitiveType.DECIMAL32, 7, 4);
        ScalarType targetDecimal64Type = ScalarType.createDecimalV3Type(
                PrimitiveType.DECIMAL64, 18, 16);
        ScalarType targetDecimal128Type = ScalarType.createDecimalV3Type(
                PrimitiveType.DECIMAL128, 30, 7);
        Assert.assertTrue(stmt.selectList.getItems().get(0).getExpr().getType().equals(targetDecimal32Type));
        Assert.assertTrue(stmt.selectList.getItems().get(1).getExpr().getType().equals(targetDecimal64Type));
        Assert.assertTrue(stmt.selectList.getItems().get(2).getExpr().getType().equals(targetDecimal128Type));
    }

    public void testDecimalTypedPredicatePushDownHelper(
            String predicate, Type targetType) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   sum(col_decimal_p38s30) as count\n" +
                "from db1.decimal_table\n" +
                "where\n" +
                "   " + predicate;

        //ctx.setQueryId(UUID.randomUUID());
        //ctx.setExecutionId(new TUniqueId());
        //StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        //stmtExecutor.execute();
        //Planner planner = stmtExecutor.planner();
        //List<PlanFragment> fragments = planner.getFragments();
        //String plan = planner.getExplainString(fragments, TExplainLevel.NORMAL);
        //System.out.println(plan);
        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr0 = stmt.whereClause.getChild(0).getChild(0);
            Expr expr1 = stmt.whereClause.getChild(1).getChild(0);
            Assert.assertTrue(expr0 instanceof SlotRef);
            Assert.assertTrue(expr1 instanceof SlotRef);
            Assert.assertEquals(expr0.type, targetType);
            Assert.assertEquals(expr1.type, targetType);
        }
        {
            Config.enable_decimal_v3 = false;
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr0 = stmt.whereClause.getChild(0).getChild(0);
            Expr expr1 = stmt.whereClause.getChild(1).getChild(0);
            Assert.assertTrue(expr0 instanceof SlotRef);
            Assert.assertTrue(expr1 instanceof SlotRef);
            Assert.assertEquals(expr0.type, targetType);
            Assert.assertEquals(expr1.type, targetType);
        }
    }

    @Test
    public void testDecimal128TypedBetweenPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p38s30 between -999.99 and 999.99";
        Type decimal128p38s30 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal128p38s30);
    }

    @Test
    public void testDecimal64TypedBetweenPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p15s10 between -999.99 and 999.99";
        Type decimal64p15s10 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal64p15s10);
    }

    @Test
    public void testDecimal32TypedBetweenPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p9s4 between -999.99 and 999.99";
        Type decimal32p9s4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal32p9s4);
    }

    @Test
    public void testDecimal128TypedCompoundPredicatePushDown() throws Exception {
        String predicate = "-999.99 < col_decimal_p38s30 and 999.99 > col_decimal_p38s30";
        Type decimal128p38s30 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal128p38s30);
    }

    @Test
    public void testDecimal64TypedCompoundPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p15s10 <= -999.99 or col_decimal_p15s10 >= 999.99";
        Type decimal64p15s10 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal64p15s10);
    }

    @Test
    public void testDecimal32TypedCompoundPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p9s4 != -999.99 and  col_decimal_p9s4 is NULL";
        Type decimal32p9s4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4);
        testDecimalTypedPredicatePushDownHelper(predicate, decimal32p9s4);
    }

    public void testDecimalTypedInPredicatePushDownHelper(
            String predicate, int n, Type targetType) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql1 = "" +
                "select\n" +
                "   count(*) as count\n" +
                "from db1.decimal_table\n" +
                "where\n" +
                "   " + predicate;
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Expr expr0 = stmt.whereClause.getChild(0);
        Assert.assertTrue(expr0 instanceof SlotRef);
        Assert.assertEquals(expr0.type, targetType);
        for (int i = 1; i <= n; ++i) {
            Expr expr = stmt.whereClause.getChild(i);
            Assert.assertEquals(expr0.type, targetType);
        }
    }

    @Test
    public void testDecimal128TypedInPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p38s30 in (999.999, -999.999, -1, 0, +1)";
        Type decimal128p38s30 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30);
        testDecimalTypedInPredicatePushDownHelper(predicate, 5, decimal128p38s30);
    }

    @Test
    public void testDecimal64TypedInPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p15s10 not in (0, 1)";
        Type decimal64p15s10 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10);
        testDecimalTypedInPredicatePushDownHelper(predicate, 2, decimal64p15s10);
    }

    @Test
    public void testDecimal32TypedIntPredicatePushDown() throws Exception {
        String predicate = "col_decimal_p9s4 not in (2, 4)";
        Type decimal32p9s4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4);
        testDecimalTypedInPredicatePushDownHelper(predicate, 2, decimal32p9s4);
    }

    public void testDecimalArithmeticHelper(String snippet, Type targetType) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   " + snippet + "\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.type, targetType);
        }
        {
            Config.enable_decimal_v3 = false;
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.type, targetType);
        }
    }

    @Test
    public void testSmallIntDivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col0_smallint/col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 6));
    }

    @Test
    public void testDecimal32p9s2DivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col1_decimal_p9s2/col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8));
    }

    @Test
    public void testDoubleDivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "cast('3.14' as double)/col0_decimal_p9s2",
                ScalarType.DOUBLE);
    }

    @Test
    public void testDecimalv2DivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal/col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 12));
    }

    @Test
    public void testDecimal128p38s3DivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col0_nullable_decimal_p38s3/col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9));
    }

    @Test
    public void testDecimal128p38s30DivDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30/col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal128p38s3DivDecimal128p38s3() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30/col1_nullable_decimal_p38s3",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal64p15s10DivDecimal64p15s10() throws Exception {
        testDecimalArithmeticHelper(
                "col_nullable_decimal_p15s10/col_decimal_p15s10",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 12));
    }

    @Test(expected = AnalysisException.class)
    public void testDecimal128p38s30DivDecimal64p15s10() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30/col_nullable_decimal_p15s10",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
        Assert.fail("should not reach here");
    }

    @Test(expected = AnalysisException.class)
    public void testDecimal128p38s30DivDecimal128p38s30() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30/col_decimal_p38s30",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
        Assert.fail("should not reach here");
    }

    @Test(expected = AnalysisException.class)
    public void testDecimal128p38s30DivDecimal32p9s9() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30/col_decimal_p9s9",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
        Assert.fail("should not reach here");
    }

    //
    @Test
    public void testDecimal32p9s2ModDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col1_decimal_p9s2%col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2));
    }

    public void testDoubleModDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "cast('3.14' as double)%col0_decimal_p9s2",
                ScalarType.DOUBLE);
    }

    @Test
    public void testDecimalv2ModDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal%col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9));
    }

    @Test
    public void testDecimal128p38s3ModDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col0_nullable_decimal_p38s3%col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 3));
    }

    @Test
    public void testDecimal128p38s30ModDecimal32p9s2() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30%col0_decimal_p9s2",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal128p38s3ModDecimal128p38s3() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30%col1_nullable_decimal_p38s3",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal64p15s10ModDecimal64p15s10() throws Exception {
        testDecimalArithmeticHelper(
                "col_nullable_decimal_p15s10%col_decimal_p15s10",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 10));
    }

    @Test
    public void testDecimal128p38s30ModDecimal64p15s10() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30%col_nullable_decimal_p15s10",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal128p38s30ModDecimal128p38s30() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30%col_decimal_p38s30",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testDecimal128p38s30ModDecimal32p9s9() throws Exception {
        testDecimalArithmeticHelper(
                "col_decimal_p38s30%col_decimal_p9s9",
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
    }

    @Test
    public void testBinaryPredicateEq() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   col0_nullable_decimal_p38s3 = 0.1 * col0_nullable_decimal_p38s3\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.type, Type.BOOLEAN);
            Assert.assertEquals(expr.getChild(0).type, ScalarType.DOUBLE);
            Assert.assertEquals(expr.getChild(1).type, ScalarType.DOUBLE);
        }
    }

    @Test
    public void testBinaryPredicateGe() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   col_decimal_p9s4 <= col0_decimal_p9s2 * col_nullable_decimal_p15s10\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.type, Type.BOOLEAN);
            Type decimal128p38s5 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 12);
            Assert.assertEquals(expr.getChild(0).type, decimal128p38s5);
            Assert.assertEquals(expr.getChild(1).type, decimal128p38s5);
        }
    }

    @Test
    public void testDecimalV3WindowFunction() throws Exception {
        String sql = "select sum(col_decimal_p9s4) over() from db1.decimal_table;";
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        Assert.assertFalse(plan.contains("not supported with OVER clause."));
    }

    @Test
    public void testAvgDecimal32() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   avg(col0_decimal_p9s2)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Type decimal128p38s8 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8);
            Type decimal32p9s2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2);
            Assert.assertEquals(expr.type, decimal128p38s8);
            Assert.assertEquals(expr.getChild(0).type, decimal32p9s2);
        }
    }

    @Test
    public void testAvgDecimal64() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   avg(col_nullable_decimal_p15s10)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Type decimal128p38s12 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 12);
            Type decimal64p15s10 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10);
            Assert.assertEquals(expr.type, decimal128p38s12);
            Assert.assertEquals(expr.getChild(0).type, decimal64p15s10);
        }
    }

    @Test
    public void testAvgDecimal128() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   avg(col_decimal_p38s30)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Type decimal128p38s30 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30);
            Assert.assertEquals(expr.type, decimal128p38s30);
            Assert.assertEquals(expr.getChild(0).type, decimal128p38s30);
        }
    }

    @Test
    public void testStddevAndVarianceOnDecimal() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   stddev(col0_decimal_p9s2)\n," +
                "   stddev_pop(col0_decimal_p9s2)\n," +
                "   stddev_samp(col0_decimal_p9s2)\n," +
                "   var_pop(col0_decimal_p9s2)\n," +
                "   var_samp(col0_decimal_p9s2)\n," +
                "   variance(col0_decimal_p9s2)\n," +
                "   variance_pop(col0_decimal_p9s2)\n," +
                "   variance_samp(col0_decimal_p9s2)\n," +
                "   stddev(col_nullable_decimal_p15s10)\n," +
                "   stddev_pop(col_nullable_decimal_p15s10)\n," +
                "   stddev_samp(col_nullable_decimal_p15s10)\n," +
                "   var_pop(col_nullable_decimal_p15s10)\n," +
                "   var_samp(col_nullable_decimal_p15s10)\n," +
                "   variance(col_nullable_decimal_p15s10)\n," +
                "   variance_pop(col_nullable_decimal_p15s10)\n," +
                "   variance_samp(col_nullable_decimal_p15s10)\n," +
                "   stddev(col_decimal_p38s30)\n," +
                "   stddev_pop(col_decimal_p38s30)\n," +
                "   stddev_samp(col_decimal_p38s30)\n," +
                "   var_pop(col_decimal_p38s30)\n," +
                "   var_samp(col_decimal_p38s30)\n," +
                "   variance(col_decimal_p38s30)\n," +
                "   variance_pop(col_decimal_p38s30)\n," +
                "   variance_samp(col_decimal_p38s30)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            List<SelectListItem> items = stmt.selectList.getItems();
            Assert.assertEquals(items.size(), 24);
            Type decimal128p38s9 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
            for (int i = 0; i < items.size(); ++i) {
                Expr expr = items.get(i).getExpr();
                Assert.assertEquals(expr.type, decimal128p38s9);
                Assert.assertEquals(((FunctionCallExpr) expr).fn.getArgs()[0], decimal128p38s9);
                Assert.assertEquals(((FunctionCallExpr) expr).fn.getReturnType(), decimal128p38s9);
                Assert.assertEquals(((AggregateFunction) ((FunctionCallExpr) expr).fn).getIntermediateType(),
                        Type.VARCHAR);
                Assert.assertEquals(expr.getChild(0).type, decimal128p38s9);
            }
        }
    }

    @Test
    public void testCoalesce() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   coalesce(cast('999.99' as decimal32(9,3)), col0_decimal_p9s2)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            List<SelectListItem> items = stmt.selectList.getItems();
            Type decimal64p9s3 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 3);
            Assert.assertEquals(items.get(0).getExpr().getType(), decimal64p9s3);
            Assert.assertEquals(items.get(0).getExpr().getChild(0).getType(), decimal64p9s3);
            Assert.assertEquals(items.get(0).getExpr().getChild(1).getType(), decimal64p9s3);
        }
    }

    @Test
    public void testIntDiv() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   col_decimal_p9s9 div 3.14\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            List<SelectListItem> items = stmt.selectList.getItems();
            Assert.assertEquals(items.get(0).getExpr().getType(), Type.BIGINT);
        }
    }

    @Test
    public void testIfnullGreatestLeastCoalesce() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   greatest(col_decimal_p9s9, 3.14),\n" +
                "   ifnull(col_nullable_decimal_p15s10, col_decimal_p9s9),\n" +
                "   least(3.1415926, col0_nullable_decimal_p38s3),\n" +
                "   coalesce(col_decimal_p38s30, 0)\n" +
                "from db1.decimal_table\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            List<SelectListItem> items = stmt.selectList.getItems();
            Assert.assertEquals(items.get(0).getExpr().getType(),
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 9));
            Assert.assertEquals(items.get(1).getExpr().getType(),
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10));
            Assert.assertEquals(items.get(2).getExpr().getType(), ScalarType.DOUBLE);
            Assert.assertEquals(items.get(3).getExpr().getType(),
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 30));
        }
    }

    @Test
    public void testWindowDecimalV3() throws Exception {
        String sql = "select key0, " +
                "first_value(col0_decimal_p9s2) over( partition by key0) from db1.decimal_table";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL32, precision:9, scale:2))"));
    }

    @Test
    public void testGetCommonTypeForDecimalAndString() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   greatest(col_decimal_p9s9, 3.14),\n" +
                "   ifnull(col_nullable_decimal_p15s10, col_decimal_p9s9),\n" +
                "   least(3.1415926, col0_nullable_decimal_p38s3),\n" +
                "   coalesce(col_decimal_p38s30, 0)\n" +
                "from db1.decimal_table where col_nullable_decimal_p15s10 = '2.7'\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.getWhereClause();
            Assert.assertTrue(expr.getChild(1) instanceof DecimalLiteral);
            Assert.assertEquals(((DecimalLiteral) expr.getChild(1)).getStringValue(), "2.7");
        }
    }

    @Test
    public void testGetCommonTypeForDecimalAndStringOverflow() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   greatest(col_decimal_p9s9, 3.14),\n" +
                "   ifnull(col_nullable_decimal_p15s10, col_decimal_p9s9),\n" +
                "   least(3.1415926, col0_nullable_decimal_p38s3),\n" +
                "   coalesce(col_decimal_p38s30, 0)\n" +
                "from db1.decimal_table where col_nullable_decimal_p15s10 = '2.5E307'\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.getWhereClause();
            Assert.assertTrue(expr.getChild(1) instanceof FloatLiteral);
            Assert.assertEquals(((FloatLiteral) expr.getChild(1)).getDoubleValue(), 2.5E307, 1E-10);
        }
    }

    @Test
    public void testGetCommonTypeForDecimalAndStringHighPrecision() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "" +
                "select\n" +
                "   greatest(col_decimal_p9s9, 3.14),\n" +
                "   ifnull(col_nullable_decimal_p15s10, col_decimal_p9s9),\n" +
                "   least(3.1415926, col0_nullable_decimal_p38s3),\n" +
                "   coalesce(col_decimal_p38s30, 0)\n" +
                "from db1.decimal_table where col_nullable_decimal_p15s10 = '2.123456789012345'\n";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr expr = stmt.getWhereClause();
            Assert.assertEquals(expr.getChild(0).getType(), ScalarType.createDecimalV3NarrowestType(20, 15));
            Assert.assertEquals(expr.getChild(1).getType(), ScalarType.createDecimalV3NarrowestType(20, 15));
        }
    }

    @Test
    public void testDecimalV3LiteralCast() throws Exception {
        String sql = "select * from db1.decimal_table WHERE CAST(IF(true, 0.38542880072101215, '-Inf')  AS BOOLEAN )";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));

        sql = "select * from db1.decimal_table WHERE CAST(ifnull(0.38542880072101215, '-Inf')  AS BOOLEAN )";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));

        sql = "select * from db1.decimal_table WHERE CAST(COALESCE(0.38542880072101215, '-Inf', 0.38542)  AS BOOLEAN )";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));

        sql = "select * from db1.decimal_table WHERE CAST(greatest(0.38542880072101215, '-Inf', 0.38542)  AS BOOLEAN )";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));

        sql = "select * from db1.decimal_table WHERE CAST(least(0.38542880072101215, '-Inf', 0.38542)  AS BOOLEAN )";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));
    }

    @Test
    public void testInnerJoinOnDecimalV3() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        String sql = "select * \n" +
                "from db1.decimal_table as t0 inner join db1.decimal_table as t1 \n" +
                "on t0.col0_nullable_decimal_p38s3 = t1.col_date";

        {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
            Expr lhs = stmt.fromClause_.get(1).onClause.getChild(0);
            Expr rhs = stmt.fromClause_.get(1).onClause.getChild(1);
            Assert.assertTrue(lhs instanceof CastExpr);
            Assert.assertTrue(rhs instanceof CastExpr);
            Assert.assertEquals(lhs.getType(), ScalarType.DOUBLE);
            Assert.assertEquals(rhs.getType(), ScalarType.DOUBLE);
        }
    }

    public void testDecimalV3BitwiseOperation() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;

        String[] sqls = {
                "select col_decimal_p15s10 & 1.0 from db1.decimal_table",
                "select col_decimal_p15s10 | col0_smallint from db1.decimal_table",
                "select col_decimal_p15s10 ^ cast('3.14' as double) from db1.decimal_table",
        };
        for (String sql : sqls) {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());

            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.getType(), ScalarType.BIGINT);
        }
    }

    @Test
    public void testNullAndDecimalV3Operation() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;

        String[] sqls = {
                "select col_decimal_p15s10 + NULL from db1.decimal_table",
                "select col_decimal_p15s10 - NULL from db1.decimal_table",
        };
        for (String sql : sqls) {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());

            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.getType(), ScalarType.createDecimalV3NarrowestType(18, 10));
        }
    }

    @Test
    public void testIntDivideDecimalV3Operation() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;

        String[] sqls = {
                "select 10 div col_decimal_p15s10 from db1.decimal_table",
                "select 10.11 div col_decimal_p15s10 from db1.decimal_table",
                "select '10.112' div col_decimal_p15s10 from db1.decimal_table",
        };
        for (String sql : sqls) {
            SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());

            Expr expr = stmt.selectList.getItems().get(0).getExpr();
            Assert.assertEquals(expr.getType(), ScalarType.BIGINT);
        }
    }
}
