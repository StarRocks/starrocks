// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

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

import java.util.UUID;

public class SelectStmtWithDecimalTypesNewPlannerTest {
    private static final String runningDir =
            "fe/mocked/DecimalDemoTestNewPlanner/" + UUID.randomUUID().toString() + "/";
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
                "col_decimal32p9s2 DECIMAL32(9,2) NOT NULL,\n" +
                "col_decimal64p13s0 DECIMAL64(13,0) NOT NULL,\n" +
                "col_double DOUBLE,\n" +
                "col_decimal128p20s3 DECIMAL128(20, 3)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";

        ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testNullif() throws Exception {
        String sql = "select  * from db1.decimal_table where 6 > nullif(col_decimal128p20s3, cast(null as DOUBLE))";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:nullif), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "has_var_args:false, signature:nullif(DOUBLE, DOUBLE), scalar_fn:TScalarFunction(symbol:), " +
                "id:0, fid:70307";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testCoalesce() throws Exception {
        String sql = "select avg(coalesce(col_decimal128p20s3, col_double)) from db1.decimal_table";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:coalesce), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "has_var_args:true, signature:coalesce(DOUBLE...), scalar_fn:TScalarFunction(symbol:), " +
                "id:0, fid:70407";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testIf() throws Exception {
        String sql = " select  if(1, cast('3.14' AS decimal32(9, 2)), cast('1.9999' AS decimal32(5, 4))) " +
                "AS res0 from db1.decimal_table;";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(
                "type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))"));

        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(
                "type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))"));
    }

    @Test
    public void testMoneyFormat() throws Exception {
        String sql = "select money_format(col_decimal128p20s3) from db1.decimal_table";
        String expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, signature:" +
                        "money_format(DECIMAL128(20,3)), scalar_fn:" +
                        "TScalarFunction(symbol:)" +
                        ", id:0, fid:304022";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc(types:" +
                        "[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, " +
                        "signature:money_format(DECIMAL128(20,3)), scalar_fn:TScalarFunction(symbol:), id:0, fid:304022";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testMultiply() throws Exception {
        String sql = "select col_decimal128p20s3 * 3.14 from db1.decimal_table";
        String expectString =
                "TPlanFragment(plan:TPlan(nodes:[TPlanNode(node_id:1, node_type:PROJECT_NODE, num_children:1, limit:-1, row_tuples:[1], nullable_tuples:[false], compact_data:false, use_vectorized:true, project_node:TProjectNode(slot_map:{6=TExpr(nodes:[TExprNode(node_type:ARITHMETIC_EXPR, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:23, scale:5))]), opcode:MULTIPLY, num_children:2, output_scale:-1, output_column:-1, use_vectorized:true, has_nullable_child:true, is_nullable:true, is_monotonic:true), TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))]), num_children:0, slot_ref:TSlotRef(slot_id:5, tuple_id:0), output_scale:-1, output_column:-1, use_vectorized:true, has_nullable_child:false, is_nullable:true, is_monotonic:true), TExprNode(node_type:DECIMAL_LITERAL, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:3, scale:2))]), num_children:0, decimal_literal:TDecimalLiteral(value:3.14), output_scale:-1, use_vectorized:true, has_nullable_child:false, is_nullable:false, is_monotonic:true)])})), TPlanNode(node_id:0, node_type:OLAP_SCAN_NODE, num_children:0, limit:-1, row_tuples:[0], nullable_tuples:[false], compact_data:false, olap_scan_node:TOlapScanNode(tuple_id:0, key_column_name:[key0], key_column_type:[INT], is_preaggregation:true, rollup_name:decimal_table, enable_column_expr_predicate:false, dict_string_id_to_int_ids:{}), use_vectorized:true)]), output_exprs:[TExpr(nodes:[TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:23, scale:5))]), num_children:0, slot_ref:TSlotRef(slot_id:6, tuple_id:1), output_scale:-1, output_column:-1, use_vectorized:true, has_nullable_child:false, is_nullable:true, is_monotonic:true)])], output_sink:TDataSink(type:RESULT_SINK, result_sink:TResultSink(type:MYSQL_PROTOCAL)), partition:TDataPartition(type:RANDOM, partition_exprs:[]))";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        System.out.println(thrift);

        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testMod() throws Exception {
        String sql = "select mod(0.022330165, NULL) as result from db1.decimal_table";
        String expectString = "TScalarType(type:DECIMAL32, precision:9, scale:9))";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testCountDecimal() throws Exception {
        String sql = "select count(col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("args: DECIMAL128; result: BIGINT;"));

        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains("arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))"));
    }

    @Test
    public void testDecimalBinaryPredicate() throws Exception {
        String sql = "select col_decimal64p13s0 > -9.223372E+18 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "cast([3: col_decimal64p13s0, DECIMAL64(13,0), false] as DECIMAL128(19,0)) > -9223372000000000000";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalInPredicates() throws Exception {
        String sql = "select * from db1.decimal_table where col_decimal64p13s0 in (0, 1, 9999, -9.223372E+18)";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "CAST(3: col_decimal64p13s0 AS DECIMAL128(19,0))" +
                " IN (0, 1, 9999, -9223372000000000000)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalBetweenPredicates() throws Exception {
        String sql = "select * from db1.decimal_table where col_decimal64p13s0 between -9.223372E+18 and 9.223372E+18";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "cast([3: col_decimal64p13s0, DECIMAL64(13,0), false] as DECIMAL128(19,0)) >= -9223372000000000000, " +
                        "cast([3: col_decimal64p13s0, DECIMAL64(13,0), false] as DECIMAL128(19,0)) <= 9223372000000000000";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Sum() throws Exception {
        String sql = "select sum(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: DECIMAL64(18,2); args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal64Sum() throws Exception {
        String sql = "select sum(col_decimal64p13s0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: DECIMAL64(18,0); args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal128Sum() throws Exception {
        String sql = "select sum(col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: DECIMAL128(38,3)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Avg() throws Exception {
        String sql = "select avg(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "avg[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: DECIMAL128(38,8)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal64Avg() throws Exception {
        String sql = "select avg(col_decimal64p13s0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "avg[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: DECIMAL128(38,6)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal128Avg() throws Exception {
        String sql = "select avg(col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "avg[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: DECIMAL128(38,9)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32MultiDistinctSum() throws Exception {
        String sql = "select multi_distinct_sum(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: DECIMAL64(18,2); args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal64MultiDistinctSum() throws Exception {
        String sql = "select multi_distinct_sum(col_decimal64p13s0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: DECIMAL64(18,0); args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal128MultiDistinctSum() throws Exception {
        String sql = "select multi_distinct_sum(col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "multi_distinct_sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: DECIMAL128(38,3)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Count() throws Exception {
        String sql = "select count(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "count[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: BIGINT";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Stddev() throws Exception {
        String sql = "select stddev(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "stddev[(cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,9))); args: DECIMAL128; result: DECIMAL128(38,9)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Variance() throws Exception {
        String sql = "select variance(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "variance[(cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,9))); args: DECIMAL128; result: DECIMAL128(38,9)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalAddNULL() throws Exception {
        String sql = "select col_decimal32p9s2 + NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) + NULL";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalSubNULL() throws Exception {
        String sql = "select col_decimal32p9s2 - NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) - NULL";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalMulNULL() throws Exception {
        String sql = "select col_decimal32p9s2 * NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> [2: col_decimal32p9s2, DECIMAL32(9,2), false] * NULL";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalDivNULL() throws Exception {
        String sql = "select col_decimal32p9s2 / NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2)) / NULL";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalModNULL() throws Exception {
        String sql = "select col_decimal32p9s2 % NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) % NULL";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testNULLDivDecimal() throws Exception {
        String sql = "select NULL / col_decimal32p9s2 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> NULL / cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2))";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testNULLModDecimal() throws Exception {
        String sql = "select NULL % col_decimal32p9s2 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> NULL % cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2))";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalAddZero() throws Exception {
        String sql = "select col_decimal32p9s2 + 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) + 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalSubZero() throws Exception {
        String sql = "select col_decimal32p9s2 - 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) - 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalMulZero() throws Exception {
        String sql = "select col_decimal32p9s2 * 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> [2: col_decimal32p9s2, DECIMAL32(9,2), false] * 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalDivZero() throws Exception {
        String sql = "select col_decimal32p9s2 / 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2)) / 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalModZero() throws Exception {
        String sql = "select col_decimal32p9s2 % 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2)) % 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testZeroDivDecimal() throws Exception {
        String sql = "select 0.0 / col_decimal32p9s2 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> 0 / cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2))";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testZeroModDecimal() throws Exception {
        String sql = "select 0.0 % col_decimal32p9s2 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> 0 % cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2))";
        Assert.assertTrue(plan.contains(snippet));
    }
}

