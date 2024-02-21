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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.regex.Pattern;

public class SelectStmtWithDecimalTypesNewPlannerTest {
    private static ConnectContext ctx;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
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
                "\"in_memory\" = \"false\"\n" +
                ");";

        ctx = UtFrameUtils.createDefaultCtx();
        Config.enable_decimal_v3 = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.getCtx().getSessionVariable().setCboPushDownAggregateMode(-1);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withTable("CREATE TABLE `test_decimal_type6` (\n" +
                "  `dec_2_1` decimal32(2, 1) NOT NULL COMMENT \"\",\n" +
                "  `dec_18_0` decimal64(18, 0) NOT NULL COMMENT \"\",\n" +
                "  `dec_18_18` decimal64(18, 18) NOT NULL COMMENT \"\",\n" +
                "  `dec_10_2` decimal64(10, 2) NOT NULL COMMENT \"\",\n" +
                "  `dec_12_10` decimal64(12, 10) NOT NULL COMMENT \"\",\n" +
                "   dec_20_3 decimal128(20, 3),\n" +
                "   dec_20_19 decimal128(20, 19)," +
                "   dec_4_2 decimal64(4, 2),\n" +
                "   dec_5_1 decimal64(5, 1)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dec_2_1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`dec_2_1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\"\n" +
                ");");
        FeConstants.enablePruneEmptyOutputScan = false;
    }

    private static String removeSlotIds(String s) {
        Pattern removeSlotIdPattern = Pattern.compile("((?<=\\[)\\b\\d+\\b:\\s*)|(\\b\\d+\\b\\s*<->\\s*)");
        return s.replaceAll(removeSlotIdPattern.pattern(), "");
    }

    @Test
    public void testNullif() throws Exception {
        String sql = "select  * from db1.decimal_table where 6 > nullif(col_decimal128p20s3, cast(null as DOUBLE))";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:nullif), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "has_var_args:false, scalar_fn:TScalarFunction(symbol:), " +
                "id:70307, fid:70307";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift, thrift.contains(expectString));
    }

    @Test
    public void testCoalesce() throws Exception {
        String sql = "select avg(coalesce(col_decimal128p20s3, col_double)) from db1.decimal_table";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:coalesce), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), ";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift, thrift.contains(expectString));
    }

    @Test
    public void testIf() throws Exception {
        String sql = " select  if(1, cast('3.14' AS decimal32(9, 2)), cast('1.9999' AS decimal32(5, 4))) " +
                "AS res0 from db1.decimal_table;";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift, thrift.contains(
                "type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                        "scalar_type:TScalarType(type:DECIMAL64, precision:11, scale:4))"));
    }

    @Test
    public void testMoneyFormat() throws Exception {
        String sql = "select money_format(col_decimal128p20s3) from db1.decimal_table";
        String expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, scalar_fn:";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc(types:" +
                        "[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, " +
                        "scalar_fn:TScalarFunction(symbol:), id:304022, fid:304022";
        thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testMultiply() throws Exception {
        String sql = "select col_decimal128p20s3 * 3.14 from db1.decimal_table";
        String expectString =
                "TExpr(nodes:[TExprNode(node_type:ARITHMETIC_EXPR, type:TTypeDesc(types:[TTypeNode(type:SCALAR," +
                        " scalar_type:TScalarType(type:DECIMAL128, precision:23, scale:5))]), opcode:MULTIPLY, num_children:2, " +
                        "output_scale:-1, output_column:-1, has_nullable_child:true, is_nullable:true, is_monotonic:true), " +
                        "TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:" +
                        "DECIMAL128, precision:20, scale:3))]), num_children:0, slot_ref:TSlotRef(slot_id:5, tuple_id:0), " +
                        "output_scale:-1, output_column:-1, has_nullable_child:false, is_nullable:true, is_monotonic:true), " +
                        "TExprNode(node_type:DECIMAL_LITERAL, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType" +
                        "(type:DECIMAL128, precision:3, scale:2))]), num_children:0, decimal_literal:TDecimalLiteral(value:3.14, " +
                        "integer_value:3A 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00), output_scale:-1, has_nullable_child:false, " +
                        "is_nullable:false, is_monotonic:true)])})";
        String plan = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assert.assertTrue(plan.contains(expectString));
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
        String snippet = "cast([3: col_decimal64p13s0, DECIMAL64(13,0), false] as DECIMAL128(19,0)) " +
                "> -9223372000000000000";
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
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        String snippet =
                "PREDICATES: CAST(3: col_decimal64p13s0 AS DECIMAL128(19,0)) >= -9223372000000000000, CAST(3: col_decimal64p13s0 AS DECIMAL128(19,0)) <= 9223372000000000000";
        Assert.assertTrue(plan, plan.contains(snippet));
    }

    @Test
    public void testDecimal32Sum() throws Exception {
        String sql = "select sum(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: DECIMAL128(38,2)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal64Sum() throws Exception {
        String sql = "select sum(col_decimal64p13s0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: DECIMAL128(38,0)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal128Sum() throws Exception {
        String sql = "select sum(col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: DECIMAL128(38,3)";
        Assert.assertTrue(plan.contains(snippet));

        sql = "select sum(dec_20_19) from test_decimal_type6";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(
                "sum[(cast([7: dec_20_19, DECIMAL128(20,19), true] as DECIMAL128(38,18))); args: DECIMAL128; result: DECIMAL128(38,18); args nullable: true; result nullable: true]"));
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
                "multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: DECIMAL128(38,2)";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal64MultiDistinctSum() throws Exception {
        String sql = "select multi_distinct_sum(col_decimal64p13s0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet =
                "multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: DECIMAL128(38,0)";
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
        String snippet = "aggregate: stddev[(cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DOUBLE)); " +
                "args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimal32Variance() throws Exception {
        String sql = "select variance(col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "aggregate: variance[(cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DOUBLE)); " +
                "args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalAddNULL() throws Exception {
        String sql = "select col_decimal32p9s2 + NULL from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(10,2)) + NULL";
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
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(10,2)) + 0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalAddRule() throws Exception {
        String sql;
        String plan;

        // decimal(10, 2) + decimal(10, 2) = decimal(11, 2)
        sql = "select count(`dec_10_2` + dec_10_2) from `test_decimal_type6`;";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("[12: cast, DECIMAL64(11,2), false] + [12: cast, DECIMAL64(11,2), false]"));

        sql = "select count(`dec_10_2` + dec_12_10) from `test_decimal_type6`;";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        // decimal64(10, 2) + decimal64(12, 10) = decimal128(21, 10);
        Assert.assertTrue(plan.contains(
                "cast([4: dec_10_2, DECIMAL64(10,2), false] as DECIMAL128(19,2)) + cast([5: dec_12_10, DECIMAL64(12,10), false] as DECIMAL128(19,10))"));
        // decimal64(18, 0) + decimal64(18, 18) = decimal(37, 18)
        sql = "select count(dec_18_0 + dec_18_18) from `test_decimal_type6`";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(
                "cast([2: dec_18_0, DECIMAL64(18,0), false] as DECIMAL128(37,0)) + cast([3: dec_18_18, DECIMAL64(18,18), false] as DECIMAL128(37,18))"));
        // const decimal64(18, 0) + decimal64(18, 18) = decimal128(37, 18) literal
        sql = "select cast(1000000000000000000 as decimal(18, 0)) + cast(0.000000000000000001 as decimal(18, 18))";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(" 2 <-> 1000000000000000000.000000000000000001"));

        sql = "select dec_2_1 + dec_2_1 from test_decimal_type6";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("[11: cast, DECIMAL32(3,1), false] + [11: cast, DECIMAL32(3,1), false]"));
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
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(9,2)) * 0.0";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testDecimalDivZero() throws Exception {
        String sql = "select col_decimal32p9s2 / 0.0 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2)) / 0.0";
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
        String snippet = "6 <-> 0.0 / cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL128(38,2))";
        Assert.assertTrue(plan.contains(snippet));
    }

    @Test
    public void testZeroModDecimal() throws Exception {
        String sql = "select 0.0 % col_decimal32p9s2 from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String snippet = "6 <-> 0.0 % cast([2: col_decimal32p9s2, DECIMAL32(9,2), false] as DECIMAL64(18,2))";
        Assert.assertTrue(plan, plan.contains(snippet));
    }

    @Test
    public void testDecimalNullableProperties() throws Exception {
        String sql;
        String plan;

        // test decimal count(no-nullable decimal)
        sql = "select count(`dec_18_0`) from `test_decimal_type6`;";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan, plan.contains(
                "aggregate: count[([2: dec_18_0, DECIMAL64(18,0), false]); args: DECIMAL64; result: BIGINT; args nullable: false; result nullable: false]"));

        // test decimal add return a nullable column
        sql = "select count(`dec_18_0` + `dec_18_18`) from `test_decimal_type6`;";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(
                "cast([2: dec_18_0, DECIMAL64(18,0), false] as DECIMAL128(37,0)) + cast([3: dec_18_18, DECIMAL64(18,18), false] as DECIMAL128(37,18))"));

        // test decimal input function input no-nullable, output is nullable
        sql = "select round(`dec_18_0`) from `test_decimal_type6`";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(
                "round[(cast([2: dec_18_0, DECIMAL64(18,0), false] as DECIMAL128(18,0))); args: DECIMAL128; result: DECIMAL128(38,0); args nullable: false; result nullable: true]"));
    }

    @Test
    public void testDoubleLiteralMul() throws Exception {
        String sql;
        String plan;
        sql = "select 123456789000000000000000000000000000.00 * 123456789.123456789 * 123456789.123456789;";
        final long sqlMode = ctx.getSessionVariable().getSqlMode();
        final long code = SqlModeHelper.encode("MODE_DOUBLE_LITERAL");
        ctx.getSessionVariable().setSqlMode(code | sqlMode);
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("  |  2 <-> 1.8816763755525075E51"));
        ctx.getSessionVariable().setSqlMode(sqlMode);
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains(
                "  |  2 <-> 123456789000000000000000000000000000.00 * 123456789.123456789 * 123456789.123456789"));
    }

    @Test
    public void testTruncate() throws Exception {
        String sql = "select TRUNCATE(0.6798916342905857, TIMEDIFF('1969-12-27 10:06:26', '1969-12-09 21:35:14'));";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("truncate[(0.6798916342905857, cast(1513872.0 as INT)); args: DOUBLE,INT; " +
                "result: DOUBLE; args nullable: true; result nullable: true]"));

        sql = "select TRUNCATE(0.6798916342905857, '10');";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("truncate[(0.6798916342905857, 10); args: DOUBLE,INT; result: DOUBLE; " +
                "args nullable: false; result nullable: true]"));

        sql = "select TRUNCATE(0.6798916342905857, to_base64('abc'));";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("truncate[(0.6798916342905857, cast(to_base64[('abc'); args: VARCHAR; " +
                "result: VARCHAR; args nullable: false; result nullable: true] as INT)); " +
                "args: DOUBLE,INT; result: DOUBLE; args nullable: true; result nullable: true]"));

        sql = "select TRUNCATE(0.6798916342905857, 3.13);";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("truncate[(0.6798916342905857, cast(3.13 as INT)); args: DOUBLE,INT; " +
                "result: DOUBLE; args nullable: true; result nullable: true]"));

        sql = "select TRUNCATE(0.6798916342905857, cast('2000-01-31 12:00:00' as datetime));";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan.contains("truncate[(0.6798916342905857, cast('2000-01-31 12:00:00' as INT)); " +
                "args: DOUBLE,INT; result: DOUBLE; args nullable: true; result nullable: true]"));
    }

    @Test
    public void testOnePhaseSumDistinctDecimal() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(1);

        String sql = "select sum(distinct col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String expectSnippet = "aggregate: multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: DECIMAL128(38,2)";
        Assert.assertTrue(plan.contains(expectSnippet));

        sql = "select sum(distinct col_decimal64p13s0) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        expectSnippet = "aggregate: multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: DECIMAL128(38,0)";
        Assert.assertTrue(plan.contains(expectSnippet));


        sql = "select sum(distinct col_decimal128p20s3) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        expectSnippet = "multi_distinct_sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); " +
                "args: DECIMAL128; result: DECIMAL128(38,3)";
        Assert.assertTrue(plan.contains(expectSnippet));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }


    @Test
    public void testTwoPhaseSumDistinct() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select sum(distinct col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String expectPhase1Snippet = "multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY";
        String expectPhase2Snippet = "multi_distinct_sum[([6: sum, VARBINARY, true]); " +
                "args: DECIMAL32; result: DECIMAL128(38,2)";
        Assert.assertTrue(plan.contains(expectPhase1Snippet) && plan.contains(expectPhase2Snippet));

        sql = "select sum(distinct col_decimal64p13s0) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        expectPhase1Snippet = "multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY";
        expectPhase2Snippet = "multi_distinct_sum[([6: sum, VARBINARY, true]); " +
                "args: DECIMAL64; result: DECIMAL128(38,0)";
        Assert.assertTrue(plan.contains(expectPhase1Snippet) && plan.contains(expectPhase2Snippet));


        sql = "select sum(distinct col_decimal128p20s3) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        expectPhase1Snippet = "multi_distinct_sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); " +
                "args: DECIMAL128; result: VARBINARY";
        expectPhase2Snippet = "multi_distinct_sum[([6: sum, VARBINARY, true]); " +
                "args: DECIMAL128; result: DECIMAL128(38,3)";
        Assert.assertTrue(plan.contains(expectPhase1Snippet) && plan.contains(expectPhase2Snippet));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }

    @Test
    public void testSumDistinctWithRewriteMultiDistinctRuleTakeEffect() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        boolean oldCboCteReUse = ctx.getSessionVariable().isCboCteReuse();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        ctx.getSessionVariable().setCboCteReuse(false);
        String sql = "select sum(distinct col_decimal32p9s2), sum(distinct col_decimal64p13s0), " +
                "sum(distinct col_decimal128p20s3) from db1.decimal_table";
        String expectPhase1Snippet = "multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY; args nullable: false; result nullable: true], " +
                "multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY; args nullable: false; result nullable: true], " +
                "multi_distinct_sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); " +
                "args: DECIMAL128; result: VARBINARY; args nullable: true; result nullable: true]";
        String expectPhase2Snippet = "multi_distinct_sum[([6: sum, VARBINARY, true]); " +
                "args: DECIMAL32; result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                "multi_distinct_sum[([7: sum, VARBINARY, true]); " +
                "args: DECIMAL64; result: DECIMAL128(38,0); args nullable: true; result nullable: true], " +
                "multi_distinct_sum[([8: sum, VARBINARY, true]); " +
                "args: DECIMAL128; result: DECIMAL128(38,3); " +
                "args nullable: true; result nullable: true]";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan, plan.contains(expectPhase1Snippet) && plan.contains(expectPhase2Snippet));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
        ctx.getSessionVariable().setCboCteReuse(oldCboCteReUse);
    }

    @Test
    public void testSumDistinctWithRewriteMultiDistinctByCTERuleTakeEffect() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select sum(distinct col_decimal32p9s2), sum(distinct col_decimal64p13s0), " +
                "sum(distinct col_decimal128p20s3) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String[] expectSnippets = {
                "multi_distinct_sum[([9: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: VARBINARY",
                "multi_distinct_sum[([10: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: VARBINARY",
                "multi_distinct_sum[([11: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: VARBINARY",
                "multi_distinct_sum[([6: sum, VARBINARY, true]); args: DECIMAL32; result: DECIMAL128(38,2)",
                "multi_distinct_sum[([7: sum, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,0)",
                "multi_distinct_sum[([8: sum, VARBINARY, true]); args: DECIMAL128; result: DECIMAL128(38,3)",
        };
        Assert.assertTrue(Arrays.asList(expectSnippets).stream().allMatch(s -> plan.contains(s)));
        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }

    @Test
    public void testOnePhaseAvgDistinctDecimal() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(1);

        String sql = "select avg(distinct col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String localAggSnippet = "aggregate: avg[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY; args nullable: false; result nullable: true]";
        String globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL32; " +
                "result: DECIMAL128(38,8); args nullable: true; result nullable: true]";
        Assert.assertTrue(plan, plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));

        sql = "select avg(distinct col_decimal64p13s0) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        localAggSnippet = "aggregate: avg[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY;";
        globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,6); " +
                "args nullable: true; result nullable: true";
        Assert.assertTrue(plan,plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));

        sql = "select avg(distinct col_decimal128p20s3) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        localAggSnippet = "aggregate: avg[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; " +
                "result: VARBINARY; args nullable: true; result nullable: true";
        globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL128; result: DECIMAL128(38,9);" +
                " args nullable: true; result nullable: true]";
        Assert.assertTrue(plan,plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }


    @Test
    public void testTwoPhaseAvgDistinct() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select avg(distinct col_decimal32p9s2) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        String localAggSnippet = "aggregate: avg[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY; args nullable: false; result nullable: true]";
        String globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL32; " +
                "result: DECIMAL128(38,8); args nullable: true; result nullable: true]";
        Assert.assertTrue(plan, plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));

        sql = "select avg(distinct col_decimal64p13s0) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        localAggSnippet = "aggregate: avg[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY;";
        globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,6); " +
                "args nullable: true; result nullable: true";
        Assert.assertTrue(plan,plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));

        sql = "select avg(distinct col_decimal128p20s3) from db1.decimal_table";
        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        localAggSnippet = "aggregate: avg[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; " +
                "result: VARBINARY; args nullable: true; result nullable: true";
        globalAggSnippet = "aggregate: avg[([6: avg, VARBINARY, true]); args: DECIMAL128; result: DECIMAL128(38,9);" +
                " args nullable: true; result nullable: true]";
        Assert.assertTrue(plan,plan.contains(localAggSnippet) && plan.contains(globalAggSnippet));
        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }

    @Test
    public void testAvgDistinctWithRewriteMultiDistinctRuleTakeEffect() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        boolean oldCboCteReUse = ctx.getSessionVariable().isCboCteReuse();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        ctx.getSessionVariable().setCboCteReuse(false);
        String sql = "select avg(distinct col_decimal32p9s2), avg(distinct col_decimal64p13s0), " +
                "avg(distinct col_decimal128p20s3) from db1.decimal_table";
        String expectPhase1Snippet = "aggregate: " +
                "multi_distinct_count[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY; args nullable: false; result nullable: false], " +
                "multi_distinct_sum[([2: col_decimal32p9s2, DECIMAL32(9,2), false]); " +
                "args: DECIMAL32; result: VARBINARY; args nullable: false; result nullable: true], " +
                "multi_distinct_count[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY; args nullable: false; result nullable: false], " +
                "multi_distinct_sum[([3: col_decimal64p13s0, DECIMAL64(13,0), false]); " +
                "args: DECIMAL64; result: VARBINARY; args nullable: false; result nullable: true], " +
                "multi_distinct_count[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); " +
                "args: DECIMAL128; result: VARBINARY; args nullable: true; result nullable: false], " +
                "multi_distinct_sum[([5: col_decimal128p20s3, DECIMAL128(20,3), true]); " +
                "args: DECIMAL128; result: VARBINARY; args nullable: true; result nullable: true]";

        String expectPhase2Snippet = "aggregate: " +
                "multi_distinct_count[([9: multi_distinct_count, VARBINARY, false]); " +
                "args: DECIMAL32; result: BIGINT; args nullable: true; result nullable: false], " +
                "multi_distinct_sum[([10: multi_distinct_sum, VARBINARY, true]); " +
                "args: DECIMAL32; result: DECIMAL128(38,2); args nullable: true; result nullable: true], " +
                "multi_distinct_count[([11: multi_distinct_count, VARBINARY, false]); " +
                "args: DECIMAL64; result: BIGINT; args nullable: true; result nullable: false], " +
                "multi_distinct_sum[([12: multi_distinct_sum, VARBINARY, true]); " +
                "args: DECIMAL64; result: DECIMAL128(38,0); args nullable: true; result nullable: true], " +
                "multi_distinct_count[([13: multi_distinct_count, VARBINARY, false]); " +
                "args: DECIMAL128; result: BIGINT; args nullable: true; result nullable: false], " +
                "multi_distinct_sum[([14: multi_distinct_sum, VARBINARY, true]); " +
                "args: DECIMAL128; result: DECIMAL128(38,3); args nullable: true; result nullable: true]";
        String projectOutputColumns = "" +
                "  |  6 <-> [10: multi_distinct_sum, DECIMAL128(38,2), true] / " +
                "cast([9: multi_distinct_count, BIGINT, false] as DECIMAL128(38,0))\n" +
                "  |  7 <-> [12: multi_distinct_sum, DECIMAL128(38,0), true] / " +
                "cast([11: multi_distinct_count, BIGINT, false] as DECIMAL128(38,0))\n" +
                "  |  8 <-> [14: multi_distinct_sum, DECIMAL128(38,3), true] / " +
                "cast([13: multi_distinct_count, BIGINT, false] as DECIMAL128(38,0))";

        String plan = removeSlotIds(UtFrameUtils.getVerboseFragmentPlan(ctx, sql));
        expectPhase1Snippet = removeSlotIds(expectPhase1Snippet);
        expectPhase2Snippet = removeSlotIds(expectPhase2Snippet);
        projectOutputColumns = removeSlotIds(projectOutputColumns);
        Assert.assertTrue(plan, plan.contains(expectPhase1Snippet) &&
                plan.contains(expectPhase2Snippet) &&
                plan.contains(projectOutputColumns));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
        ctx.getSessionVariable().setCboCteReuse(oldCboCteReUse);
    }

    @Test
    public void testAvgDistinctWithRewriteMultiDistinctByCTERuleTakeEffect() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select avg(distinct col_decimal32p9s2), avg(distinct col_decimal64p13s0), " +
                "avg(distinct col_decimal128p20s3) from db1.decimal_table";
        String plan = removeSlotIds(UtFrameUtils.getVerboseFragmentPlan(ctx, sql));
        String[] expectSnippets = {
                "  |  6 <-> [9: sum, DECIMAL128(38,2), true] / " +
                        "cast([11: count, BIGINT, false] as DECIMAL128(38,0))\n" +
                        "  |  7 <-> [13: sum, DECIMAL128(38,0), true] / " +
                        "cast([15: count, BIGINT, false] as DECIMAL128(38,0))\n" +
                        "  |  8 <-> [17: sum, DECIMAL128(38,3), true] / " +
                        "cast([19: count, BIGINT, false] as DECIMAL128(38,0))",
                "multi_distinct_sum[([10: col_decimal32p9s2, DECIMAL32(9,2), false]); args: DECIMAL32; result: VARBINARY",
                "multi_distinct_sum[([14: col_decimal64p13s0, DECIMAL64(13,0), false]); args: DECIMAL64; result: VARBINARY",
                "multi_distinct_sum[([18: col_decimal128p20s3, DECIMAL128(20,3), true]); args: DECIMAL128; result: VARBINARY",
                "multi_distinct_sum[([9: sum, VARBINARY, true]); args: DECIMAL32; result: DECIMAL128(38,2)",
                "multi_distinct_sum[([13: sum, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,0)",
                "multi_distinct_sum[([17: sum, VARBINARY, true]); args: DECIMAL128; result: DECIMAL128(38,3)",
        };
        Assert.assertTrue(Arrays.asList(expectSnippets).stream().allMatch(s -> plan.contains(removeSlotIds(s))));
        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
    }

    @Test
    public void testAvgDistinctNonDecimalType() throws Exception {
        int oldStage = ctx.getSessionVariable().getNewPlannerAggStage();
        boolean oldCboCteReUse = ctx.getSessionVariable().isCboCteReuse();
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select avg(distinct key0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan, plan.contains("aggregate: avg[([1: key0, INT, false]); args: INT; result: VARBINARY; " +
                "args nullable: false; result nullable: true]"));
        Assert.assertTrue(plan, plan.contains("aggregate: avg[([6: avg, VARBINARY, true]); args: INT; result: DOUBLE; " +
                "args nullable: true; result nullable: true]"));

        ctx.getSessionVariable().setNewPlanerAggStage(oldStage);
        ctx.getSessionVariable().setCboCteReuse(oldCboCteReUse);
    }

    @Test
    public void testDecimalTypedWhenClausesOfCaseWhenWithoutCaseClause() throws Exception {
        String sql = "select case when col_decimal64p13s0 then col_decimal64p13s0 else 0 end from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan, plan.contains("  |  6 <-> if[(cast([3: col_decimal64p13s0, DECIMAL64(13,0), false]" +
                " as BOOLEAN), [3: col_decimal64p13s0, DECIMAL64(13,0), false], 0); " +
                "args: BOOLEAN,DECIMAL64,DECIMAL64; result: DECIMAL64(13,0); args nullable: true;" +
                " result nullable: true]\n"));
    }

    @Test
    public void testFirstArgOfIfIsDecimal() throws Exception {
        String sql = "select if(col_decimal64p13s0, col_decimal64p13s0, 0) from db1.decimal_table";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, sql);
        Assert.assertTrue(plan, plan.contains("if[(cast([3: col_decimal64p13s0, DECIMAL64(13,0), false] as BOOLEAN), " +
                "[3: col_decimal64p13s0, DECIMAL64(13,0), false], 0); args: BOOLEAN,DECIMAL64,DECIMAL64; " +
                "result: DECIMAL64(13,0); args nullable: true; result nullable: true]"));
    }
}

