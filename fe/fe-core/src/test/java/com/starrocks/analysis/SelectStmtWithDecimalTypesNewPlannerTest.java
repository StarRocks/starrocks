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
    private static String runningDir = "fe/mocked/DecimalDemoTestNewPlanner/" + UUID.randomUUID().toString() + "/";
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
                "col_double DOUBLE,\n" +
                "col_decimal128p20s3 DECIMAL128(20, 3)\n" +
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
    public void testNullif() throws Exception {
        String sql = "select  * from db1.decimal_table where 6 > nullif(col_decimal128p20s3, cast(null as DOUBLE))";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:nullif), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "has_var_args:false, signature:nullif(DOUBLE, DOUBLE), scalar_fn:TScalarFunction(symbol:), " +
                "id:0, fid:70307)";
        String thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testCoalesce() throws Exception {
        String sql = "select avg(coalesce(col_decimal128p20s3, col_double)) from db1.decimal_table";
        String expectString = "fn:TFunction(name:TFunctionName(function_name:coalesce), binary_type:BUILTIN, " +
                "arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))]), " +
                "has_var_args:true, signature:coalesce(DOUBLE...), scalar_fn:TScalarFunction(symbol:), " +
                "id:0, fid:70407)";
        String thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));

        thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testIf() throws Exception {
        String sql = " select  if(1, cast('3.14' AS decimal32(9, 2)), cast('1.9999' AS decimal32(5, 4))) " +
                "AS res0 from db1.decimal_table;";
        String thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))"));

        thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        Assert.assertTrue(thrift.contains(
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DOUBLE))"));
    }

    @Test
    public void testMoneyFormat() throws Exception {
        String sql = "select money_format(col_decimal128p20s3) from db1.decimal_table";
        String expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, signature:" +
                        "money_format(DECIMAL128(20,3)), scalar_fn:" +
                        "TScalarFunction(symbol:_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE)" +
                        ", id:0, fid:304022)";
        String thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        System.out.println(thrift);
        Assert.assertTrue(thrift.contains(expectString));

        expectString =
                "fn:TFunction(name:TFunctionName(function_name:money_format), binary_type:BUILTIN, arg_types:[TTypeDesc(types:" +
                        "[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL128, precision:20, scale:3))])], ret_type:TTypeDesc" +
                        "(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), has_var_args:false, " +
                        "signature:money_format(DECIMAL128(20,3)), scalar_fn:TScalarFunction(symbol:" +
                        "_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE), id:0, fid:304022)";
        thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        System.out.println(thrift);
        Assert.assertTrue(thrift.contains(expectString));
    }

    @Test
    public void testMultiply() throws Exception {
        String sql = "select col_decimal128p20s3 * 3.14 from db1.decimal_table";
        String expectString = "TExprNode(node_type:ARITHMETIC_EXPR, type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL128, precision:38, scale:5))]), opcode:MULTIPLY, num_children:2," +
                " output_scale:-1, output_column:-1, use_vectorized:true, has_nullable_child:true, is_nullable:true)," +
                " TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType" +
                "(type:DECIMAL128, precision:20, scale:3))]), num_children:0, slot_ref:TSlotRef(slot_id:3, tuple_id:0)," +
                " output_scale:-1, output_column:-1, use_vectorized:true, has_nullable_child:false, is_nullable:true)," +
                " TExprNode(node_type:DECIMAL_LITERAL, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:" +
                "TScalarType(type:DECIMAL128, precision:38, scale:2))]), num_children:0, decimal_literal:" +
                "TDecimalLiteral(value:3.14), output_scale:-1, use_vectorized:true, has_nullable_child:false," +
                " is_nullable:false)";

        String thrift = UtFrameUtils.getPlanThriftStringForNewPlanner(ctx, sql);
        System.out.println(thrift);
        Assert.assertTrue(thrift.contains(expectString));
    }
}

