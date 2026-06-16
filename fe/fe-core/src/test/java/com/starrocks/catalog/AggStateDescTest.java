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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.sql.analyzer.ColumnDefAnalyzer;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class AggStateDescTest {
    @Test
    public void testNewAggStateDesc() {
        AggregateFunction sum = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(IntegerType.INT), IntegerType.BIGINT, IntegerType.BIGINT, false, true, false);
        AggStateDesc aggStateDesc = new AggStateDesc(sum.functionName(), sum.getReturnType(), 
                Arrays.asList(sum.getArgs()), AggStateDesc.isAggFuncResultNullable(sum.functionName()));
        Assertions.assertEquals(1, aggStateDesc.getArgTypes().size());
        Assertions.assertEquals(IntegerType.INT, aggStateDesc.getArgTypes().get(0));
        Assertions.assertEquals(IntegerType.BIGINT, aggStateDesc.getReturnType());
        Assertions.assertEquals(FunctionSet.SUM, aggStateDesc.getFunctionName());
        Assertions.assertEquals(true, aggStateDesc.getResultNullable());
        Assertions.assertEquals("sum(int(11))", aggStateDesc.toSql());
        Assertions.assertEquals("sum(int(11))", aggStateDesc.toString());
        Assertions.assertNotNull(TypeSerializer.toThrift(aggStateDesc));
        try {
            Assertions.assertNotNull(ColumnDefAnalyzer.getAggregateFunction(aggStateDesc));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCompareAggStateDesc() {
        AggregateFunction sum1 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(IntegerType.INT), IntegerType.BIGINT, IntegerType.BIGINT, false, true, false);
        AggStateDesc aggStateDesc1 = new AggStateDesc(sum1.functionName(), sum1.getReturnType(), 
                Arrays.asList(sum1.getArgs()), AggStateDesc.isAggFuncResultNullable(sum1.functionName()));
        AggregateFunction sum2 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(IntegerType.INT), IntegerType.BIGINT, IntegerType.BIGINT, false, true, false);
        AggStateDesc aggStateDesc2 = new AggStateDesc(sum2.functionName(), sum2.getReturnType(), 
                Arrays.asList(sum2.getArgs()), AggStateDesc.isAggFuncResultNullable(sum2.functionName()));
        Assertions.assertEquals(aggStateDesc1, aggStateDesc2);
        AggregateFunction sum3 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(FloatType.FLOAT), FloatType.DOUBLE, FloatType.DOUBLE, false, true, false);
        AggStateDesc aggStateDesc3 = new AggStateDesc(sum3.functionName(), sum3.getReturnType(), 
                Arrays.asList(sum3.getArgs()), AggStateDesc.isAggFuncResultNullable(sum3.functionName()));
        Assertions.assertNotEquals(aggStateDesc3, aggStateDesc2);

        AggStateDesc cloned3 = aggStateDesc3.clone();
        Assertions.assertEquals(aggStateDesc3, cloned3);
    }

    @Test
    public void testToSqlEmitsCreateAggregateFunctionHeader() {
        AggregateFunction fn = new AggregateFunction(
                new FunctionName("test_db", "my_sum"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        String sql = fn.toSql(false);
        Assertions.assertTrue(sql.startsWith("CREATE AGGREGATE FUNCTION test_db.my_sum"), sql);
        Assertions.assertTrue(sql.contains("RETURNS"), sql);
        Assertions.assertFalse(sql.contains("IF NOT EXISTS"), sql);
    }

    @Test
    public void testToSqlEmitsIfNotExistsWhenRequested() {
        AggregateFunction fn = new AggregateFunction(
                new FunctionName("test_db", "my_sum"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        Assertions.assertTrue(fn.toSql(true).contains("CREATE AGGREGATE FUNCTION IF NOT EXISTS"));
    }

    @Test
    public void testToSqlEmitsTypeFileSymbolProperties() {
        AggregateFunction fn = new AggregateFunction(
                new FunctionName("test_db", "my_sum"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        fn.setLocation(new HdfsURI("file:///tmp/udf.jar"));
        String sql = fn.toSql(false);
        Assertions.assertTrue(sql.contains("\"type\" = \"StarrocksJar\""), sql);
        Assertions.assertTrue(sql.contains("\"file\" = \"file:///tmp/udf.jar\""), sql);
    }

    @Test
    public void testToSqlEmitsAnalyticPropertyForUDWF() {
        // isAnalyticFn=true marks this as a window function — surfaced as "analytic" = "true".
        AggregateFunction fn = new AggregateFunction(
                new FunctionName("test_db", "my_window"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false, true);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        Assertions.assertTrue(fn.toSql(false).contains("\"analytic\" = \"true\""));
    }

    @Test
    public void testToSqlIsolationOnlyEmittedWhenShared() {
        // Default isolation is isolated; key omitted unless explicitly set to shared.
        AggregateFunction isolated = new AggregateFunction(
                new FunctionName("test_db", "my_sum"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false);
        isolated.setBinaryType(TFunctionBinaryType.SRJAR);
        Assertions.assertFalse(isolated.toSql(false).contains("\"isolation\""),
                "isolation key should be omitted when default (isolated)");

        AggregateFunction shared = new AggregateFunction(
                new FunctionName("test_db", "my_sum"),
                Lists.<Type>newArrayList(IntegerType.INT),
                IntegerType.BIGINT, null, false);
        shared.setBinaryType(TFunctionBinaryType.SRJAR);
        shared.setIsolationType(false);
        Assertions.assertTrue(shared.toSql(false).contains("\"isolation\" = \"shared\""),
                "isolation key should be emitted as 'shared' when isolationType is false");
    }
}
