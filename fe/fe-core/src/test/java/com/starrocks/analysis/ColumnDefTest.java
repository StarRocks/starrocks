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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ColumnDefTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.ColumnDefAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnDef.DefaultValueDef;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnDefTest {
    private TypeDef intCol;
    private TypeDef BigIntCol;
    private TypeDef stringCol;
    private TypeDef floatCol;
    private TypeDef booleanCol;
    private TypeDef arrayIntCol;

    @BeforeEach
    public void setUp() {
        intCol = new TypeDef(IntegerType.INT);
        BigIntCol = new TypeDef(IntegerType.BIGINT);
        stringCol = new TypeDef(TypeFactory.createCharType(10));
        floatCol = new TypeDef(FloatType.FLOAT);
        booleanCol = new TypeDef(BooleanType.BOOLEAN);
        arrayIntCol = new TypeDef(new ArrayType(IntegerType.INT));
    }

    @Test
    public void testNormal() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", intCol);
        ColumnDefAnalyzer.analyze(column, true);

        Assertions.assertEquals("col", column.getName());
        Assertions.assertEquals(PrimitiveType.INT, column.getType().getPrimitiveType());
        Assertions.assertNull(column.getAggregateType());
        Assertions.assertNull(column.getDefaultValue());
        Assertions.assertFalse(column.isAllowNull());
        Assertions.assertEquals("", column.getComment());

        // default
        column =
                new ColumnDef("col", intCol, true, null, null, false, new DefaultValueDef(true, new StringLiteral("10")), "");
        ColumnDefAnalyzer.analyze(column, true);
        Assertions.assertNull(column.getAggregateType());
        Assertions.assertEquals("10", column.getDefaultValue());
        Assertions.assertEquals("col", column.getName());
        Assertions.assertEquals(PrimitiveType.INT, column.getType().getPrimitiveType());
        Assertions.assertFalse(column.isAllowNull());
        Assertions.assertEquals("", column.getComment());

        // agg
        column = new ColumnDef("col", floatCol, false, AggregateType.SUM, null, false,
                new DefaultValueDef(true, new StringLiteral("10")), "");
        ColumnDefAnalyzer.analyze(column, true);
        Assertions.assertEquals("10", column.getDefaultValue());
        Assertions.assertEquals(AggregateType.SUM, column.getAggregateType());
        Assertions.assertEquals("col", column.getName());
        Assertions.assertEquals(PrimitiveType.FLOAT, column.getType().getPrimitiveType());
        Assertions.assertFalse(column.isAllowNull());
        Assertions.assertEquals("", column.getComment());
    }

    @Test
    public void testReplaceIfNotNull() throws AnalysisException {
        {
            // not allow null
            // although here is default value is NOT_SET but after analyze it will be set to NULL and allowed NULL trick.
            ColumnDef columnDef =
                    new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, null, false,
                            DefaultValueDef.NOT_SET,
                            "");
            ColumnDefAnalyzer.analyze(columnDef, true);
            Assertions.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, columnDef.getAggregateType());
            Assertions.assertTrue(columnDef.getDefaultValueDef().expr instanceof NullLiteral);

            Column column = ColumnBuilder.buildColumn(columnDef);
            Assertions.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregationType());
            Assertions.assertNull(column.getDefaultExpr());
            Assertions.assertNull(column.getDefaultValue());
        }
        {
            // not allow null
            ColumnDef column = new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, null, false,
                    new DefaultValueDef(true, new StringLiteral("10")), "");
            ColumnDefAnalyzer.analyze(column, true);
            Assertions.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregateType());
            Assertions.assertEquals("col", column.getName());
            Assertions.assertEquals(PrimitiveType.INT, column.getType().getPrimitiveType());
            Assertions.assertTrue(column.isAllowNull());
            Assertions.assertEquals("10", column.getDefaultValue());
            Assertions.assertEquals("", column.getComment());
        }
    }

    @Test
    public void testAutoIncrement() throws AnalysisException {
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", false, null, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    Boolean.TRUE, null, "");
            ColumnDefAnalyzer.analyze(column, true);

            Assertions.assertEquals("col", column.getName());
            Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assertions.assertNull(column.getAggregateType());
            Assertions.assertNull(column.getDefaultValue());
            Assertions.assertFalse(column.isAllowNull());
            Assertions.assertTrue(column.isAutoIncrement());
            Assertions.assertEquals("", column.getComment());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", false, null, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    null, null, "");
            ColumnDefAnalyzer.analyze(column, true);

            Assertions.assertEquals("col", column.getName());
            Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assertions.assertNull(column.getAggregateType());
            Assertions.assertNull(column.getDefaultValue());
            Assertions.assertFalse(column.isAllowNull());
            Assertions.assertFalse(column.isAutoIncrement());
            Assertions.assertEquals("", column.getComment());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", true, null, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    Boolean.TRUE, null, "");
            ColumnDefAnalyzer.analyze(column, true);

            Assertions.assertEquals("col", column.getName());
            Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assertions.assertNull(column.getAggregateType());
            Assertions.assertNull(column.getDefaultValue());
            Assertions.assertFalse(column.isAllowNull());
            Assertions.assertTrue(column.isAutoIncrement());
            Assertions.assertEquals("", column.getComment());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", true, null, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    null, null, "");
            ColumnDefAnalyzer.analyze(column, true);

            Assertions.assertEquals("col", column.getName());
            Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assertions.assertNull(column.getAggregateType());
            Assertions.assertNull(column.getDefaultValue());
            Assertions.assertFalse(column.isAllowNull());
            Assertions.assertFalse(column.isAutoIncrement());
            Assertions.assertEquals("", column.getComment());
        }
    }

    @Test
    public void testFloatKey() {
        assertThrows(AnalysisException.class, () -> {
            ColumnDef column = new ColumnDef("col", floatCol);
            column.setIsKey(true);
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testArrayKey() {
        assertThrows(AnalysisException.class, () -> {
            ColumnDef column = new ColumnDef("col", arrayIntCol);
            column.setIsKey(true);
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testArrayDefaultValue() {
        assertThrows(AnalysisException.class, () -> {
            ColumnDef column = new ColumnDef("col", arrayIntCol, false, null, null, true,
                    new DefaultValueDef(true, new StringLiteral("[1]")), "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testStrSum() {
        assertThrows(AnalysisException.class, () -> {
            ColumnDef column = new ColumnDef("col", stringCol, false, AggregateType.SUM, null, true, DefaultValueDef.NOT_SET, "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testBooleanDefaultValue() throws AnalysisException {
        ColumnDef column1 =
                new ColumnDef("col", booleanCol, true, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1")), "");
        ColumnDefAnalyzer.analyze(column1, true);
        Assertions.assertEquals("1", column1.getDefaultValue());

        ColumnDef column2 =
                new ColumnDef("col", booleanCol, true, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("true")), "");
        ColumnDefAnalyzer.analyze(column2, true);
        Assertions.assertEquals("true", column2.getDefaultValue());

        ColumnDef column3 =
                new ColumnDef("col", booleanCol, true, null, null,
                        true, new DefaultValueDef(true, new StringLiteral("10")), "");
        try {
            ColumnDefAnalyzer.analyze(column3, true);
        } catch (AnalysisException e) {
            Assertions.assertEquals("Invalid default value for 'col': Invalid BOOLEAN literal: 10", e.getMessage());
        }
    }

    @Test
    public void testFloatDefaultValue() throws AnalysisException {
        ColumnDef column1 =
                new ColumnDef("col", floatCol, false, null, null,
                        true, new DefaultValueDef(true, new StringLiteral("1")), "");
        ColumnDefAnalyzer.analyze(column1, true);
        Assertions.assertEquals("1", column1.getDefaultValue());

        ColumnDef column2 =
                new ColumnDef("col", floatCol, false, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1.1")), "");
        ColumnDefAnalyzer.analyze(column2, true);
        Assertions.assertEquals("1.1", column2.getDefaultValue());

        ColumnDef column3 =
                new ColumnDef("col", floatCol, false, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1" + ".100000000")), "");
        Assertions.assertEquals("1.100000000", column3.getDefaultValue());

        ColumnDef column4 =
                new ColumnDef("col", floatCol, false, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1.1234567")), "");
        Assertions.assertEquals("1.1234567", column4.getDefaultValue());

        ColumnDef column5 =
                new ColumnDef("col", floatCol, false, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1.12345678")), "");
        try {
            ColumnDefAnalyzer.analyze(column5, true);
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("Default value will loose precision: 1.12345678"));
        }

        ColumnDef column6 =
                new ColumnDef("col", floatCol, false, null, null,
                        true, new DefaultValueDef(true, new StringLiteral("123456789")), "");
        try {
            ColumnDefAnalyzer.analyze(column6, true);
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("Default value will loose precision: 123456789"));
        }
        ColumnDef column7 =
                new ColumnDef("col", floatCol, false, null, null, true,
                        new DefaultValueDef(true, new StringLiteral("1.99E38")), "");
        Assertions.assertEquals("1.99E38", column7.getDefaultValue());
    }

    @Test
    public void testArrayHLL() {
        assertThrows(SemanticException.class, () -> {
            ColumnDef column =
                    new ColumnDef("col", new TypeDef(new ArrayType(HLLType.HLL)), false, null, null,
                            true, DefaultValueDef.NOT_SET, "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testArrayBitmap() {
        assertThrows(SemanticException.class, () -> {
            ColumnDef column =
                    new ColumnDef("col", new TypeDef(new ArrayType(BitmapType.BITMAP)), false, null, null, true,
                            DefaultValueDef.NOT_SET,
                            "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testArrayPercentile() {
        assertThrows(SemanticException.class, () -> {
            ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(PercentileType.PERCENTILE)), false, null, null, true,
                    DefaultValueDef.NOT_SET, "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testInvalidVarcharInsideArray() {
        assertThrows(SemanticException.class, () -> {
            Type tooLongVarchar = TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength() + 1);
            ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(tooLongVarchar)), false, null, null, true,
                    DefaultValueDef.NOT_SET, "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testInvalidDecimalInsideArray() {
        assertThrows(SemanticException.class, () -> {
            Type invalidDecimal = TypeFactory.createDecimalV2Type(100, -1);
            ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(invalidDecimal)), false, null, null, true,
                    DefaultValueDef.NOT_SET, "");
            ColumnDefAnalyzer.analyze(column, true);
        });
    }

    @Test
    public void testColumnWithAggStateDesc() throws AnalysisException {
        AggregateFunction sum = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(IntegerType.INT), IntegerType.BIGINT, IntegerType.BIGINT, false, true, false);
        AggStateDesc aggStateDesc = new AggStateDesc(sum.functionName(), sum.getReturnType(), 
                Arrays.asList(sum.getArgs()), AggStateDesc.isAggFuncResultNullable(sum.functionName()));
        ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", false, AggregateType.AGG_STATE_UNION,
                aggStateDesc, Boolean.FALSE, DefaultValueDef.NOT_SET, Boolean.TRUE, null, "");
        ColumnDefAnalyzer.analyze(column, true);
        Assertions.assertEquals("col", column.getName());
        Assertions.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
        Assertions.assertEquals(AggregateType.AGG_STATE_UNION, column.getAggregateType());
        Assertions.assertNull(column.getDefaultValue());
        Assertions.assertFalse(column.isAllowNull());
        Assertions.assertTrue(column.isAutoIncrement());
        Assertions.assertEquals("", column.getComment());
    }
}
