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

import com.starrocks.analysis.ColumnDef.DefaultValueDef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnDefTest {
    private TypeDef intCol;
    private TypeDef BigIntCol;
    private TypeDef stringCol;
    private TypeDef floatCol;
    private TypeDef booleanCol;
    private TypeDef arrayIntCol;

    @Before
    public void setUp() {
        intCol = new TypeDef(ScalarType.createType(PrimitiveType.INT));
        BigIntCol = new TypeDef(ScalarType.createType(PrimitiveType.BIGINT));
        stringCol = new TypeDef(ScalarType.createCharType(10));
        floatCol = new TypeDef(ScalarType.createType(PrimitiveType.FLOAT));
        booleanCol = new TypeDef(ScalarType.createType(PrimitiveType.BOOLEAN));
        arrayIntCol = new TypeDef(new ArrayType(Type.INT));
    }

    @Test
    public void testNormal() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", intCol);
        column.analyze(true);

        Assert.assertEquals("`col` int(11) NOT NULL COMMENT \"\"", column.toString());
        Assert.assertEquals("col", column.getName());
        Assert.assertEquals(PrimitiveType.INT, column.getType().getPrimitiveType());
        Assert.assertNull(column.getAggregateType());
        Assert.assertNull(column.getDefaultValue());

        // default
        column =
                new ColumnDef("col", intCol, true, null, false, new DefaultValueDef(true, new StringLiteral("10")), "");
        column.analyze(true);
        Assert.assertNull(column.getAggregateType());
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals("`col` int(11) NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());

        // agg
        column = new ColumnDef("col", floatCol, false, AggregateType.SUM, false,
                new DefaultValueDef(true, new StringLiteral("10")), "");
        column.analyze(true);
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals(AggregateType.SUM, column.getAggregateType());
        Assert.assertEquals("`col` float SUM NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());
    }

    @Test
    public void testReplaceIfNotNull() throws AnalysisException {
        {
            // not allow null
            // although here is default value is NOT_SET but after analyze it will be set to NULL and allowed NULL trick.
            ColumnDef column =
                    new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, false,
                            DefaultValueDef.NOT_SET,
                            "");
            column.analyze(true);
            Assert.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregateType());
            Assert.assertEquals("`col` int(11) REPLACE_IF_NOT_NULL NULL DEFAULT NULL COMMENT \"\"", column.toSql());
        }
        {
            // not allow null
            ColumnDef column = new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, false,
                    new DefaultValueDef(true, new StringLiteral("10")), "");
            column.analyze(true);
            Assert.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregateType());
            Assert.assertEquals("`col` int(11) REPLACE_IF_NOT_NULL NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());
        }
    }

    @Test
    public void testAutoIncrement() throws AnalysisException {
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", false, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    Boolean.TRUE, "");
            column.analyze(true);

            Assert.assertEquals("`col` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\"", column.toString());
            Assert.assertEquals("col", column.getName());
            Assert.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assert.assertNull(column.getAggregateType());
            Assert.assertNull(column.getDefaultValue());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", false, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    null, "");
            column.analyze(true);

            Assert.assertEquals("`col` bigint(20) NOT NULL COMMENT \"\"", column.toString());
            Assert.assertEquals("col", column.getName());
            Assert.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assert.assertNull(column.getAggregateType());
            Assert.assertNull(column.getDefaultValue());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", true, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    Boolean.TRUE, "");
            column.analyze(true);

            Assert.assertEquals("`col` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\"", column.toString());
            Assert.assertEquals("col", column.getName());
            Assert.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assert.assertNull(column.getAggregateType());
            Assert.assertNull(column.getDefaultValue());
        }
        {
            ColumnDef column = new ColumnDef("col", BigIntCol, "utf8", true, null, Boolean.FALSE, DefaultValueDef.NOT_SET,
                    null, "");
            column.analyze(true);

            Assert.assertEquals("`col` bigint(20) NOT NULL COMMENT \"\"", column.toString());
            Assert.assertEquals("col", column.getName());
            Assert.assertEquals(PrimitiveType.BIGINT, column.getType().getPrimitiveType());
            Assert.assertNull(column.getAggregateType());
            Assert.assertNull(column.getDefaultValue());
        }
    }

    @Test(expected = AnalysisException.class)
    public void testFloatKey() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", floatCol);
        column.setIsKey(true);
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testArrayKey() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", arrayIntCol);
        column.setIsKey(true);
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testArrayDefaultValue() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", arrayIntCol, false, null, true,
                new DefaultValueDef(true, new StringLiteral("[1]")), "");
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testStrSum() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", stringCol, false, AggregateType.SUM, true, DefaultValueDef.NOT_SET, "");
        column.analyze(true);
    }

    @Test
    public void testBooleanDefaultValue() throws AnalysisException {
        ColumnDef column1 =
                new ColumnDef("col", booleanCol, true, null, true, new DefaultValueDef(true, new StringLiteral("1")),
                        "");
        column1.analyze(true);
        Assert.assertEquals("1", column1.getDefaultValue());

        ColumnDef column2 =
                new ColumnDef("col", booleanCol, true, null, true, new DefaultValueDef(true, new StringLiteral("true")),
                        "");
        column2.analyze(true);
        Assert.assertEquals("true", column2.getDefaultValue());

        ColumnDef column3 =
                new ColumnDef("col", booleanCol, true, null, true, new DefaultValueDef(true, new StringLiteral("10")),
                        "");
        try {
            column3.analyze(true);
        } catch (AnalysisException e) {
            Assert.assertEquals("Invalid default value for 'col': Invalid BOOLEAN literal: 10", e.getMessage());
        }
    }

    @Test(expected = AnalysisException.class)
    public void testArrayHLL() throws AnalysisException {
        ColumnDef column =
                new ColumnDef("col", new TypeDef(new ArrayType(Type.HLL)), false, null, true, DefaultValueDef.NOT_SET,
                        "");
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testArrayBitmap() throws AnalysisException {
        ColumnDef column =
                new ColumnDef("col", new TypeDef(new ArrayType(Type.BITMAP)), false, null, true,
                        DefaultValueDef.NOT_SET,
                        "");
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testArrayPercentile() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(Type.PERCENTILE)), false, null, true,
                DefaultValueDef.NOT_SET, "");
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidVarcharInsideArray() throws AnalysisException {
        Type tooLongVarchar = ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH + 1);
        ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(tooLongVarchar)), false, null, true,
                DefaultValueDef.NOT_SET, "");
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidDecimalInsideArray() throws AnalysisException {
        Type invalidDecimal = ScalarType.createDecimalV2Type(100, -1);
        ColumnDef column = new ColumnDef("col", new TypeDef(new ArrayType(invalidDecimal)), false, null, true,
                DefaultValueDef.NOT_SET, "");
        column.analyze(true);
    }
}
