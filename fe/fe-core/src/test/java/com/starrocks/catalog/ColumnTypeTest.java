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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ColumnTypeTest.java

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

package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.TypeDefAnalyzer;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnTypeTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testPrimitiveType() throws AnalysisException {
        TypeDef type = new TypeDef(TypeFactory.createType(PrimitiveType.INT));

        TypeDefAnalyzer.analyze(type);

        Assertions.assertEquals(PrimitiveType.INT, type.getType().getPrimitiveType());
        Assertions.assertEquals("int(11)", type.toSql());

        // equal type
        TypeDef type2 = new TypeDef(TypeFactory.createType(PrimitiveType.INT));
        Assertions.assertEquals(type.getType(), type2.getType());

        // not equal type
        TypeDef type3 = new TypeDef(TypeFactory.createType(PrimitiveType.BIGINT));
        Assertions.assertNotSame(type.getType(), type3.getType());
    }

    @Test
    public void testInvalidType() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = new TypeDef(TypeFactory.createType(PrimitiveType.INVALID_TYPE));
            TypeDefAnalyzer.analyze(type);
        });
    }

    @Test
    public void testCharType() throws AnalysisException {
        TypeDef type = new TypeDef(TypeFactory.createVarcharType(10));
        TypeDefAnalyzer.analyze(type);
        Assertions.assertEquals("VARCHAR(10)", type.toString());
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getType().getPrimitiveType());
        Assertions.assertEquals(10, ((ScalarType) type.getType()).getLength());

        // equal type
        TypeDef type2 = new TypeDef(TypeFactory.createVarcharType(10));
        Assertions.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = new TypeDef(TypeFactory.createVarcharType(3));
        Assertions.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = new TypeDef(TypeFactory.createType(PrimitiveType.BIGINT));
        Assertions.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testCharInvalid() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = new TypeDef(TypeFactory.createVarcharType(-1));
            TypeDefAnalyzer.analyze(type);
            Assertions.fail("No Exception throws");
        });
    }

    @Test
    public void testDecimal() throws AnalysisException {
        TypeDef type = new TypeDef(TypeFactory.createDecimalV2Type(12, 5));
        TypeDefAnalyzer.analyze(type);
        Assertions.assertEquals("DECIMAL(12,5)", type.toString());
        Assertions.assertEquals(PrimitiveType.DECIMALV2, type.getType().getPrimitiveType());
        Assertions.assertEquals(12, ((ScalarType) type.getType()).getScalarPrecision());
        Assertions.assertEquals(5, ((ScalarType) type.getType()).getScalarScale());

        // equal type
        TypeDef type2 = new TypeDef(TypeFactory.createDecimalV2Type(12, 5));
        Assertions.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = new TypeDef(TypeFactory.createDecimalV2Type(11, 5));
        Assertions.assertNotEquals(type.getType(), type3.getType());
        type3 = new TypeDef(TypeFactory.createDecimalV2Type(12, 4));
        Assertions.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = new TypeDef(TypeFactory.createType(PrimitiveType.BIGINT));
        Assertions.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testDecimalPreFail() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = new TypeDef(TypeFactory.createDecimalV2Type(28, 3));
            TypeDefAnalyzer.analyze(type);
        });
    }

    @Test
    public void testDecimalScaleFail() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = new TypeDef(TypeFactory.createDecimalV2Type(27, 10));
            TypeDefAnalyzer.analyze(type);
        });
    }

    @Test
    public void testDecimalScaleLargeFial() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = new TypeDef(TypeFactory.createDecimalV2Type(8, 9));
            TypeDefAnalyzer.analyze(type);
        });
    }
}
