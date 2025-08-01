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

import com.starrocks.analysis.TypeDef;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

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
        TypeDef type = TypeDef.create(PrimitiveType.INT);

        type.analyze();

        Assertions.assertEquals(PrimitiveType.INT, type.getType().getPrimitiveType());
        Assertions.assertEquals("int(11)", type.toSql());

        // equal type
        TypeDef type2 = TypeDef.create(PrimitiveType.INT);
        Assertions.assertEquals(type.getType(), type2.getType());

        // not equal type
        TypeDef type3 = TypeDef.create(PrimitiveType.BIGINT);
        Assertions.assertNotSame(type.getType(), type3.getType());
    }

    @Test
    public void testInvalidType() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = TypeDef.create(PrimitiveType.INVALID_TYPE);
            type.analyze();
        });
    }

    @Test
    public void testCharType() throws AnalysisException {
        TypeDef type = TypeDef.createVarchar(10);
        type.analyze();
        Assertions.assertEquals("VARCHAR(10)", type.toString());
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getType().getPrimitiveType());
        Assertions.assertEquals(10, ((ScalarType) type.getType()).getLength());

        // equal type
        TypeDef type2 = TypeDef.createVarchar(10);
        Assertions.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createVarchar(3);
        Assertions.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testCharInvalid() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = TypeDef.createVarchar(-1);
            type.analyze();
            Assertions.fail("No Exception throws");
        });
    }

    @Test
    public void testDecimal() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(12, 5);
        type.analyze();
        Assertions.assertEquals("DECIMAL(12,5)", type.toString());
        Assertions.assertEquals(PrimitiveType.DECIMALV2, type.getType().getPrimitiveType());
        Assertions.assertEquals(12, ((ScalarType) type.getType()).getScalarPrecision());
        Assertions.assertEquals(5, ((ScalarType) type.getType()).getScalarScale());

        // equal type
        TypeDef type2 = TypeDef.createDecimal(12, 5);
        Assertions.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createDecimal(11, 5);
        Assertions.assertNotEquals(type.getType(), type3.getType());
        type3 = TypeDef.createDecimal(12, 4);
        Assertions.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testDecimalPreFail() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = TypeDef.createDecimal(28, 3);
            type.analyze();
        });
    }

    @Test
    public void testDecimalScaleFail() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = TypeDef.createDecimal(27, 10);
            type.analyze();
        });
    }

    @Test
    public void testDecimalScaleLargeFial() {
        assertThrows(SemanticException.class, () -> {
            TypeDef type = TypeDef.createDecimal(8, 9);
            type.analyze();
        });
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./columnType");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        ScalarType type1 = Type.NULL;
        ColumnType.write(dos, type1);

        ScalarType type2 = ScalarType.createType(PrimitiveType.BIGINT);
        ColumnType.write(dos, type2);

        ScalarType type3 = ScalarType.createDecimalV2Type(1, 1);
        ColumnType.write(dos, type3);

        ScalarType type4 = ScalarType.createDecimalV2Type(1, 1);
        ColumnType.write(dos, type4);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Type rType1 = ColumnType.read(dis);
        Assertions.assertTrue(rType1.equals(type1));

        Type rType2 = ColumnType.read(dis);
        Assertions.assertTrue(rType2.equals(type2));

        Type rType3 = ColumnType.read(dis);

        // Change it when remove DecimalV2
        Assertions.assertTrue(rType3.equals(type3) || rType3.equals(type4));

        Assertions.assertFalse(type1.equals(this));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
