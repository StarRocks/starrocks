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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ColumnTest.java

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

import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnDef.DefaultValueDef;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.CURRENT_TIMESTAMP_VALUE;
import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.NOT_SET;
import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnTest {

    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testSchemaChangeNotAllow() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.JSON), false, null, false,
                NULL_DEFAULT_VALUE, "");
        Column newColumn = new Column("user", ScalarType.createVarcharType(1), true, null, false,
                NULL_DEFAULT_VALUE, "");
        try {
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail();
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("JSON needs minimum length of "));
        }

        Column largeColumn = new Column("user", ScalarType.createVarcharType(1025), true, null, false,
                NULL_DEFAULT_VALUE, "");
        oldColumn.checkSchemaChangeAllowed(largeColumn);
    }

    @Test
    public void testSchemaChangeAllowNormal() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                NULL_DEFAULT_VALUE, "");
        Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.VARCHAR), true, null, false,
                NULL_DEFAULT_VALUE, "");
        oldColumn.checkSchemaChangeAllowed(newColumn);

        oldColumn = new Column("user", ScalarType.createType(PrimitiveType.VARCHAR), true, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        newColumn = new Column("user", ScalarType.createType(PrimitiveType.VARCHAR), true, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        oldColumn.checkSchemaChangeAllowed(newColumn);

        oldColumn = new Column("user", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                CURRENT_TIMESTAMP_VALUE, "");
        newColumn = new Column("user", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                CURRENT_TIMESTAMP_VALUE, "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testSchemaChangeAllowedDefaultValue() {
        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    NOT_SET, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            Column newColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    NOT_SET, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    NOT_SET, "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    NOT_SET, "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("1")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            Column newColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        } catch (DdlException ex) {
        }

    }

    @Test
    public void testSchemaChangeAllowedNullToNonNull() {
        assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, true,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testAutoIncrement() {
        Column col = new Column("col", ScalarType.createType(PrimitiveType.BIGINT), true, null, Boolean.FALSE,
                ColumnDef.DefaultValueDef.NOT_SET, "");
        col.setIsAutoIncrement(true);
        Assertions.assertTrue(col.isAutoIncrement() == true);
    }

    @Test
    public void testSchemaChangeAllowedInvolvingDecimalv3() throws DdlException {
        Column decimalColumn =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 15, 3), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");

        Column varcharColumn1 = new Column("user", ScalarType.createVarchar(50), false, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        varcharColumn1.checkSchemaChangeAllowed(decimalColumn);
        decimalColumn.checkSchemaChangeAllowed(varcharColumn1);

        Column varcharColumn2 = new Column("user", ScalarType.createVarchar(10), false, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        varcharColumn2.checkSchemaChangeAllowed(decimalColumn);
        try {
            decimalColumn.checkSchemaChangeAllowed(varcharColumn2);
            Assertions.fail("No exception throws");
        } catch (DdlException ex) {
        }

        Column decimalColumn2 =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 4), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        decimalColumn.checkSchemaChangeAllowed(decimalColumn2);

        try {
            decimalColumn2.checkSchemaChangeAllowed(decimalColumn);
            Assertions.fail("No exception throws");
        } catch (DdlException ex) {

        }

        Column decimalv2Column = new Column("user", ScalarType.createDecimalV2Type(), false, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        Column decimalColumn3 =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        decimalv2Column.checkSchemaChangeAllowed(decimalColumn3);

        try {
            decimalColumn3.checkSchemaChangeAllowed(decimalv2Column);
            Assertions.fail("No exception throws");
        } catch (DdlException ex) {

        }

        Column decimalColumn4 =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 13), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        try {
            decimalv2Column.checkSchemaChangeAllowed(decimalColumn4);
            Assertions.fail("No exception throws");
        } catch (DdlException ex) {

        }

        try {
            decimalColumn4.checkSchemaChangeAllowed(decimalColumn2);
            Assertions.fail("No exception throws");
        } catch (DdlException ex) {

        }
    }

    @Test
    public void testLscColumn() {
        Column f0 = new Column("f0", Type.INT, true, AggregateType.NONE, null, false,
                new DefaultValueDef(true, NullLiteral.create(Type.INT)), "", 0);

        Index i0 = new Index("i0",
                Collections.singletonList(ColumnId.create("f0")), IndexType.BITMAP, "");

        Set<ColumnId> bfColumns = new HashSet<>();
        bfColumns.add(ColumnId.create("f0"));
        TColumn t0 = f0.toThrift();
        f0.setIndexFlag(t0, Collections.singletonList(i0), bfColumns);

        Assertions.assertEquals(t0.has_bitmap_index, true);
        Assertions.assertEquals(t0.is_bloom_filter_column, true);

        Assertions.assertEquals(f0.getUniqueId(), 0);
        f0.setUniqueId(1);

        Assertions.assertEquals(f0.getUniqueId(), 1);

    }

    @Test
    public void testColumnDeserialization() {
        String str = "{\"name\": \"test\"}";
        Column column = GsonUtils.GSON.fromJson(str, Column.class);
        Assertions.assertEquals("test", column.getColumnId().getId());
    }

    @Test
    public void testToSqlWithoutAggregateTypeName() {
        String comment = "{\"id\":\"0\",\"value\":\"1\"}";
        Column column = new Column("col", ScalarType.createType(PrimitiveType.JSON), false, null, true, null, comment);
        String toSql = column.toSqlWithoutAggregateTypeName(null);

        Assertions.assertEquals("`col` json NULL COMMENT \"{\\\"id\\\":\\\"0\\\",\\\"value\\\":\\\"1\\\"}\"", toSql);
    }
}
