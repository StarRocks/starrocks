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

import com.starrocks.analysis.IndexDef.IndexType;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnDef.DefaultValueDef;
import com.starrocks.thrift.TColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collections;

import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.CURRENT_TIMESTAMP_VALUE;
import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.NOT_SET;
import static com.starrocks.sql.ast.ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE;

public class ColumnTest {

    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @Before
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./columnTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Column column1 = new Column("user",
                ScalarType.createCharType(20), false, AggregateType.SUM, "", "");
        column1.write(dos);
        Column column2 = new Column("age",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20", "");
        column2.write(dos);

        Column column3 = new Column("name", Type.BIGINT);
        column3.setIsKey(true);
        column3.write(dos);

        Column column4 = new Column("age",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20",
                "");
        column4.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Column rColumn1 = Column.read(dis);
        Assert.assertEquals("user", rColumn1.getName());
        Assert.assertEquals(PrimitiveType.CHAR, rColumn1.getPrimitiveType());
        Assert.assertEquals(AggregateType.SUM, rColumn1.getAggregationType());
        Assert.assertEquals("", rColumn1.getDefaultValue());
        Assert.assertEquals(0, rColumn1.getScale());
        Assert.assertEquals(0, rColumn1.getPrecision());
        Assert.assertEquals(20, rColumn1.getStrLen());
        Assert.assertFalse(rColumn1.isAllowNull());

        // 3. Test read()
        Column rColumn2 = Column.read(dis);
        Assert.assertEquals("age", rColumn2.getName());
        Assert.assertEquals(PrimitiveType.INT, rColumn2.getPrimitiveType());
        Assert.assertEquals(AggregateType.REPLACE, rColumn2.getAggregationType());
        Assert.assertEquals("20", rColumn2.getDefaultValue());

        Column rColumn3 = Column.read(dis);
        Assert.assertTrue(rColumn3.equals(column3));

        Column rColumn4 = Column.read(dis);
        Assert.assertTrue(rColumn4.equals(column4));

        Assert.assertEquals(rColumn2.toString(), column2.toString());
        Assert.assertTrue(column1.equals(column1));
        Assert.assertFalse(column1.equals(this));

        // 4. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testSchemaChangeNotAllow() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.JSON), false, null, false,
                NULL_DEFAULT_VALUE, "");
        Column newColumn = new Column("user", ScalarType.createVarcharType(1), true, null, false,
                NULL_DEFAULT_VALUE, "");
        try {
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("JSON needs minimum length of "));
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
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            Column newColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    NOT_SET, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    NOT_SET, "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    NOT_SET, "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("1")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

        try {
            Column oldColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    CURRENT_TIMESTAMP_VALUE, "");
            Column newColumn = new Column("dt", ScalarType.createType(PrimitiveType.DATETIME), true, null, false,
                    new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException ex) {
        }

    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeAllowedNullToNonNull() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testAutoIncrement() {
        Column col = new Column("col", ScalarType.createType(PrimitiveType.BIGINT), true, null, Boolean.FALSE,
                ColumnDef.DefaultValueDef.NOT_SET, "");
        col.setIsAutoIncrement(true);
        Assert.assertTrue(col.isAutoIncrement() == true);
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
            Assert.fail("No exception throws");
        } catch (DdlException ex) {
        }

        Column decimalColumn2 =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 4), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        decimalColumn.checkSchemaChangeAllowed(decimalColumn2);

        try {
            decimalColumn2.checkSchemaChangeAllowed(decimalColumn);
            Assert.fail("No exception throws");
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
            Assert.fail("No exception throws");
        } catch (DdlException ex) {

        }

        Column decimalColumn4 =
                new Column("user", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 13), false, null, true,
                        new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "");
        try {
            decimalv2Column.checkSchemaChangeAllowed(decimalColumn4);
            Assert.fail("No exception throws");
        } catch (DdlException ex) {

        }

        try {
            decimalColumn4.checkSchemaChangeAllowed(decimalColumn2);
            Assert.fail("No exception throws");
        } catch (DdlException ex) {

        }
    }

    @Test
    public void testLscColumn() {
        Column f0 = new Column("f0", Type.INT, true, AggregateType.NONE, false,
                new DefaultValueDef(true, NullLiteral.create(Type.INT)), "", 0);

        Index i0 = new Index("i0",
                Collections.singletonList("f0"), IndexType.BITMAP, "");

        TColumn t0 = f0.toThrift();
        f0.setIndexFlag(t0, Collections.singletonList(i0));

        Assert.assertEquals(t0.has_bitmap_index, true);

        Assert.assertEquals(f0.getUniqueId(), 0);
        f0.setUniqueId(1);

        Assert.assertEquals(f0.getUniqueId(), 1);

    }
}
