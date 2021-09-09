// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ExprTest.java

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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class ExprTest {

    @Test
    public void testGetTableNameToColumnNames(@Mocked Analyzer analyzer,
                                              @Injectable SlotDescriptor slotDesc1,
                                              @Injectable SlotDescriptor slotDesc2,
                                              @Injectable TupleDescriptor tupleDescriptor1,
                                              @Injectable TupleDescriptor tupleDescriptor2,
                                              @Injectable Table tableA,
                                              @Injectable Table tableB) throws AnalysisException {
        TableName tableAName = new TableName("test", "tableA");
        TableName tableBName = new TableName("test", "tableB");
        SlotRef tableAColumn1 = new SlotRef(tableAName, "c1");
        SlotRef tableBColumn1 = new SlotRef(tableBName, "c1");
        Expr whereExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, tableAColumn1, tableBColumn1);
        Deencapsulation.setField(tableAColumn1, "desc", slotDesc1);
        Deencapsulation.setField(tableBColumn1, "desc", slotDesc2);
        new Expectations() {
            {
                slotDesc1.isMaterialized();
                result = true;
                slotDesc2.isMaterialized();
                result = true;
                slotDesc1.getColumn().getName();
                result = "c1";
                slotDesc2.getColumn().getName();
                result = "c1";
                slotDesc1.getParent();
                result = tupleDescriptor1;
                slotDesc2.getParent();
                result = tupleDescriptor2;
                tupleDescriptor1.getTable();
                result = tableA;
                tupleDescriptor2.getTable();
                result = tableB;
                tableA.getId();
                result = 1;
                tableB.getId();
                result = 2;

            }
        };

        Map<Long, Set<String>> tableNameToColumnNames = Maps.newHashMap();
        whereExpr.getTableIdToColumnNames(tableNameToColumnNames);
        Assert.assertEquals(tableNameToColumnNames.size(), 2);
        Set<String> tableAColumns = tableNameToColumnNames.get(new Long(1));
        Assert.assertNotEquals(tableAColumns, null);
        Assert.assertTrue(tableAColumns.contains("c1"));
        Set<String> tableBColumns = tableNameToColumnNames.get(new Long(2));
        Assert.assertNotEquals(tableBColumns, null);
        Assert.assertTrue(tableBColumns.contains("c1"));
    }

    @Test
    public void testUncheckedCastTo() throws AnalysisException {
        // uncheckedCastTo should return new object

        // date
        DateLiteral dateLiteral = new DateLiteral(2020, 4, 5, 12, 0, 5);
        Assert.assertTrue(dateLiteral.getType().isDatetime());
        DateLiteral castLiteral = (DateLiteral) dateLiteral.uncheckedCastTo(Type.DATE);
        Assert.assertFalse(dateLiteral == castLiteral);
        Assert.assertTrue(dateLiteral.getType().isDatetime());
        Assert.assertTrue(castLiteral.getType().isDate());

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        Assert.assertEquals(12, dateLiteral.getHour());
        Assert.assertEquals(0, dateLiteral.getMinute());
        Assert.assertEquals(5, dateLiteral.getSecond());

        DateLiteral dateLiteral2 = new DateLiteral(2020, 4, 5);
        Assert.assertTrue(dateLiteral2.getType().isDate());
        castLiteral = (DateLiteral) dateLiteral2.uncheckedCastTo(Type.DATETIME);
        Assert.assertFalse(dateLiteral2 == castLiteral);
        Assert.assertTrue(dateLiteral2.getType().isDate());
        Assert.assertTrue(castLiteral.getType().isDatetime());

        Assert.assertEquals(0, castLiteral.getHour());
        Assert.assertEquals(0, castLiteral.getMinute());
        Assert.assertEquals(0, castLiteral.getSecond());

        // float
        FloatLiteral floatLiteral = new FloatLiteral(0.1, Type.FLOAT);
        Assert.assertTrue(floatLiteral.getType().isFloat());
        FloatLiteral castFloatLiteral = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.DOUBLE);
        Assert.assertTrue(floatLiteral.getType().isFloat());
        Assert.assertTrue(castFloatLiteral.getType().isDouble());
        Assert.assertFalse(floatLiteral == castFloatLiteral);
        FloatLiteral castFloatLiteral2 = (FloatLiteral) floatLiteral.uncheckedCastTo(Type.FLOAT);
        Assert.assertTrue(floatLiteral == castFloatLiteral2);

        // int
        IntLiteral intLiteral = new IntLiteral(200);
        Assert.assertTrue(intLiteral.getType().isSmallint());
        IntLiteral castIntLiteral = (IntLiteral) intLiteral.uncheckedCastTo(Type.INT);
        Assert.assertTrue(intLiteral.getType().isSmallint());
        Assert.assertTrue(castIntLiteral.getType().isInt());
        Assert.assertFalse(intLiteral == castIntLiteral);
        IntLiteral castIntLiteral2 = (IntLiteral) intLiteral.uncheckedCastTo(Type.SMALLINT);
        Assert.assertTrue(intLiteral == castIntLiteral2);

        // null
        NullLiteral nullLiternal = NullLiteral.create(Type.DATE);
        Assert.assertTrue(nullLiternal.getType().isDate());
        NullLiteral castNullLiteral = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATETIME);
        Assert.assertTrue(nullLiternal.getType().isDate());
        Assert.assertTrue(castNullLiteral.getType().isDatetime());
        Assert.assertFalse(nullLiternal == castNullLiteral);
        NullLiteral castNullLiteral2 = (NullLiteral) nullLiternal.uncheckedCastTo(Type.DATE);
        Assert.assertTrue(nullLiternal == castNullLiteral2);

        // string
        StringLiteral stringLiteral = new StringLiteral("abc");
        Assert.assertTrue(stringLiteral.getType().isVarchar());
        StringLiteral castStringLiteral = (StringLiteral) stringLiteral.uncheckedCastTo(Type.CHAR);
        Assert.assertTrue(stringLiteral.getType().isVarchar());
        Assert.assertTrue(castStringLiteral.getType().isChar());
        Assert.assertFalse(stringLiteral == castStringLiteral);
        StringLiteral castStringLiteral2 = (StringLiteral) stringLiteral.uncheckedCastTo(Type.VARCHAR);
        Assert.assertTrue(stringLiteral == castStringLiteral2);
    }
}
