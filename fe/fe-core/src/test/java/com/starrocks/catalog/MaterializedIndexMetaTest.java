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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MaterializedIndexMetaTest.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.mv.MVUtils.MATERIALIZED_VIEW_NAME_PREFIX;

public class MaterializedIndexMetaTest {

    private static String fileName = "./MaterializedIndexMetaSerializeTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSetDefineExprCaseInsensitive() {
        List<Column> schema = Lists.newArrayList();
        Column column = new Column("UPPER", Type.ARRAY_VARCHAR);
        schema.add(column);
        MaterializedIndexMeta meta = new MaterializedIndexMeta(0, schema, 0, 0,
                (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS, null);

        Map<String, Expr> columnNameToDefineExpr = Maps.newHashMap();
        columnNameToDefineExpr.put("upper", new StringLiteral());
        meta.setColumnsDefineExpr(columnNameToDefineExpr);
        Assert.assertNotNull(column.getDefineExpr());
    }

    @Test
    public void testSerializeMaterializedIndexMeta(@Mocked CreateMaterializedViewStmt stmt)
            throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        String mvColumnName =
                MATERIALIZED_VIEW_NAME_PREFIX + FunctionSet.BITMAP_UNION + "_" + "k1";
        List<Column> schema = Lists.newArrayList();
        ColumnDef.DefaultValueDef defaultValue1 = new ColumnDef.DefaultValueDef(true, new StringLiteral("1"));
        schema.add(new Column("K1", Type.TINYINT, true, null, true, defaultValue1, "abc"));
        schema.add(new Column("k2", Type.SMALLINT, true, null, true, defaultValue1, "debug"));
        schema.add(new Column("k3", Type.INT, true, null, true, defaultValue1, ""));
        schema.add(new Column("k4", Type.BIGINT, true, null, true, defaultValue1, "**"));
        schema.add(new Column("k5", Type.LARGEINT, true, null, true, null, ""));
        schema.add(new Column("k6", Type.DOUBLE, true, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("1.1")), ""));
        schema.add(new Column("k7", Type.FLOAT, true, null, true, defaultValue1, ""));
        schema.add(new Column("k8", Type.DATE, true, null, true, defaultValue1, ""));
        schema.add(new Column("k9", Type.DATETIME, true, null, true, defaultValue1, ""));
        schema.add(new Column("k10", Type.VARCHAR, true, null, true, defaultValue1, ""));
        schema.add(new Column("k11", Type.DECIMALV2, true, null, true, defaultValue1, ""));
        schema.add(new Column("k12", Type.INT, true, null, true, defaultValue1, ""));
        schema.add(new Column("v1", Type.INT, false, AggregateType.SUM, true, defaultValue1, ""));
        schema.add(new Column(mvColumnName, Type.BITMAP, false, AggregateType.BITMAP_UNION, false, defaultValue1, ""));
        short shortKeyColumnCount = 1;
        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(1, schema, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.DUP_KEYS, new OriginStatement(
                "create materialized view test as select K1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, sum(v1), "
                        + "bitmap_union(to_bitmap(K1)) from test group by K1, k2, k3, k4, k5, "
                        + "k6, k7, k8, k9, k10, k11, k12",
                0));
        indexMeta.write(out);
        out.flush();
        out.close();

        List<Expr> params = Lists.newArrayList();
        SlotRef param1 = new SlotRef(new TableName(null, "test"), "c1");
        params.add(param1);
        Map<String, Expr> columnNameToDefineExpr = Maps.newHashMap();
        columnNameToDefineExpr.put(mvColumnName, new FunctionCallExpr(new FunctionName("to_bitmap"), params));
        new Expectations() {
            {
                stmt.parseDefineExprWithoutAnalyze(anyString);
                result = columnNameToDefineExpr;
            }
        };

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        MaterializedIndexMeta readIndexMeta = MaterializedIndexMeta.read(in);
        Assert.assertEquals(1, readIndexMeta.getIndexId());
        List<Column> resultColumns = readIndexMeta.getSchema();
        for (Column column : resultColumns) {
            if (column.getName().equals(mvColumnName)) {
                Assert.assertTrue(column.getDefineExpr() instanceof FunctionCallExpr);
                Assert.assertTrue(column.getType().isBitmapType());
                Assert.assertEquals(AggregateType.BITMAP_UNION, column.getAggregationType());
                Assert.assertEquals("to_bitmap", ((FunctionCallExpr) column.getDefineExpr()).getFnName().getFunction());
            } else {
                Assert.assertEquals(null, column.getDefineExpr());
            }
        }
    }
}
