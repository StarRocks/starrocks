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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ColumnGsonSerializationTest.java

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
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.ColumnDef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ColumnGsonSerializationTest {

    private static String fileName = "./ColumnGsonSerializationTest";

    @AfterEach
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    public static class ColumnList implements Writable {
        @SerializedName(value = "columns")
        public List<Column> columns = Lists.newArrayList();

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

    }

    @Test
    public void testSerializeDefaultExprHasArgumentsPreserved() {
        // current_timestamp(3) -- hasArguments must survive a Gson roundtrip.
        FunctionCallExpr ctsExpr = new FunctionCallExpr("current_timestamp",
                Lists.newArrayList(new IntLiteral(3L, Type.INT)));
        Column tsCol = new Column("ts", Type.DATETIME, false, null, true,
                new ColumnDef.DefaultValueDef(true, true, ctsExpr), "");

        Assertions.assertNotNull(tsCol.getDefaultExpr());
        Assertions.assertTrue(tsCol.getDefaultExpr().hasArgs(),
                "parser-built DefaultExpr for current_timestamp(3) must carry hasArguments=true");

        String json = GsonUtils.GSON.toJson(tsCol);
        Column restored = GsonUtils.GSON.fromJson(json, Column.class);

        Assertions.assertNotNull(restored.getDefaultExpr());
        Assertions.assertTrue(restored.getDefaultExpr().hasArgs(),
                "hasArguments must persist across Gson roundtrip");
        // VARY rather than CONST: precision-bearing time functions are not 'empty' generators.
        Assertions.assertEquals(Column.DefaultValueType.VARY, restored.getDefaultValueType(),
                "current_timestamp(N) must stay classified as VARY after roundtrip");
    }

    @Test
    public void testLegacyDefaultExprWithoutHasArgumentsRestoresFromString() {
        // Columns persisted before @SerializedName("hasArguments") existed wrote JSON without the field.
        // Simulate that by stripping "hasArguments":true from the serialized form and verify that
        // gsonPostProcess recovers the flag from the expr string.
        FunctionCallExpr ctsExpr = new FunctionCallExpr("current_timestamp",
                Lists.newArrayList(new IntLiteral(3L, Type.INT)));
        Column tsCol = new Column("ts", Type.DATETIME, false, null, true,
                new ColumnDef.DefaultValueDef(true, true, ctsExpr), "");
        String legacyJson = GsonUtils.GSON.toJson(tsCol).replaceAll(",\"hasArguments\":(true|false)", "");
        Assertions.assertFalse(legacyJson.contains("hasArguments"),
                "test precondition: hasArguments must be absent from the simulated legacy JSON");

        Column restored = GsonUtils.GSON.fromJson(legacyJson, Column.class);

        Assertions.assertNotNull(restored.getDefaultExpr());
        Assertions.assertEquals("current_timestamp(3)", restored.getDefaultExpr().getExpr());
        Assertions.assertTrue(restored.getDefaultExpr().hasArgs(),
                "legacy persisted current_timestamp(3) must recover hasArguments via gsonPostProcess");
        Assertions.assertEquals(Column.DefaultValueType.VARY, restored.getDefaultValueType());

        // An empty-arg form persisted under the same legacy schema must stay empty.
        FunctionCallExpr emptyExpr = new FunctionCallExpr("current_timestamp", Lists.newArrayList());
        Column tsEmpty = new Column("ts", Type.DATETIME, false, null, true,
                new ColumnDef.DefaultValueDef(true, false, emptyExpr), "");
        String legacyEmptyJson =
                GsonUtils.GSON.toJson(tsEmpty).replaceAll(",\"hasArguments\":(true|false)", "");
        Column restoredEmpty = GsonUtils.GSON.fromJson(legacyEmptyJson, Column.class);
        Assertions.assertFalse(restoredEmpty.getDefaultExpr().hasArgs(),
                "legacy current_timestamp() must remain hasArguments=false");
        Assertions.assertEquals(Column.DefaultValueType.CONST, restoredEmpty.getDefaultValueType());

        // Same empty form but with trailing whitespace in the persisted expr string. The regex
        // pre-check trims, so without symmetric trimming on the endsWith("()") guard the backfill
        // would misclassify this as having args. Both checks must operate on the trimmed value.
        String legacyEmptyWithSpaces = legacyEmptyJson.replace(
                "\"expr\":\"current_timestamp()\"", "\"expr\":\"current_timestamp() \"");
        Column restoredEmptyWithSpaces = GsonUtils.GSON.fromJson(legacyEmptyWithSpaces, Column.class);
        Assertions.assertFalse(restoredEmptyWithSpaces.getDefaultExpr().hasArgs(),
                "legacy current_timestamp() with trailing whitespace must remain hasArguments=false");
        Assertions.assertEquals(Column.DefaultValueType.CONST, restoredEmptyWithSpaces.getDefaultValueType());
    }

    @Test
    public void testSerializeColumn() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Column c1 = new Column("c1", Type.fromPrimitiveType(PrimitiveType.BIGINT), true, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("1")), "abc");

        String c1Json = GsonUtils.GSON.toJson(c1);
        Text.writeString(out, c1Json);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        String readJson = Text.readString(in);
        Column readC1 = GsonUtils.GSON.fromJson(readJson, Column.class);

        Assertions.assertEquals(c1, readC1);
    }
}
