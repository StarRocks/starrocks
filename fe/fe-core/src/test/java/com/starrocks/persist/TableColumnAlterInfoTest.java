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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableColumnAlterInfoTest {
    @Test
    public void test() {
        TableColumnAlterInfo info = new TableColumnAlterInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, Collections.emptyMap());

        Assertions.assertEquals(1, info.getDbId());
        Assertions.assertEquals(1, info.getTableId());
        Assertions.assertEquals(0, info.getIndexes().size());
        Assertions.assertEquals(0, info.getIndexSchemaMap().size());
        Assertions.assertEquals(0, info.getJobId());
        Assertions.assertEquals(1, info.getTxnId());
        Assertions.assertEquals(0, info.getIndexToNewSchemaId().size());

        TableColumnAlterInfo info2 = new TableColumnAlterInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, Collections.emptyMap());

        Assertions.assertEquals(info.hashCode(), info2.hashCode());
        Assertions.assertEquals(info, info2);
        Assertions.assertNotNull(info.toString());

    }

    @Test
    public void testEditLogRoundtripPreservesCurrentTimestampPrecision() {
        // Edit-log replay path: TableColumnAlterInfo carries the new schema as List<Column>, written via
        // GsonUtils.GSON.toJson and read back symmetrically. A Column whose default is current_timestamp(3)
        // must come out the other side with hasArguments still true, otherwise replay silently demotes the
        // precision-bearing generator to current_timestamp().
        FunctionCallExpr ctsExpr = new FunctionCallExpr("current_timestamp",
                Lists.newArrayList(new IntLiteral(3L, IntegerType.INT)));
        Column tsCol = new Column("ts", DateType.DATETIME, false, null, true,
                new ColumnDef.DefaultValueDef(true, true, ctsExpr), "");
        Assertions.assertTrue(tsCol.getDefaultExpr().hasArgs());

        Map<Long, List<Column>> indexSchemaMap = new HashMap<>();
        indexSchemaMap.put(100L, Lists.newArrayList(tsCol));

        TableColumnAlterInfo info = new TableColumnAlterInfo(1, 1,
                indexSchemaMap, Collections.emptyList(), 0, 1, Collections.emptyMap());

        String json = GsonUtils.GSON.toJson(info);
        TableColumnAlterInfo replayed = GsonUtils.GSON.fromJson(json, TableColumnAlterInfo.class);

        Column replayedTs = replayed.getIndexSchemaMap().get(100L).get(0);
        Assertions.assertNotNull(replayedTs.getDefaultExpr());
        Assertions.assertTrue(replayedTs.getDefaultExpr().hasArgs(),
                "edit-log replay must preserve hasArguments for current_timestamp(3)");
        Assertions.assertEquals(Column.DefaultValueType.VARY, replayedTs.getDefaultValueType());
    }

}
