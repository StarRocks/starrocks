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

package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.common.DdlException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeneratedPartitionColumnMetadataTest {
    private static final String NAME = "__generated_partition_column_x";

    private Column generated(boolean internal) {
        SlotRef expr = new SlotRef(null, "k");
        expr.setType(IntegerType.BIGINT);
        ColumnDef def;
        if (internal) {
            def = ColumnDef.newInternalGeneratedPartitionColumn(NAME, new TypeDef(IntegerType.BIGINT), expr);
        } else {
            def = new ColumnDef(NAME, new TypeDef(IntegerType.BIGINT), null, false, null, null, true,
                    ColumnDef.DefaultValueDef.NOT_SET, null, expr, "");
        }
        def.setAggregateType(AggregateType.REPLACE);
        return ColumnBuilder.buildGeneratedColumn(null, def);
    }

    private OlapTable table(Column generated) {
        Column key = new Column("k", IntegerType.BIGINT);
        key.setIsKey(true);
        Column droppedKey = new Column("j", IntegerType.BIGINT);
        droppedKey.setIsKey(true);
        Column value = new Column("v", IntegerType.BIGINT);
        value.setAggregationType(AggregateType.SUM, false);
        List<Column> schema = List.of(key, droppedKey, value, generated);
        OlapTable table = new OlapTable(1L, "origin_table", schema, KeysType.AGG_KEYS,
                new ListPartitionInfo(PartitionType.LIST, List.of(generated)),
                new HashDistributionInfo(1, List.of(key)));
        table.setBaseIndexMetaId(1L);
        table.setIndexMeta(1L, "origin_table", schema, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        return table;
    }

    @Test
    public void testOriginSurvivesColumnMetadataLifecycle() {
        Column original = generated(true);
        assertTrue(original.isInternalGeneratedPartitionColumn());
        assertTrue(new Column(original).isInternalGeneratedPartitionColumn());
        assertTrue(original.deepCopy().isInternalGeneratedPartitionColumn());
        Column restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), Column.class);
        assertTrue(restored.isInternalGeneratedPartitionColumn());
        ColumnDef def = restored.toColumnDef(table(restored));
        assertTrue(def.isInternalGeneratedPartitionColumn());
        assertTrue(ColumnBuilder.buildGeneratedColumn(table(restored), def).isInternalGeneratedPartitionColumn());
        Column ordinary = generated(false);
        assertFalse(new Column(ordinary).isInternalGeneratedPartitionColumn());
        assertFalse(ordinary.deepCopy().isInternalGeneratedPartitionColumn());
        assertFalse(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(ordinary), Column.class)
                .isInternalGeneratedPartitionColumn());
    }

    @Test
    public void testLegacyMetadataDoesNotRecoverOriginFromName() {
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(generated(true))).getAsJsonObject();
        json.remove("internalGeneratedPartitionColumn");
        Column legacy = GsonUtils.GSON.fromJson(json, Column.class);
        assertTrue(legacy.isGeneratedColumn());
        assertEquals(AggregateType.REPLACE, legacy.getAggregationType());
        assertFalse(legacy.isInternalGeneratedPartitionColumn());
        assertFalse(legacy.toColumnDef(table(legacy)).isInternalGeneratedPartitionColumn());
    }

    @Test
    public void testOriginIsASchemaAttribute() {
        Column internal = generated(true);
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(internal)).getAsJsonObject();
        json.addProperty("internalGeneratedPartitionColumn", false);
        Column user = GsonUtils.GSON.fromJson(json, Column.class);
        assertFalse(internal.equalsIgnoreComment(user));
        assertFalse(internal.differsOnlyInKeyness(user));
        assertFalse(internal.differsOnlyInType(user));
        assertFalse(internal.isSchemaCompatible(user));
    }

    private boolean multiVersionCache(Column column) throws Exception {
        OlapTable table = table(column);
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(table);
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(0), tuple, "OlapScanNode", 1L);
        Method method = OlapScanNode.class.getDeclaredMethod("canUseMultiVersionCache");
        method.setAccessible(true);
        return (boolean) method.invoke(scan);
    }

    @Test
    public void testMultiVersionCacheRequiresTrustedOrigin() throws Exception {
        assertTrue(multiVersionCache(generated(true)));
        assertFalse(multiVersionCache(generated(false)));
    }

    private void dropIndependentKey(Column column, String rollup) throws Exception {
        OlapTable table = table(column);
        Map<Long, LinkedList<Column>> schemas = new java.util.HashMap<>();
        schemas.put(1L, new LinkedList<>(table.getBaseSchema()));
        if (rollup != null) {
            table.setIndexMeta(2L, rollup, table.getBaseSchema(), 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);
            schemas.put(2L, new LinkedList<>(table.getBaseSchema()));
        }
        Method method = SchemaChangeHandler.class.getDeclaredMethod("processDropColumn",
                DropColumnClause.class, OlapTable.class, Map.class, List.class);
        method.setAccessible(true);
        method.invoke(new SchemaChangeHandler(), new DropColumnClause("j", rollup, Map.of()),
                table, schemas, new ArrayList<Index>());
    }

    @Test
    public void testBaseAndRollupKeyDropRequireTrustedOrigin() throws Exception {
        for (String rollup : new String[] {null, "origin_rollup"}) {
            dropIndependentKey(generated(true), rollup);
            InvocationTargetException error = assertThrows(InvocationTargetException.class,
                    () -> dropIndependentKey(generated(false), rollup));
            assertTrue(error.getCause() instanceof DdlException);
            assertTrue(error.getCause().getMessage().contains("REPLACE aggregation method"));
        }
    }
}
