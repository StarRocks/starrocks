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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.thrift.TColumn;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

// Covers AvroSchemaEvolver.plan: it diffs the full writer schema the BE ships against the live table and
// decides, per field, ADD COLUMN / ADD FIELD / MODIFY / skip / PAUSE. The plan only reads table.getColumn,
// so the table is a mock; writer columns are real TColumns carrying a serialized type, exactly as the BE
// builds them. Exercises every fit-predicate family and every pause reason.
public class AvroSchemaEvolverTest {

    private static final Set<String> NO_META = Collections.emptySet();

    // A writer field as the BE ships it: name + serialized type, nullable, no default.
    private static TColumn writer(String name, Type type) {
        TColumn tc = new TColumn();
        tc.setColumn_name(name);
        tc.setType_desc(TypeSerializer.toThrift(type));
        return tc;
    }

    private static PendingSchemaChange schema(TColumn... cols) {
        return new PendingSchemaChange(1, Lists.newArrayList(cols), Collections.emptyList());
    }

    private static StructType struct(StructField... fields) {
        return new StructType(Lists.newArrayList(fields));
    }

    private static OlapTable tableWith(Column... columns) {
        OlapTable table = Mockito.mock(OlapTable.class);
        for (Column c : columns) {
            Mockito.when(table.getColumn(c.getName())).thenReturn(c);
        }
        return table;
    }

    private static AvroSchemaEvolver.Plan plan(OlapTable table, PendingSchemaChange pending) {
        return AvroSchemaEvolver.plan(table, pending, NO_META);
    }

    @Test
    public void addNewColumn() {
        AvroSchemaEvolver.Plan plan = plan(tableWith(), schema(writer("c", IntegerType.INT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        AddColumnClause add = (AddColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals("c", add.getColumnDef().getName());
        Assertions.assertEquals(PrimitiveType.INT, add.getColumnDef().getType().getPrimitiveType());
        Assertions.assertTrue(add.getColumnDef().isAllowNull());
    }

    @Test
    public void skipExactType() {
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", IntegerType.INT)),
                schema(writer("c", IntegerType.INT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void skipWhenColumnIsWider() {
        // int writer into a BIGINT column already fits.
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", IntegerType.BIGINT)),
                schema(writer("c", IntegerType.INT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void skipVarcharHoldsAnything() {
        // A varchar column holds any value, including a record (the reader JSON-encodes it).
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", VarcharType.VARCHAR)),
                schema(writer("c", struct(new StructField("a", IntegerType.INT)))));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void modifyIntToBigint() {
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", IntegerType.INT)),
                schema(writer("c", IntegerType.BIGINT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        ModifyColumnClause mod = (ModifyColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals("c", mod.getColumnDef().getName());
        Assertions.assertEquals(PrimitiveType.BIGINT, mod.getColumnDef().getType().getPrimitiveType());
    }

    @Test
    public void modifyFloatToDouble() {
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", FloatType.FLOAT)),
                schema(writer("c", FloatType.DOUBLE)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        ModifyColumnClause mod = (ModifyColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals(PrimitiveType.DOUBLE, mod.getColumnDef().getType().getPrimitiveType());
    }

    @Test
    public void skipIntegerIntoFloatColumn() {
        // An integer always fits a float/double column.
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", FloatType.DOUBLE)),
                schema(writer("c", IntegerType.INT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void skipDecimalShrink() {
        Type col = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        Type writer = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 8, 2);
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", col)), schema(writer("c", writer)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void modifyDecimalGrow() {
        Type col = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        Type writer = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 2);
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", col)), schema(writer("c", writer)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        Assertions.assertInstanceOf(ModifyColumnClause.class, plan.getClauses().get(0));
    }

    @Test
    public void migrateDecimalV2ColumnToV3() {
        // A writer decimal always maps to v3 (get_avro_type) and the native reader has no V2 path, so a
        // DECIMALV2 column never holds it unchanged. It is migrated to its lossless v3 form DECIMAL128(27, 9)
        // -- the single legal V2 -> v3 conversion -- so the reader can read the field.
        Type col = TypeFactory.createDecimalV2Type(27, 9);
        Type writer = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", col)), schema(writer("c", writer)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        ModifyColumnClause mod = (ModifyColumnClause) plan.getClauses().get(0);
        ScalarType target = (ScalarType) mod.getColumnDef().getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL128, target.getPrimitiveType());
        Assertions.assertEquals(27, target.getScalarPrecision());
        Assertions.assertEquals(9, target.getScalarScale());
    }

    @Test
    public void pauseDecimalV2ColumnWriterBeyondCapacity() {
        // A writer decimal wider than V2's (27, 9) capacity cannot be migrated (the engine only allows
        // V2 -> the exact (27, 9) v3 form), so it pauses for the operator.
        Type col = TypeFactory.createDecimalV2Type(27, 9);
        Type writer = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 30, 4);
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", col)), schema(writer("c", writer)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
        Assertions.assertTrue(plan.getPauseReason().contains("c"));
    }

    @Test
    public void pauseScalarColumnRecordWriter() {
        // Column is a scalar, the field became a record: not representable, pause.
        AvroSchemaEvolver.Plan plan = plan(tableWith(new Column("c", IntegerType.BIGINT)),
                schema(writer("c", struct(new StructField("a", IntegerType.INT)))));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseStructColumnScalarWriter() {
        AvroSchemaEvolver.Plan plan = plan(
                tableWith(new Column("c", struct(new StructField("a", IntegerType.INT)))),
                schema(writer("c", IntegerType.BIGINT)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseCaseOnlyNameDifference() {
        // Live column is "foo"; getColumn is case-insensitive, so the writer field "Foo" resolves to it.
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getColumn("Foo")).thenReturn(new Column("foo", IntegerType.INT));
        AvroSchemaEvolver.Plan plan = plan(table, schema(writer("Foo", IntegerType.INT)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseKeyColumn() {
        Column key = new Column("k", IntegerType.INT);
        key.setIsKey(true);
        AvroSchemaEvolver.Plan plan = plan(tableWith(key), schema(writer("k", IntegerType.BIGINT)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void skipKeyColumnThatFits() {
        // The writer schema carries every field, key columns included. A key column the writer value still
        // fits must not pause the job; only the genuinely new field is added.
        Column key = new Column("k", IntegerType.BIGINT);
        key.setIsKey(true);
        AvroSchemaEvolver.Plan plan = plan(tableWith(key),
                schema(writer("k", IntegerType.BIGINT), writer("v", IntegerType.INT)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        AddColumnClause add = (AddColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals("v", add.getColumnDef().getName());
    }

    @Test
    public void pauseMetadataColumnCollision() {
        AvroSchemaEvolver.Plan plan = AvroSchemaEvolver.plan(tableWith(), schema(writer("off", IntegerType.BIGINT)),
                Sets.newHashSet("off"));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void addFieldToStruct() {
        Column c = new Column("c", struct(new StructField("a", IntegerType.INT)));
        StructType writerType = struct(new StructField("a", IntegerType.INT), new StructField("b", IntegerType.INT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c), schema(writer("c", writerType)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        AddFieldClause add = (AddFieldClause) plan.getClauses().get(0);
        Assertions.assertEquals("c", add.getColName());
        Assertions.assertEquals("b", add.getFieldDesc().getFieldName());
        Assertions.assertTrue(add.getFieldDesc().getNestedParentFieldNames().isEmpty());
    }

    @Test
    public void addFieldToNestedStruct() {
        Column c = new Column("c", struct(
                new StructField("a", IntegerType.INT),
                new StructField("s", struct(new StructField("x", IntegerType.INT)))));
        StructType writerType = struct(
                new StructField("a", IntegerType.INT),
                new StructField("s", struct(new StructField("x", IntegerType.INT),
                        new StructField("y", IntegerType.INT))));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c), schema(writer("c", writerType)));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        AddFieldClause add = (AddFieldClause) plan.getClauses().get(0);
        Assertions.assertEquals("c", add.getColName());
        Assertions.assertEquals("y", add.getFieldDesc().getFieldName());
        Assertions.assertEquals(Lists.newArrayList("s"), add.getFieldDesc().getNestedParentFieldNames());
    }

    @Test
    public void widenColumnToVarcharMax() {
        Column v = new Column("v", new VarcharType(10));
        PendingSchemaChange pending = new PendingSchemaChange(1, Collections.emptyList(), Lists.newArrayList("v"));
        AvroSchemaEvolver.Plan plan = plan(tableWith(v), pending);

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        ModifyColumnClause mod = (ModifyColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals("v", mod.getColumnDef().getName());
        Assertions.assertEquals(StringType.MAX_STRING_LENGTH,
                ((ScalarType) mod.getColumnDef().getType()).getLength());
    }

    @Test
    public void skipArrayColumnThatFits() {
        // ARRAY<BIGINT> already holds an ARRAY<INT> writer (the element widens), so no change.
        Column c = new Column("c", new ArrayType(IntegerType.BIGINT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c), schema(writer("c", new ArrayType(IntegerType.INT))));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseArrayElementTypeChange() {
        // An ARRAY element type that no longer fits cannot be evolved (there is no per-element MODIFY); pause.
        Column c = new Column("c", new ArrayType(IntegerType.INT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c), schema(writer("c", new ArrayType(IntegerType.BIGINT))));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void skipMapColumnThatFits() {
        // Only the map value side is judged; an INT value into a BIGINT-valued map fits.
        Column c = new Column("c", new MapType(VarcharType.VARCHAR, IntegerType.BIGINT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c),
                schema(writer("c", new MapType(VarcharType.VARCHAR, IntegerType.INT))));

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseMapValueTypeChange() {
        Column c = new Column("c", new MapType(VarcharType.VARCHAR, IntegerType.INT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c),
                schema(writer("c", new MapType(VarcharType.VARCHAR, IntegerType.BIGINT))));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseMapNonStringKeyColumn() {
        // Avro map keys are always strings; a MAP<INT,...> column cannot take them even though the
        // value side fits. Mirrors the BE coverage check, which escalates this.
        Column c = new Column("c", new MapType(IntegerType.INT, IntegerType.BIGINT));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c),
                schema(writer("c", new MapType(VarcharType.VARCHAR, IntegerType.INT))));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseCaseOnlyStructSubfield() {
        // The live struct has subfield "Foo"; the writer carries "foo". The BE matches subfield names
        // exactly and keeps escalating, and ADD FIELD cannot resolve it (subfield names are unique
        // ignoring case), so the planner must pause instead of treating the column as covered.
        Column c = new Column("c", struct(new StructField("Foo", IntegerType.INT)));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c),
                schema(writer("c", struct(new StructField("foo", IntegerType.INT)))));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseGeneratedColumnCollision() {
        Column gen = Mockito.mock(Column.class);
        Mockito.when(gen.getName()).thenReturn("g");
        Mockito.when(gen.getType()).thenReturn(IntegerType.INT);
        Mockito.when(gen.isGeneratedColumn()).thenReturn(true);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getColumn("g")).thenReturn(gen);

        AvroSchemaEvolver.Plan plan = plan(table, schema(writer("g", IntegerType.INT)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void pauseAutoIncrementColumnCollision() {
        Column autoInc = Mockito.mock(Column.class);
        Mockito.when(autoInc.getName()).thenReturn("id");
        Mockito.when(autoInc.getType()).thenReturn(IntegerType.BIGINT);
        Mockito.when(autoInc.isAutoIncrement()).thenReturn(true);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getColumn("id")).thenReturn(autoInc);

        AvroSchemaEvolver.Plan plan = plan(table, schema(writer("id", IntegerType.BIGINT)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
    }

    @Test
    public void widenVarbinaryColumnToMax() {
        Column v = new Column("v", TypeFactory.createVarbinary(16));
        PendingSchemaChange pending = new PendingSchemaChange(1, Collections.emptyList(), Lists.newArrayList("v"));
        AvroSchemaEvolver.Plan plan = plan(tableWith(v), pending);

        Assertions.assertFalse(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        ModifyColumnClause mod = (ModifyColumnClause) plan.getClauses().get(0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, mod.getColumnDef().getType().getPrimitiveType());
        Assertions.assertEquals(StringType.MAX_STRING_LENGTH,
                ((ScalarType) mod.getColumnDef().getType()).getLength());
    }

    @Test
    public void skipAlreadyWidenedColumn() {
        // A widen column already at the max width (or an unbounded varbinary) produces no clause, so the
        // daemon re-tick after the widen ALTER has landed sees an empty plan and converges, instead of
        // re-emitting a no-op MODIFY that it would read as "did not converge" and pause the job on.
        Column varcharAtMax = new Column("s", new VarcharType(StringType.MAX_STRING_LENGTH));
        Column varbinaryUnbounded = new Column("b", new VarbinaryType());
        PendingSchemaChange pending =
                new PendingSchemaChange(1, Collections.emptyList(), Lists.newArrayList("s", "b"));
        AvroSchemaEvolver.Plan plan = plan(tableWith(varcharAtMax, varbinaryUnbounded), pending);

        Assertions.assertTrue(plan.getClauses().isEmpty());
        Assertions.assertFalse(plan.shouldPause());
    }

    @Test
    public void pauseNestedStructSubfieldTypeChange() {
        // A subfield whose type changed -- not a new field to add, not a nested struct to recurse into --
        // cannot be evolved (a struct subfield type is not modifiable via ALTER), so it pauses.
        Column c = new Column("c", struct(new StructField("a",
                struct(new StructField("x", IntegerType.INT)))));
        StructType writerType = struct(new StructField("a",
                struct(new StructField("x", VarcharType.VARCHAR))));
        AvroSchemaEvolver.Plan plan = plan(tableWith(c), schema(writer("c", writerType)));

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertTrue(plan.getClauses().isEmpty());
        Assertions.assertTrue(plan.getPauseReason().contains("x"));
    }

    @Test
    public void partialBatchAppliesResolvableThenPauses() {
        // One new field (resolvable -> ADD COLUMN) plus one incompatible field (pause).
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getColumn("b")).thenReturn(new Column("b", IntegerType.BIGINT));
        PendingSchemaChange pending = schema(
                writer("a", IntegerType.INT),
                writer("b", struct(new StructField("x", IntegerType.INT))));
        AvroSchemaEvolver.Plan plan = plan(table, pending);

        Assertions.assertTrue(plan.shouldPause());
        Assertions.assertEquals(1, plan.getClauses().size());
        Assertions.assertInstanceOf(AddColumnClause.class, plan.getClauses().get(0));
        Assertions.assertTrue(plan.getPauseReason().contains("b"));
    }

    @Test
    public void describeClauseRendersReadableAlter() {
        // The pause message for a refused heavy change names the exact ALTER; check each clause kind renders.
        Assertions.assertTrue(AvroSchemaEvolver.describeClause(
                        plan(tableWith(), schema(writer("c", IntegerType.INT))).getClauses().get(0))
                .startsWith("ADD COLUMN c"));
        Assertions.assertTrue(AvroSchemaEvolver.describeClause(
                        plan(tableWith(new Column("c", IntegerType.INT)), schema(writer("c", IntegerType.BIGINT)))
                                .getClauses().get(0))
                .startsWith("MODIFY COLUMN c"));
        Column s = new Column("s", struct(new StructField("a", IntegerType.INT)));
        StructType w = struct(new StructField("a", IntegerType.INT), new StructField("b", IntegerType.INT));
        Assertions.assertTrue(AvroSchemaEvolver.describeClause(
                        plan(tableWith(s), schema(writer("s", w))).getClauses().get(0))
                .contains("ADD FIELD b"));
    }
}
