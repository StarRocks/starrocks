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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.PARTITION_KEY;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;

public class IcebergEqualityDeleteRewriteRuleTest {

    // The LEFT ANTI JOIN that applies equality deletes must compare identity columns with null-safe
    // equals (EQ_FOR_NULL / <=>), not plain EQ: per the Iceberg spec a NULL value in a delete column
    // matches a NULL row value, and plain EQ would evaluate NULL = NULL -> UNKNOWN and silently keep
    // the row alive. The $data_sequence_number bound must stay a strict LT.
    @Test
    public void testBuildOnPredicateUsesNullSafeEqualsForIdentityColumns() {
        ColumnRefOperator leftId = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator leftSeq = new ColumnRefOperator(2, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);
        ColumnRefOperator rightId = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator rightSeq = new ColumnRefOperator(4, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);

        Map<String, ColumnRefOperator> leftCols = new HashMap<>();
        leftCols.put("id", leftId);
        leftCols.put(DATA_SEQUENCE_NUMBER, leftSeq);

        IcebergEqualityDeleteRewriteRule rule = new IcebergEqualityDeleteRewriteRule();
        ScalarOperator onPredicate = Deencapsulation.invoke(
                rule, "buildOnPredicate", leftCols, List.of(rightId, rightSeq), false);

        Map<String, BinaryType> byColumn = new HashMap<>();
        collectBinaryTypes(onPredicate, byColumn);

        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, byColumn.get("id"),
                "identity column must use null-safe equals so NULL-key rows are deleted");
        Assertions.assertEquals(BinaryType.LT, byColumn.get(DATA_SEQUENCE_NUMBER),
                "data sequence number bound must remain a strict less-than");
    }

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));

    // A partitioned delete applies only to its own partition; the anti join must check that unless an equality
    // key match already implies the same partition.
    @Test
    public void testNeedsPartitionMatch() {
        PartitionSpec unpartitioned = PartitionSpec.builderFor(SCHEMA).withSpecId(0).build();
        PartitionSpec byP = PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("p").build();
        PartitionSpec byBucket = PartitionSpec.builderFor(SCHEMA).withSpecId(2).bucket("id", 4).build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(SCHEMA);
        Mockito.when(table.specs()).thenReturn(Map.of(0, unpartitioned, 1, byP, 2, byBucket));

        List<Integer> byId = List.of(1);
        List<Integer> byIdAndP = List.of(1, 2);
        // the key does not contain the partition column
        Assertions.assertTrue(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 1)), byId));
        // identity partition column is part of the key (Flink upsert): the plan stays unchanged
        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byIdAndP, 1)), byIdAndP));
        // a non-identity transform is never implied by the key
        Assertions.assertTrue(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 2)), byId));
        // global deletes need no partition check
        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 0)), byId));
        // one partitioned spec in the group is enough; schemas of other keys are ignored
        Assertions.assertTrue(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byId, 1)), byId));
        // deletes of two specs, each implied by the key: a global delete must still reach data files of the
        // other spec, and a partitioned one must not, which only the partition key tells apart
        Assertions.assertTrue(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byIdAndP, 0), IcebergDeleteSchema.of(byIdAndP, 1)), byIdAndP));
        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 1)), byIdAndP));

        // a real column named $partition_key: keep the plan without the partition check
        Mockito.when(table.schema()).thenReturn(new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()),
                Types.NestedField.optional(3, PARTITION_KEY, Types.StringType.get())));
        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.needsPartitionMatch(
                table, Set.of(IcebergDeleteSchema.of(byId, 1)), byId));
    }

    // The partition key matches when equal. With global deletes a NULL delete-side key matches every row;
    // without them the plain equality stays a hash join key.
    @Test
    public void testBuildOnPredicateMatchesPartitionKey() {
        ColumnRefOperator leftId = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator leftKey = new ColumnRefOperator(2, StringType.STRING, PARTITION_KEY, true);
        ColumnRefOperator rightId = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator rightKey = new ColumnRefOperator(4, StringType.STRING, PARTITION_KEY, true);

        Map<String, ColumnRefOperator> leftCols = new HashMap<>();
        leftCols.put("id", leftId);
        leftCols.put(PARTITION_KEY, leftKey);
        IcebergEqualityDeleteRewriteRule rule = new IcebergEqualityDeleteRewriteRule();

        CompoundPredicateOperator withGlobal = (CompoundPredicateOperator) Deencapsulation.invoke(
                rule, "buildOnPredicate", leftCols, List.of(rightId, rightKey), true);
        Assertions.assertTrue(withGlobal.isAnd());
        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, ((BinaryPredicateOperator) withGlobal.getChild(0)).getBinaryType());
        CompoundPredicateOperator or = (CompoundPredicateOperator) withGlobal.getChild(1);
        Assertions.assertTrue(or.isOr());
        IsNullPredicateOperator isNull = (IsNullPredicateOperator) or.getChild(0);
        Assertions.assertFalse(isNull.isNotNull());
        Assertions.assertEquals(rightKey, isNull.getChild(0));
        assertPartitionKeyEquality(or.getChild(1), leftKey, rightKey);

        CompoundPredicateOperator withoutGlobal = (CompoundPredicateOperator) Deencapsulation.invoke(
                rule, "buildOnPredicate", leftCols, List.of(rightId, rightKey), false);
        assertPartitionKeyEquality(withoutGlobal.getChild(1), leftKey, rightKey);
    }

    @Test
    public void testHasGlobalDeletes() {
        PartitionSpec unpartitioned = PartitionSpec.builderFor(SCHEMA).withSpecId(0).build();
        PartitionSpec byP = PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("p").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.specs()).thenReturn(Map.of(0, unpartitioned, 1, byP));
        List<Integer> byId = List.of(1);

        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.hasGlobalDeletes(
                table, Set.of(IcebergDeleteSchema.of(byId, 1)), byId));
        Assertions.assertTrue(IcebergEqualityDeleteRewriteRule.hasGlobalDeletes(
                table, Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byId, 1)), byId));
        // a global delete of another key does not count
        Assertions.assertFalse(IcebergEqualityDeleteRewriteRule.hasGlobalDeletes(
                table, Set.of(IcebergDeleteSchema.of(List.of(2), 0), IcebergDeleteSchema.of(byId, 1)), byId));
    }

    // With deletes of more than one spec, a table that has a real $partition_key column cannot tell the specs
    // apart, so the rewrite must fail instead of applying partitioned deletes to other specs.
    @Test
    public void testTransformRejectsPartitionKeyColumnAcrossSpecs() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()),
                Types.NestedField.optional(3, PARTITION_KEY, Types.StringType.get()));
        Table table = mockNativeTable(schema);
        List<Integer> byId = List.of(1);
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> runTransform(table, Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byId, 1))));
        Assertions.assertTrue(e.getMessage().contains("span partition specs"), e.getMessage());
    }

    // Deletes of an unpartitioned spec are global: their anti join must not match $spec_id, or they would miss
    // data files of the other spec. Deletes of a partitioned spec implied by the key still match $spec_id.
    @Test
    public void testTransformGlobalDeletesSkipSpecIdMatch() {
        List<Integer> byId = List.of(1);
        List<Integer> byIdAndP = List.of(1, 2);
        OptExpression result = runTransform(mockNativeTable(SCHEMA),
                Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byIdAndP, 1)));

        List<LogicalIcebergEqualityDeleteScanOperator> eqScans = new ArrayList<>();
        collectEqDeleteScans(result, eqScans);
        Assertions.assertEquals(2, eqScans.size());
        Map<String, List<String>> columnsByTable = new HashMap<>();
        for (LogicalIcebergEqualityDeleteScanOperator eqScan : eqScans) {
            columnsByTable.put(eqScan.getTable().getName(),
                    eqScan.getColRefToColumnMetaMap().values().stream().map(Column::getName).toList());
        }
        Assertions.assertEquals(List.of("id", DATA_SEQUENCE_NUMBER), columnsByTable.get("t1_eq_delete_id"));
        Assertions.assertEquals(List.of("id", "p", DATA_SEQUENCE_NUMBER, SPEC_ID), columnsByTable.get("t1_eq_delete_id_p"));
    }

    private static Table mockNativeTable(Schema schema) {
        PartitionSpec unpartitioned = PartitionSpec.builderFor(schema).withSpecId(0).build();
        PartitionSpec byP = PartitionSpec.builderFor(schema).withSpecId(1).identity("p").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(byP);
        Mockito.when(table.specs()).thenReturn(Map.of(0, unpartitioned, 1, byP));
        return table;
    }

    private static OptExpression runTransform(Table nativeTable, Set<IcebergDeleteSchema> deleteSchemas) {
        List<Column> columns = List.of(new Column("id", IntegerType.INT, true), new Column("p", StringType.STRING, true));
        IcebergTable icebergTable = new IcebergTable(1L, "t1", "iceberg_catalog", null, "db1", "t1", "",
                columns, nativeTable, ImmutableMap.of());
        ColumnRefFactory factory = new ColumnRefFactory();
        ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumn = ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> columnToColRef = ImmutableMap.builder();
        for (Column column : columns) {
            ColumnRefOperator ref = factory.create(column.getName(), column.getType(), true);
            colRefToColumn.put(ref, column);
            columnToColRef.put(column, ref);
        }
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(icebergTable, colRefToColumn.build(),
                columnToColRef.build(), -1, null);
        scan.setDeleteSchemas(deleteSchemas);
        List<OptExpression> result = new IcebergEqualityDeleteRewriteRule().transform(
                OptExpression.create(scan), OptimizerFactory.mockContext(factory));
        Assertions.assertEquals(1, result.size());
        return result.get(0);
    }

    private static void collectEqDeleteScans(OptExpression expr, List<LogicalIcebergEqualityDeleteScanOperator> out) {
        if (expr.getOp() instanceof LogicalIcebergEqualityDeleteScanOperator eqScan) {
            out.add(eqScan);
        }
        for (OptExpression child : expr.getInputs()) {
            collectEqDeleteScans(child, out);
        }
    }

    private static void assertPartitionKeyEquality(ScalarOperator op, ColumnRefOperator left, ColumnRefOperator right) {
        BinaryPredicateOperator eq = (BinaryPredicateOperator) op;
        Assertions.assertEquals(BinaryType.EQ, eq.getBinaryType());
        Assertions.assertEquals(List.of(left, right), eq.getChildren());
    }

    // Walk the AND tree and index each leaf comparison by the name of its right-hand (delete-table) column.
    private void collectBinaryTypes(ScalarOperator op, Map<String, BinaryType> out) {
        if (op instanceof BinaryPredicateOperator binary) {
            ColumnRefOperator rightCol = (ColumnRefOperator) binary.getChild(1);
            out.put(rightCol.getName(), binary.getBinaryType());
            return;
        }
        for (ScalarOperator child : op.getChildren()) {
            collectBinaryTypes(child, out);
        }
    }
}
