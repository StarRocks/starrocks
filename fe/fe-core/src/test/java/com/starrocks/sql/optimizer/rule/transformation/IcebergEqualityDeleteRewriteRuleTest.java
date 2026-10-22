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

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.sql.ast.expression.BinaryType;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.PARTITION_KEY;

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
