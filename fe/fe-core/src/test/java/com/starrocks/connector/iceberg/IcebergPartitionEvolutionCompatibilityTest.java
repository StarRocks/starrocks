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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.starrocks.connector.iceberg.TableTestBase.SCHEMA_E;
import static com.starrocks.connector.iceberg.TableTestBase.SCHEMA_H;

/**
 * {@link IcebergPartitionUtils#checkPartitionEvolutionCompatible} on real in-memory Iceberg tables whose
 * partition spec is evolved through the Iceberg API, as a writer would do it.
 */
public class IcebergPartitionEvolutionCompatibilityTest {
    @TempDir
    public File temp;

    private TestTables.TestTable createV2(Schema schema, String name, PartitionSpec spec) {
        File tableDir = new File(temp, name);
        return TestTables.create(tableDir, name, schema, spec, 2);
    }

    private TestTables.TestTable createV1(Schema schema, String name, PartitionSpec spec) {
        File tableDir = new File(temp, name);
        return TestTables.create(tableDir, name, schema, spec, 1);
    }

    private static Optional<String> check(TestTables.TestTable table, String... refColumns) {
        return IcebergPartitionUtils.checkPartitionEvolutionCompatible(table, ImmutableList.copyOf(refColumns));
    }

    @Test
    public void testSingleSpecIsCompatible() {
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_single",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").build());
        Assertions.assertEquals(1, table.specs().size());
        Assertions.assertTrue(check(table, "k2").isEmpty());
        Assertions.assertTrue(check(table).isEmpty());
    }

    @Test
    public void testAppendedFieldIsCompatibleForPrefixColumns() {
        // (k2) -> (k2, k3): the appended field does not move k2, so an MV partitioned by k2 still maps by position.
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_append",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").build());
        table.updateSpec().addField("k3").commit();
        Assertions.assertEquals(2, table.specs().size());
        Assertions.assertEquals(2, table.spec().fields().size());

        Assertions.assertTrue(check(table, "k2").isEmpty());
        Assertions.assertTrue(check(table).isEmpty());
        // column lookup is case-insensitive, as elsewhere in the analyzer
        Assertions.assertTrue(check(table, "K2").isEmpty());

        // k3 is absent from spec 0, so spec-0 partition names have no value for it.
        Optional<String> reason = check(table, "k3");
        Assertions.assertTrue(reason.isPresent());
        Assertions.assertTrue(reason.get().contains("k3"), reason.get());
        Assertions.assertTrue(check(table, "k2", "k3").isPresent());
    }

    @Test
    public void testTwiceAppendedKeepsSharedPrefix() {
        // (k2) -> (k2, k3) -> (k2, k3, k4): only k2 is present in every spec.
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_append2",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").build());
        table.updateSpec().addField("k3").commit();
        table.updateSpec().addField("k4").commit();
        Assertions.assertEquals(3, table.specs().size());

        Assertions.assertTrue(check(table, "k2").isEmpty());
        for (String column : new String[] {"k3", "k4"}) {
            Optional<String> reason = check(table, column);
            Assertions.assertTrue(reason.isPresent());
            Assertions.assertTrue(reason.get().contains("is not in every historical partition spec"), reason.get());
        }
    }

    @Test
    public void testReplacedFieldIsIncompatible() {
        // (k2) -> (k3): position 0 changed its source column, so positions no longer line up across specs.
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_replace",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").build());
        table.updateSpec().removeField("k2").addField("k3").commit();
        Assertions.assertEquals(2, table.specs().size());

        for (Optional<String> reason : ImmutableList.of(check(table), check(table, "k3"))) {
            Assertions.assertTrue(reason.isPresent());
            Assertions.assertTrue(reason.get().contains("differs from"), reason.get());
        }
    }

    @Test
    public void testDroppedFieldIsIncompatible() {
        // (k2, k3) -> (k2): the old spec is longer than the current one, so old partition names carry a
        // value the current partition columns do not know about.
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_drop",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").identity("k3").build());
        table.updateSpec().removeField("k3").commit();
        Assertions.assertEquals(2, table.specs().size());

        Optional<String> reason = check(table, "k2");
        Assertions.assertTrue(reason.isPresent());
        Assertions.assertTrue(reason.get().contains("has more fields than"), reason.get());
    }

    @Test
    public void testTransformChangeIsIncompatible() {
        // month(ts) -> identity(ts): the same source column at the same position, but updateSpec() gives the
        // new field a new field id, so the field id and the transform both differ. A change of the transform
        // alone (same field id) is covered by the t0_date_month_identity_evolution CREATE tests.
        TestTables.TestTable table = createV2(SCHEMA_E, "evo_transform",
                PartitionSpec.builderFor(SCHEMA_E).month("ts").build());
        table.updateSpec().removeField("ts_month").addField("ts").commit();
        Assertions.assertEquals(2, table.specs().size());

        for (Optional<String> reason : ImmutableList.of(check(table), check(table, "ts"))) {
            Assertions.assertTrue(reason.isPresent());
            Assertions.assertTrue(reason.get().contains("differs from"), reason.get());
        }
    }

    @Test
    public void testVoidFieldInCurrentSpecIsIncompatible() {
        // format v1 keeps a dropped field in place with a void transform: (k2, k3) -> (k2, void(k3)).
        TestTables.TestTable table = createV1(SCHEMA_H, "evo_v1_void",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").identity("k3").build());
        table.updateSpec().removeField("k3").commit();
        Assertions.assertEquals(2, table.specs().size());
        Assertions.assertTrue(table.spec().fields().get(1).transform().isVoid());

        Optional<String> reason = check(table, "k2");
        Assertions.assertTrue(reason.isPresent());
        Assertions.assertTrue(reason.get().contains("dropped field"), reason.get());
    }

    @Test
    public void testFieldIdsOutOfOrderAreIncompatible() {
        // (k2, k3) -> (k3) -> (k3, k2): format v2 recycles k2's old field id when it is added back, so the
        // appended field has a smaller id than the one before it.
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_recycle",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").identity("k3").build());
        table.updateSpec().removeField("k2").commit();
        table.updateSpec().addField("k2").commit();
        Assertions.assertEquals(3, table.specs().size());
        Assertions.assertTrue(table.spec().fields().get(1).fieldId() < table.spec().fields().get(0).fieldId());

        Optional<String> reason = check(table, "k3");
        Assertions.assertTrue(reason.isPresent());
        Assertions.assertTrue(reason.get().contains("not in the order they were added"), reason.get());
    }

    @Test
    public void testUnknownRefColumnIsIncompatible() {
        TestTables.TestTable table = createV2(SCHEMA_H, "evo_unknown",
                PartitionSpec.builderFor(SCHEMA_H).identity("k2").build());
        table.updateSpec().addField("k3").commit();
        List<String> refColumns = ImmutableList.of("k5");
        Optional<String> reason = IcebergPartitionUtils.checkPartitionEvolutionCompatible(table, refColumns);
        Assertions.assertTrue(reason.isPresent());
        Assertions.assertTrue(reason.get().contains("k5"), reason.get());
    }
}
