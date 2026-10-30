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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static com.starrocks.type.VarcharType.VARCHAR;

/**
 * A data file's partition tuple is laid out positionally per the spec it was written with, so it only
 * decodes correctly against that spec. These two cases guard the two halves of that contract in
 * {@link IcebergMetadata#icebergPartitionValues}; removing either production change fails one of them.
 *
 * Fixture mirrors what StarRocks DDL produces, taken from the Iceberg metadata a 4.1.6-ee cluster
 * wrote: `PARTITION BY (d1, d2)` then `ALTER TABLE ... DROP PARTITION COLUMN d1` leaves
 * spec 0 = [identity(d1), identity(d2)] and spec 1 = [identity(d2)]. The field is removed rather than
 * left as a void placeholder, so d2 moves from index 1 to index 0.
 */
public class IcebergPartitionValuesSpecTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "d1", Types.StringType.get()),
            Types.NestedField.required(3, "d2", Types.StringType.get()));

    /** The schema after the column itself is dropped: field id 2 is gone, 3 (d2) survives. */
    private static final Schema SCHEMA_WITHOUT_D1 = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(3, "d2", Types.StringType.get()));

    /** The spec the old file was written under. */
    private static final PartitionSpec SPEC_0 =
            PartitionSpec.builderFor(SCHEMA).withSpecId(0).identity("d1").identity("d2").build();

    /** The current spec after DROP PARTITION COLUMN d1. */
    private static final PartitionSpec SPEC_1 =
            PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("d2").build();

    /** The same current spec once d1 no longer exists in the schema at all. */
    private static final PartitionSpec SPEC_1_AFTER_COLUMN_DROP =
            PartitionSpec.builderFor(SCHEMA_WITHOUT_D1).withSpecId(1).identity("d2").build();

    private static final String D1_VALUE = "2026-01-01";
    private static final String D2_VALUE = "2026-06-01";

    private static IcebergTable table(PartitionSpec currentSpec, Schema currentSchema) {
        BaseTable nativeTable = Mockito.mock(BaseTable.class);
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata meta = Mockito.mock(TableMetadata.class);
        Mockito.when(nativeTable.schema()).thenReturn(currentSchema);
        Mockito.when(nativeTable.spec()).thenReturn(currentSpec);
        Mockito.when(nativeTable.operations()).thenReturn(ops);
        Mockito.when(ops.current()).thenReturn(meta);
        Mockito.when(meta.formatVersion()).thenReturn(2);

        List<Column> srSchema = Lists.newArrayList(
                new Column("d1", VARCHAR),
                new Column("d2", VARCHAR));
        return IcebergTable.builder()
                .setId(1)
                .setSrTableName("logs")
                .setCatalogName("cat")
                .setResourceName("res")
                .setCatalogDBName("icedb")
                .setCatalogTableName("logs")
                .setFullSchema(srSchema)
                .setIcebergProperties(Maps.newHashMap())
                .setNativeTable(nativeTable)
                .build();
    }

    /** A scan task for a file written under {@code writtenWith}, carrying both partition values. */
    private static FileScanTask oldFileTask() {
        PartitionData partition = new PartitionData(SPEC_0.partitionType());
        partition.set(0, D1_VALUE);
        partition.set(1, D2_VALUE);

        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.partition()).thenReturn(partition);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.spec()).thenReturn(SPEC_0);
        Mockito.when(task.file()).thenReturn(dataFile);
        return task;
    }

    /**
     * Decoding must use the file's own spec. Against the table's current spec, index 0 holds d1's
     * value but is labelled d2, so residual pruning compares the wrong value and can drop a file that
     * should have matched.
     */
    @Test
    public void testOldFileDecodesWithItsOwnSpec() {
        Map<String, String> values =
                IcebergMetadata.icebergPartitionValues(oldFileTask(), table(SPEC_1, SCHEMA), false);

        Assertions.assertEquals(D2_VALUE, values.get("d2"),
                "d2 must decode to its own value; reading d1's value here is what prunes the file away");
    }

    /**
     * The file's own spec can reference a column later dropped from the table, which is legal once the
     * column has left the current spec. {@code findColumnName} then returns null, and a null key NPEs
     * in {@link com.starrocks.connector.PartitionCastPredicatePruner#partitionMayMatch}, which
     * lower-cases every key.
     */
    @Test
    public void testDroppedSourceColumnEmitsNoNullKey() {
        Map<String, String> values = IcebergMetadata.icebergPartitionValues(
                oldFileTask(), table(SPEC_1_AFTER_COLUMN_DROP, SCHEMA_WITHOUT_D1), false);

        Assertions.assertFalse(values.containsKey(null),
                "a null key NPEs in partitionMayMatch when it lower-cases every key");
        Assertions.assertEquals(D2_VALUE, values.get("d2"),
                "the surviving partition column must still decode to its own value");
    }
}
