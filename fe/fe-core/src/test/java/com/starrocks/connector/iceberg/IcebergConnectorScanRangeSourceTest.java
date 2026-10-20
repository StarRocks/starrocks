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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.starrocks.catalog.Type.DATE;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.VARCHAR;

/**
 * Test cases for IcebergConnectorScanRangeSource focusing on initBucketInfo and extractBucketId methods
 */
public class IcebergConnectorScanRangeSourceTest extends TableTestBase {
    private TupleDescriptor tupleDescriptor;

    @BeforeEach
    public void setUp() {
        // Setup tuple descriptor
        tupleDescriptor = new TupleDescriptor(new TupleId(1));

        // Setup slot descriptors
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), tupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));

        tupleDescriptor.addSlot(idSlot);
        tupleDescriptor.addSlot(dataSlot);
    }

    @Test
    public void testExtractBucketIdFromTask() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor, Optional.of(bucketProperties),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());
        for (FileScanTask fileScanTask : fileScanTasks) {
            int mappingId = scanRangeSource.extractBucketId(fileScanTask);
            if (fileScanTask.file().location().endsWith("data-j1.parquet")) {
                // 1 * (64 + 1) + 1 data-j1
                Assertions.assertEquals((BUCKETS_NUMBER2 + 1) + 1, mappingId);
            } else {
                // 2 * (64 + 1) + 1 data-j2
                Assertions.assertEquals(2 * (BUCKETS_NUMBER2 + 1) + 1, mappingId);
            }
        }
    }

    @Test
    public void testExtractBucketIdFromTaskOnlyOneBucketProperty() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        List<BucketProperty> oneBucketProperties = List.of(bucketProperties.get(0));

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor, Optional.of(oneBucketProperties),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());
        for (FileScanTask fileScanTask : fileScanTasks) {
            int mappingId = scanRangeSource.extractBucketId(fileScanTask);
            if (fileScanTask.file().location().endsWith("data-j1.parquet")) {
                System.out.println("J1 mapping id: " + mappingId);
                // 1 data-j1
                Assertions.assertEquals(1, mappingId);
            } else {
                // 2 data-j2
                Assertions.assertEquals(2, mappingId);
            }
        }
    }

    @Test
    public void testSamePartitionIdForSamePartitionKeysAcrossDifferentSources() throws Exception {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema,
                mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        List<BucketProperty> oneBucketProperties = List.of(bucketProperties.get(0));
        // Use the same partition key values for both
        PartitionIdGenerator partitionIdGenerator = PartitionIdGenerator.of();
        IcebergConnectorScanRangeSource scanRangeSource1 = new IcebergConnectorScanRangeSource(
                icebergTable, RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor,
                Optional.of(oneBucketProperties), partitionIdGenerator, false, false);
        IcebergConnectorScanRangeSource scanRangeSource2 = new IcebergConnectorScanRangeSource(
                icebergTable, RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor,
                Optional.of(oneBucketProperties), partitionIdGenerator, false, false);
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertFalse(fileScanTasks.isEmpty());
        for (FileScanTask fileScanTask : fileScanTasks) {
            // Simulate partition id generation for the same partition key values
            long partitionId1 = scanRangeSource1.addPartition(fileScanTask);
            long partitionId2 = scanRangeSource2.addPartition(fileScanTask);
            Assertions.assertEquals(partitionId1, partitionId2, "Partition IDs should " +
                    "be the same for the same partition keys and values");
        }
    }

    /**
     * Test that for an Iceberg V1 table, dropping an identity partition field does not cause NPE
     * when scanning files.
     * Background: For V1 tables, Iceberg's `removeField` does NOT remove the field from the spec.
     * Instead, the field is kept with its transform rewritten to `VoidTransform` (always-null),
     * and the field name stays the same as the source column. This is why we need to check for
     * `isIdentity` when adding partitions.
     */
    @Test
    public void testAddPartitionWithV1DroppedIdentityPartitionFieldDoesNotThrow() throws Exception {
        PartitionSpec specIdentityK1 = PartitionSpec.builderFor(SCHEMA_F).identity("k1").build();
        TestTables.TestTable v1Table = create(SCHEMA_F, specIdentityK1, "ti_k1", 1);
        Assertions.assertEquals(1, v1Table.operations().current().formatVersion());

        DataFile fileBeforeDrop = DataFiles.builder(v1Table.spec())
                .withPath("/path/to/i-before-drop.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("k1=1")
                .withRecordCount(2)
                .build();
        v1Table.newFastAppend().appendFile(fileBeforeDrop).commit();
        // Drop the identity partition field "k1".
        // For V1, this keeps the field in the spec with VoidTransform.
        v1Table.updateSpec().removeField("k1").commit();
        // Sanity check: the new spec still contains a field whose name is "k1"
        // (V1 semantics), but its transform is no longer identity.
        PartitionSpec newSpec = v1Table.spec();
        Assertions.assertTrue(newSpec.fields().stream()
                        .anyMatch(f -> f.name().equals("k1") && !f.transform().isIdentity()),
                "V1 drop identity partition field should keep a non-identity (void) field with the same name");
        DataFile fileAfterDrop = DataFiles.builder(newSpec)
                .withPath("/path/to/i-after-drop.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build();
        v1Table.newFastAppend().appendFile(fileAfterDrop).commit();

        TupleDescriptor localTuple = new TupleDescriptor(new TupleId(101));
        SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
        k1Slot.setType(INT);
        k1Slot.setColumn(new Column("k1", INT));
        localTuple.addSlot(k1Slot);
        SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
        dtSlot.setType(DATE);
        dtSlot.setColumn(new Column("dt", DATE));
        localTuple.addSlot(dtSlot);

        List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v1_drop_id", "iceberg_catalog",
                "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> tasks = Lists.newArrayList(v1Table.newScan().planFiles());
        Assertions.assertFalse(tasks.isEmpty(), "should scan at least one file");
        for (FileScanTask task : tasks) {
            long pid = Assertions.assertDoesNotThrow(() -> scanRangeSource.addPartition(task),
                    "addPartition should not throw NPE for V1 table with dropped identity partition field");
            Assertions.assertTrue(pid >= 0);
        }
    }

    /**
     * V1 iceberg table with the only identity partition field dropped. The spec
     * is rewritten to {@code [VoidTransform(k1)]}. Both lists must end up empty so that
     * BE's {@code _init_partition_values} does NOT read past the end of {@code _partition_values}.
     */
    @Test
    public void testBuildPartitionSlotIdsExcludesVoidFieldAfterDrop() throws Exception {
        ConnectContext prevCtx = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
        try {
            PartitionSpec specIdentityK1 = PartitionSpec.builderFor(SCHEMA_F).identity("k1").build();
            TestTables.TestTable v1Table = create(SCHEMA_F, specIdentityK1, "ti_void_only", 1);

            // Drop the only identity partition field; for V1 this rewrites it to VoidTransform.
            v1Table.updateSpec().removeField("k1").commit();
            PartitionSpec newSpec = v1Table.spec();
            Assertions.assertTrue(newSpec.fields().stream().noneMatch(f -> f.transform().isIdentity()),
                    "after drop, spec should contain no identity field");

            DataFile fileAfterDrop = DataFiles.builder(newSpec)
                    .withPath("/path/to/void-only.parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(fileAfterDrop).commit();

            TupleDescriptor localTuple = new TupleDescriptor(new TupleId(102));
            SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
            k1Slot.setType(INT);
            k1Slot.setColumn(new Column("k1", INT));
            localTuple.addSlot(k1Slot);
            SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
            dtSlot.setType(DATE);
            dtSlot.setColumn(new Column("dt", DATE));
            localTuple.addSlot(dtSlot);

            List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
            IcebergTable icebergTable = new IcebergTable(1, "iceberg_void_only", "iceberg_catalog",
                    "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

            IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                    RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                    PartitionIdGenerator.of(), false, false);

            List<FileScanTask> tasks = Lists.newArrayList(v1Table.newScan().planFiles());
            Assertions.assertFalse(tasks.isEmpty());
            for (FileScanTask task : tasks) {
                long pid = scanRangeSource.addPartition(task);
                THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(task, task.file(), pid);

                List<Integer> identitySlotIds = hdfsScanRange.getIdentity_partition_slot_ids();
                int identitySlotIdsSize = identitySlotIds == null ? 0 : identitySlotIds.size();

                THdfsPartition partitionValue = hdfsScanRange.getPartition_value();
                int keyExprsSize = partitionValue == null || partitionValue.getPartition_key_exprs() == null
                        ? 0 : partitionValue.getPartition_key_exprs().size();

                Assertions.assertEquals(0, identitySlotIdsSize,
                        "VoidTransform field must NOT contribute to identity_partition_slot_ids");
                Assertions.assertEquals(0, keyExprsSize,
                        "VoidTransform field must NOT contribute to partition_key_exprs");
            }
        } finally {
            if (prevCtx == null) {
                ConnectContext.remove();
            } else {
                prevCtx.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testCacheKeyShouldUseEffectiveIdentityKey() throws Exception {
        ConnectContext prevCtx = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
        try {
            PartitionSpec specBothIdentity =
                    PartitionSpec.builderFor(SCHEMA_F).identity("k1").identity("dt").build();
            TestTables.TestTable v1Table = create(SCHEMA_F, specBothIdentity, "ti_reuse", 1);
            Assertions.assertEquals(1, v1Table.operations().current().formatVersion());

            PartitionKey oldKey = new PartitionKey(specBothIdentity, SCHEMA_F);
            oldKey.set(0, null); // k1 = NULL
            oldKey.set(1, null); // dt = NULL
            DataFile oldFile = DataFiles.builder(specBothIdentity)
                    .withPath("/path/to/old-before-drop.parquet")
                    .withFileSizeInBytes(10)
                    .withPartition(oldKey)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(oldFile).commit();

            // Drop the identity partition field "k1". For V1 this rewrites it to VoidTransform,
            // i.e. new spec = [void(k1), identity(dt)]. VoidTransform always writes NULL for k1.
            v1Table.updateSpec().removeField("k1").commit();
            PartitionSpec newSpec = v1Table.spec();
            Assertions.assertNotEquals(specBothIdentity.specId(), newSpec.specId(),
                    "drop should produce a new spec id");
            Assertions.assertTrue(newSpec.fields().stream()
                            .anyMatch(f -> f.name().equals("k1") && !f.transform().isIdentity()),
                    "after V1 drop, k1 should still be present but with VoidTransform");

            // New file under the new spec, partition struct = (void=NULL, dt=NULL).
            PartitionKey newKey = new PartitionKey(newSpec, SCHEMA_F);
            newKey.set(0, null); // void(k1) = NULL (mandatory)
            newKey.set(1, null); // dt = NULL (same value as old file)
            DataFile newFile = DataFiles.builder(newSpec)
                    .withPath("/path/to/new-after-drop.parquet")
                    .withFileSizeInBytes(10)
                    .withPartition(newKey)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(newFile).commit();

            // Build the tuple descriptor with both partition columns materialised.
            TupleDescriptor localTuple = new TupleDescriptor(new TupleId(201));
            SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
            k1Slot.setType(INT);
            k1Slot.setColumn(new Column("k1", INT));
            localTuple.addSlot(k1Slot);
            SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
            dtSlot.setType(DATE);
            dtSlot.setColumn(new Column("dt", DATE));
            localTuple.addSlot(dtSlot);

            List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
            IcebergTable icebergTable = new IcebergTable(1, "iceberg_reuse", "iceberg_catalog",
                    "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

            IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                    RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                    PartitionIdGenerator.of(), false, false);

            List<FileScanTask> allTasks = Lists.newArrayList(v1Table.newScan().planFiles());
            FileScanTask oldTask = allTasks.stream()
                    .filter(t -> t.file().path().toString().endsWith("old-before-drop.parquet"))
                    .findFirst().orElseThrow();
            FileScanTask newTask = allTasks.stream()
                    .filter(t -> t.file().path().toString().endsWith("new-after-drop.parquet"))
                    .findFirst().orElseThrow();
            // Sanity check: the two files come from different specs.
            Assertions.assertNotEquals(oldTask.spec().specId(), newTask.spec().specId(),
                    "old and new file must use different specs to reproduce the bug");

            long oldPid = scanRangeSource.addPartition(oldTask);
            long newPid = scanRangeSource.addPartition(newTask);

            Assertions.assertNotEquals(oldPid, newPid,
                    "Files from different specs must NOT share a partition id, even when their "
                            + "raw Iceberg partition structs happen to be equal.");

            THdfsScanRange oldRange = scanRangeSource.buildScanRange(oldTask, oldTask.file(), oldPid);
            THdfsScanRange newRange = scanRangeSource.buildScanRange(newTask, newTask.file(), newPid);

            int oldKeyExprs = oldRange.getPartition_value().getPartition_key_exprs().size();
            int oldIdentitySlotIds = oldRange.getIdentity_partition_slot_ids() == null
                    ? 0 : oldRange.getIdentity_partition_slot_ids().size();
            int newKeyExprs = newRange.getPartition_value().getPartition_key_exprs().size();
            int newIdentitySlotIds = newRange.getIdentity_partition_slot_ids() == null
                    ? 0 : newRange.getIdentity_partition_slot_ids().size();

            Assertions.assertEquals(2, oldKeyExprs);
            Assertions.assertEquals(2, oldIdentitySlotIds);
            Assertions.assertEquals(1, newKeyExprs);
            Assertions.assertEquals(1, newIdentitySlotIds);
            Assertions.assertEquals(newKeyExprs, newIdentitySlotIds,
                    "partition_key_exprs.size() and identity_partition_slot_ids.size() must match -- "
                            + "otherwise BE's _init_partition_values goes out of sync.");
        } finally {
            if (prevCtx == null) {
                ConnectContext.remove();
            } else {
                prevCtx.setThreadLocalInfo();
            }
        }
    }

    // Data files and equality delete files get the same key exactly when they share spec and partition.
    @Test
    public void testBuildPartitionKey() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        PartitionSpec unpartitioned = PartitionSpec.builderFor(schema).withSpecId(0).build();
        PartitionSpec byP = PartitionSpec.builderFor(schema).withSpecId(1).identity("p").build();
        PartitionSpec byPAgain = PartitionSpec.builderFor(schema).withSpecId(2).identity("p").build();

        Assertions.assertNull(IcebergConnectorScanRangeSource.buildPartitionKey(unpartitioned, null));
        String a = IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("a"));
        Assertions.assertEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("a")));
        Assertions.assertNotEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("b")));
        // the same partition value under another spec is another partition
        Assertions.assertNotEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byPAgain, partition("a")));
        // a null partition value differs from every string value
        String nullKey = IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition(null));
        for (String value : List.of("", "n", "null")) {
            Assertions.assertNotEquals(nullKey, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition(value)));
        }
    }

    // int -> long and float -> double promotions: a side that sees the field under either type, holding a value
    // of either class, must produce the same key.
    @Test
    public void testBuildPartitionKeyAcrossTypePromotion() {
        PartitionSpec byIntP = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "p", Types.IntegerType.get()))).withSpecId(0).identity("p").build();
        PartitionSpec byLongP = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "p", Types.LongType.get()))).withSpecId(0).identity("p").build();
        String key = IcebergConnectorScanRangeSource.buildPartitionKey(byIntP, partition(5));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byLongP, partition(5L)));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byLongP, partition(5)));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byIntP, partition(5L)));

        PartitionSpec byFloatQ = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "q", Types.FloatType.get()))).withSpecId(0).identity("q").build();
        PartitionSpec byDoubleQ = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "q", Types.DoubleType.get()))).withSpecId(0).identity("q").build();
        Assertions.assertEquals(IcebergConnectorScanRangeSource.buildPartitionKey(byFloatQ, partition(1.5f)),
                IcebergConnectorScanRangeSource.buildPartitionKey(byDoubleQ, partition(1.5d)));
    }

    // Only the column the equality delete rewrite adds is the partition key; a real column of that name is not.
    @Test
    public void testIsPartitionKeyColumn() {
        Schema plain = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Schema withRealColumn = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, IcebergTable.PARTITION_KEY, Types.StringType.get()));
        Assertions.assertTrue(IcebergConnectorScanRangeSource.isPartitionKeyColumn(IcebergTable.PARTITION_KEY, plain));
        Assertions.assertFalse(IcebergConnectorScanRangeSource.isPartitionKeyColumn("id", plain));
        Assertions.assertFalse(
                IcebergConnectorScanRangeSource.isPartitionKeyColumn(IcebergTable.PARTITION_KEY, withRealColumn));
    }

    // Each equality-id group has its own delete scan, which must only read the delete files of that group.
    @Test
    public void testDeleteFileScanRangesKeepOnlyTheirEqualityGroup() {
        DeleteFile deletesByData = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofEqualityDeletes(2)
                .withPath("/path/to/data-a-deletes-by-data.orc")
                .withFormat(FileFormat.ORC)
                .withFileSizeInBytes(10)
                .withPartitionPath("data_bucket=0")
                .withRecordCount(1)
                .build();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A2_DELETES).addDeletes(deletesByData).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());
        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        Assertions.assertEquals(2, fileScanTask.deletes().size());

        Assertions.assertEquals(List.of(FILE_A2_DELETES.location()), deleteScanPaths(icebergTable, fileScanTask, 1));
        Assertions.assertEquals(List.of(deletesByData.location()), deleteScanPaths(icebergTable, fileScanTask, 2));
    }

    private List<String> deleteScanPaths(IcebergTable icebergTable, FileScanTask fileScanTask, int equalityId) {
        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY,
                IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, List.of(equalityId)),
                tupleDescriptor, Optional.empty(), PartitionIdGenerator.of(), false, false);
        List<String> paths = new ArrayList<>();
        for (TScanRangeLocations locations : scanRangeSource.toScanRanges(fileScanTask)) {
            paths.add(locations.getScan_range().getHdfs_scan_range().getFull_path());
        }
        return paths;
    }

    private static StructLike partition(Object value) {
        return new StructLike() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass) {
                return javaClass.cast(value);
            }

            @Override
            public <T> void set(int pos, T newValue) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
