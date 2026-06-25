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

package com.starrocks.connector.iceberg.procedure;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.TestTables;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class IcebergMaintenanceTaskStatsTest extends TableTestBase {

    @Test
    public void testNullOperation() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        Assertions.assertFalse(stats.hasMaterialChange());
        Assertions.assertEquals("{}", stats.toJson());
        // collectOutputs is a no-op when the operation is unset
        stats.collectOutputs(Mockito.mock(Table.class));
        Assertions.assertEquals("{}", stats.toJson());
    }

    @Test
    public void testExpireSnapshotsCollectAndJson() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(3);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.snapshots()).thenReturn(List.of(Mockito.mock(Snapshot.class)));
        stats.collectOutputs(table);

        Assertions.assertEquals(1, stats.getSnapshotCountOutput());
        Assertions.assertTrue(stats.hasMaterialChange());

        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(3, json.get("snapshot_count_input").getAsLong());
        Assertions.assertEquals(1, json.get("snapshot_count_output").getAsLong());
        Assertions.assertEquals(2, json.get("snapshot_removed_count").getAsLong());
    }

    @Test
    public void testExpireSnapshotsSkippedWhenNothingRemoved() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(2);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.snapshots())
                .thenReturn(List.of(Mockito.mock(Snapshot.class), Mockito.mock(Snapshot.class)));
        stats.collectOutputs(table);

        // input == output: nothing was removed
        Assertions.assertFalse(stats.hasMaterialChange());
    }

    @Test
    public void testExpireSnapshotsJsonOmitsUncollectedOutput() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.EXPIRE_SNAPSHOTS);
        stats.setSnapshotCountInput(5);
        // output never collected (-1): not emitted, and no removed_count either
        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(5, json.get("snapshot_count_input").getAsLong());
        Assertions.assertFalse(json.has("snapshot_count_output"));
        Assertions.assertFalse(json.has("snapshot_removed_count"));
    }

    @Test
    public void testRewriteManifestsMaterialChangeAndJson() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);
        stats.setManifestCountInput(5);
        stats.setManifestBytesInput(1000);
        // rewrite material change is driven purely by executed
        Assertions.assertFalse(stats.hasMaterialChange());
        stats.setExecuted(true);
        Assertions.assertTrue(stats.hasMaterialChange());

        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(5, json.get("manifest_file_count_input").getAsLong());
        Assertions.assertEquals(1000, json.get("manifest_bytes_total_input").getAsLong());
        // output side never collected: omitted
        Assertions.assertFalse(json.has("manifest_file_count_output"));
    }

    @Test
    public void testRewriteManifestsCollectOutputs() {
        TestTables.TestTable table = create(SCHEMA_A, SPEC_A, "stats_rewrite_out", 2);
        // two separate fast appends produce two manifests
        table.newFastAppend().appendFile(FILE_A).commit();
        table.newFastAppend().appendFile(FILE_A_1).commit();

        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);
        stats.setManifestTargetSizeBytes(8L * 1024 * 1024);
        stats.collectOutputs(table);

        Assertions.assertEquals(2, stats.getManifestCountOutput());
        Assertions.assertTrue(stats.getManifestBytesOutput() > 0);
        // both manifests are far below half the target size, so both count as residual small files
        Assertions.assertEquals(2, stats.getManifestSmallFilesOutput());

        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(2, json.get("manifest_file_count_output").getAsLong());
        Assertions.assertEquals(2, json.get("small_manifest_files_count_output").getAsLong());
    }

    @Test
    public void testRewriteManifestsCollectOutputsNoSnapshot() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REWRITE_MANIFESTS);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.currentSnapshot()).thenReturn(null);
        stats.collectOutputs(table);
        // nothing collected, output stays at the -1 sentinel
        Assertions.assertEquals(-1, stats.getManifestCountOutput());
    }

    @Test
    public void testRemoveOrphanFilesMaterialChangeAndJson() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);
        stats.addOrphanDetected(5);
        stats.addOrphanRemoved(3, 300);
        stats.setPartiallyApplied(true);
        Assertions.assertTrue(stats.hasMaterialChange());
        Assertions.assertTrue(stats.isPartiallyApplied());
        Assertions.assertEquals(5, stats.getOrphanFilesDetected());
        Assertions.assertEquals(3, stats.getOrphanFilesRemoved());
        Assertions.assertEquals(300, stats.getOrphanBytesRemoved());

        // collectOutputs is a no-op for orphan removal (accumulated during execute)
        stats.collectOutputs(Mockito.mock(Table.class));

        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(5, json.get("orphan_file_count_detected").getAsLong());
        Assertions.assertEquals(3, json.get("orphan_file_removed_count").getAsLong());
        Assertions.assertEquals(300, json.get("orphan_bytes_removed").getAsLong());
    }

    @Test
    public void testRemoveOrphanFilesSkippedWhenNothingRemoved() {
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        stats.setOperation(IcebergTableOperation.REMOVE_ORPHAN_FILES);
        stats.addOrphanDetected(4);
        // detected some, removed none: not a material change
        Assertions.assertFalse(stats.hasMaterialChange());

        JsonObject json = JsonParser.parseString(stats.toJson()).getAsJsonObject();
        Assertions.assertEquals(0, json.get("orphan_file_removed_count").getAsLong());
    }
}
