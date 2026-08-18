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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;


public class IcebergPartitionStatsProviderTest extends TableTestBase {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private static void attachStatsFile(org.apache.iceberg.Table table, long snapshotId, String path) {
        PartitionStatisticsFile statsFile = ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(snapshotId)
                .path(path)
                .fileSizeInBytes(0L)
                .build();
        table.updatePartitionStatistics().setPartitionStatistics(statsFile).commit();
        table.refresh();
    }

    // PartitionStatsHandler.computeAndWriteStatsFile writes the parquet to disk but does not register
    // the file in TableMetadata. Without a follow-up updatePartitionStatistics().commit() the provider
    // sees an empty partitionStatisticsFiles() list and falls back. Wrap the writer so tests pick up
    // the persisted file the same way production code does.
    private static PartitionStatisticsFile computeAndAttachStatsFile(org.apache.iceberg.Table table, long snapshotId)
            throws Exception {
        PartitionStatisticsFile statsFile = PartitionStatsHandler.computeAndWriteStatsFile(table, snapshotId);
        if (statsFile != null) {
            table.updatePartitionStatistics().setPartitionStatistics(statsFile).commit();
            table.refresh();
        }
        return statsFile;
    }

    @Test
    public void emptyTable_returnsEmpty() {
        // No commits → no current snapshot. snapshotId=-1 must short-circuit without NPE.
        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, -1);
        Assertions.assertTrue(result.isEmpty(), "expected fallback when table has no current snapshot");
    }

    @Test
    public void noStatsFile_returnsEmpty() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, -1);
        Assertions.assertTrue(result.isEmpty(), "expected fallback when no stats file attached");
    }

    @Test
    public void unknownSnapshotId_returnsEmpty() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long currentId = mockedNativeTableB.currentSnapshot().snapshotId();
        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, currentId + 0x7FFFFFFFL);
        Assertions.assertTrue(result.isEmpty(), "expected fallback when snapshotId is not in table metadata");
    }

    @Test
    public void orphanStatsFile_returnsEmpty() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long realId = mockedNativeTableB.currentSnapshot().snapshotId();
        // Stats file referencing a snapshot that is not in the ancestor chain of any real snapshot.
        attachStatsFile(mockedNativeTableB, realId + 0x7FFFFFFFL, "/tmp/orphan-stats.parquet");

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, -1);
        Assertions.assertTrue(result.isEmpty(),
                "stats file off the ancestor chain must not be used; fallback expected");
    }

    @Test
    public void killSwitchOff_returnsEmpty() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        attachStatsFile(mockedNativeTableB, snapshotId, "/tmp/fake-stats.parquet");

        ConnectContext.set(connectContext);
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prev = sv.enableIcebergPartitionStats();
        sv.setEnableIcebergPartitionStats(false);
        try {
            Optional<Map<String, Partition>> result =
                    IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, -1);
            Assertions.assertTrue(result.isEmpty(),
                    "kill switch must disable the fast path regardless of stats file presence");
        } finally {
            sv.setEnableIcebergPartitionStats(prev);
        }
    }

    @Test
    public void corruptedStatsPath_returnsEmpty() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        // Path points to a file that does not exist; reading must fail-soft into fallback.
        attachStatsFile(mockedNativeTableB, snapshotId, "/tmp/does-not-exist-" + snapshotId + ".parquet");

        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, -1);
        Assertions.assertTrue(result.isEmpty(),
                "unreadable stats file must trigger fallback rather than throw");
    }

    @Test
    public void exactMatchStatsFile_returnsMap() throws Exception {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        // Compute and persist a real stats file through apache iceberg's public writer.
        PartitionStatisticsFile statsFile =
                computeAndAttachStatsFile(mockedNativeTableB, snapshotId);
        Assertions.assertNotNull(statsFile, "stats file must be produced for a populated snapshot");
        Assertions.assertEquals(snapshotId, statsFile.snapshotId());

        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, snapshotId);
        Assertions.assertTrue(result.isPresent(), "exact-match stats file must populate fast path");
        Assertions.assertFalse(result.get().isEmpty(), "fast path map must contain at least one partition");
    }

    @Test
    public void incrementalMerge_returnsMap() throws Exception {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long statsSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableB, statsSnapshotId);

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        long targetSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();

        Optional<Map<String, Partition>> result =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, targetSnapshotId);
        Assertions.assertTrue(result.isPresent(),
                "stats file on ancestor + reachable lineage must populate fast path");
        Assertions.assertFalse(result.get().isEmpty());
    }

    @Test
    public void bitEquivalenceVsPartitionsTable_singleSnapshot() throws Exception {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableB, snapshotId);

        Optional<Map<String, Partition>> fast =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, snapshotId);
        Assertions.assertTrue(fast.isPresent());

        // Run the existing PartitionsTable scan path through the disabled-fast-path branch and
        // compare entry-by-entry. modifiedTime and version must be byte-identical so the cached
        // result stays interchangeable between callers.
        Map<String, Partition> reference;
        IcebergTable wrapper = new IcebergTable(1, "srTable", "iceberg_catalog",
                "resource", "db", "tb", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());
        ConnectContext.set(connectContext);
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prev = sv.enableIcebergPartitionStats();
        sv.setEnableIcebergPartitionStats(false);
        try {
            reference = new MockIcebergCatalog().getPartitions(wrapper, snapshotId, null);
        } finally {
            sv.setEnableIcebergPartitionStats(prev);
        }

        assertBitEquivalent(reference, fast.get());
    }

    @Test
    public void bitEquivalenceVsPartitionsTable_bucketTransform() throws Exception {
        // mockedNativeTableA uses SPEC_A = bucket("data", 16); FILE_A_1/FILE_A_2 land in different buckets.
        // Exercises partition-name conversion under a non-identity transform.
        mockedNativeTableA.newAppend().appendFile(FILE_A_1).commit();
        mockedNativeTableA.newAppend().appendFile(FILE_A_2).commit();
        long snapshotId = mockedNativeTableA.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableA, snapshotId);

        Optional<Map<String, Partition>> fast =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableA, snapshotId);
        Assertions.assertTrue(fast.isPresent(), "bucket-partitioned stats file must populate fast path");

        IcebergTable wrapper = new IcebergTable(3, "srTable", "iceberg_catalog",
                "resource", "db", "ta", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<String, Partition> reference = readWithFastPathDisabled(wrapper, snapshotId);
        assertBitEquivalent(reference, fast.get());
    }

    @Test
    public void bitEquivalenceVsPartitionsTable_withDeleteFile() throws Exception {
        // v2 table required for delete files — mockedNativeTableC is SCHEMA_B/SPEC_B at formatVersion=2.
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newRowDelta().addDeletes(FILE_C_1).commit();
        long snapshotId = mockedNativeTableC.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableC, snapshotId);

        Optional<Map<String, Partition>> fast =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableC, snapshotId);
        Assertions.assertTrue(fast.isPresent());

        IcebergTable wrapper = new IcebergTable(4, "srTable", "iceberg_catalog",
                "resource", "db", "tc", "", Lists.newArrayList(), mockedNativeTableC, Maps.newHashMap());
        Map<String, Partition> reference = readWithFastPathDisabled(wrapper, snapshotId);
        assertBitEquivalent(reference, fast.get());
    }

    @Test
    public void bitEquivalenceVsPartitionsTable_incrementalMergeSamePartition() throws Exception {
        // Regression: stats file contains partition k2=3 from FILE_B_2; incremental commit appends
        // ANOTHER file to the SAME partition (FILE_B_3 also in k2=3). When merging delta into base,
        // base map keys are GenericRecord (from stats file) while delta map keys are PartitionData
        // (from manifests); without normalization the merge would create duplicate entries that
        // collapse to only one in convertToPartitionMap, dropping either base or delta counters.
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        long statsSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableB, statsSnapshotId);

        mockedNativeTableB.newAppend().appendFile(FILE_B_3).commit();
        mockedNativeTableB.refresh();
        long targetSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();

        Optional<Map<String, Partition>> fast =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, targetSnapshotId);
        Assertions.assertTrue(fast.isPresent());

        IcebergTable wrapper = new IcebergTable(6, "srTable", "iceberg_catalog",
                "resource", "db", "tb", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());
        Map<String, Partition> reference = readWithFastPathDisabled(wrapper, targetSnapshotId);
        assertBitEquivalent(reference, fast.get());
    }

    @Test
    public void rewriteManifests_doesNotChangeFingerprint() throws Exception {
        // ManifestEntry.snapshotId() is preserved across rewriteManifests, so the seven stat inputs
        // (record/file counts, sizes, delete counts) are unchanged and the fingerprint must stay
        // identical even though manifest files were physically replaced.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        long beforeId = mockedNativeTableB.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableB, beforeId);
        Map<String, Partition> before = IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, beforeId).orElseThrow();

        mockedNativeTableB.rewriteManifests().clusterBy(file -> "all").commit();
        long afterId = mockedNativeTableB.currentSnapshot().snapshotId();
        // Re-compute stats on the rewrite snapshot so the fast path engages with an exact match.
        computeAndAttachStatsFile(mockedNativeTableB, afterId);
        Map<String, Partition> after = IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, afterId).orElseThrow();

        Assertions.assertEquals(before.keySet(), after.keySet(),
                "rewriteManifests must not add or drop partitions");
        for (String name : before.keySet()) {
            Assertions.assertEquals(before.get(name).getVersion(), after.get(name).getVersion(),
                    "rewriteManifests must not change fingerprint for partition " + name);
        }
    }

    @Test
    public void expiredSnapshot_statsPathKeepsModifiedTime() throws Exception {
        // S1: partition k2=2 changes.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
        // S2: partition k2=3 changes; stats file pinned at S2 persists per-partition timestamps for both.
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        long s2 = mockedNativeTableB.currentSnapshot().snapshotId();
        computeAndAttachStatsFile(mockedNativeTableB, s2);
        // Expire S1; FILE_B_1's snapshot lineage is now lost.
        mockedNativeTableB.expireSnapshots().expireSnapshotId(s1).commit();
        mockedNativeTableB.refresh();

        Map<String, Partition> fast =
                IcebergPartitionStatsProvider.tryRead(mockedNativeTableB, s2).orElseThrow();

        IcebergTable wrapper = new IcebergTable(5, "srTable", "iceberg_catalog",
                "resource", "db", "tb", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());
        Map<String, Partition> fallback = readWithFastPathDisabled(wrapper, s2);

        Partition fastK2 = fast.get("k2=2");
        Partition fallbackK2 = fallback.get("k2=2");
        Assertions.assertNotNull(fastK2);
        Assertions.assertNotNull(fallbackK2);
        Assertions.assertTrue(fastK2.getModifiedTime() > 0,
                "stats path must surface persisted last_updated_at after the source snapshot expired");
        Assertions.assertEquals(-1L, fallbackK2.getModifiedTime(),
                "manifest path has no way to resolve last_updated_at when the source snapshot was expired");
    }

    private Map<String, Partition> readWithFastPathDisabled(IcebergTable wrapper, long snapshotId) {
        ConnectContext.set(connectContext);
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prev = sv.enableIcebergPartitionStats();
        sv.setEnableIcebergPartitionStats(false);
        try {
            return new MockIcebergCatalog().getPartitions(wrapper, snapshotId, null);
        } finally {
            sv.setEnableIcebergPartitionStats(prev);
        }
    }

    private static void assertBitEquivalent(Map<String, Partition> reference, Map<String, Partition> fast) {
        Assertions.assertEquals(reference.size(), fast.size(),
                "stats path and PartitionsTable path must agree on partition count");
        for (Map.Entry<String, Partition> entry : reference.entrySet()) {
            Partition fastEntry = fast.get(entry.getKey());
            Assertions.assertNotNull(fastEntry,
                    "fast path must contain the same partition keys as PartitionsTable path: " + entry.getKey());
            Assertions.assertEquals(entry.getValue().getModifiedTime(), fastEntry.getModifiedTime(),
                    "modifiedTime must be bit-equivalent for partition " + entry.getKey());
            Assertions.assertEquals(entry.getValue().getVersion(), fastEntry.getVersion(),
                    "version must be bit-equivalent for partition " + entry.getKey());
        }
    }
}
