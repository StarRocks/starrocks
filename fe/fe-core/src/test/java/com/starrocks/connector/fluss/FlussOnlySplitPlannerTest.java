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

package com.starrocks.connector.fluss;

import com.google.common.collect.Lists;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlussOnlySplitPlannerTest {
    private static final TablePath PATH = TablePath.of("db1", "orders");
    private static final long TABLE_ID = 42L;
    private static final TableInfo TABLE_INFO = TableInfo.of(PATH, TABLE_ID, 1, TableDescriptor.builder()
            .schema(Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.STRING()).build())
            .distributedBy(3, "id").build(), null, 1000L, 2000L);
    private static final PartitionInfo P1 = partition(1001L, "2026-07-01");
    private static final PartitionInfo P2 = partition(1002L, "2026-07-02");

    @Mocked
    private Admin admin;

    @Mocked
    private KvSnapshotLease kvSnapshotLease;

    @Test
    public void testPrimaryKeyTableReadsTheLatestKvSnapshotPlusTheLogAfterIt() throws Exception {
        List<Map<TableBucket, Long>> leased = mockLease(Map.of());
        new Expectations() {
            {
                admin.getLatestKvSnapshots(PATH);
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, null,
                        mapOf(5L, null, 6L), mapOf(10L, null, 7L)));
                admin.listOffsets(PATH, (Collection<Integer>) any, withInstanceOf(OffsetSpec.LatestSpec.class));
                result = offsets(12L, 3L, 7L);
            }
        };

        List<SourceSplitBase> splits = planner(null).planPrimaryKeyTable(newLease());

        // Bucket 1 has no KV snapshot yet, so its whole changelog is read and nothing is leased for it.
        Assertions.assertEquals(List.of(Map.of(bucket(null, 0), 5L, bucket(null, 2), 6L)), leased);
        Assertions.assertEquals(3, splits.size());
        assertSnapshotLogSplit(splits.get(0), null, 0, 5L, 10L, 12L);
        assertSnapshotLogSplit(splits.get(1), null, 1, -1L, LogScanner.EARLIEST_OFFSET, 3L);
        assertSnapshotLogSplit(splits.get(2), null, 2, 6L, 7L, 7L);
    }

    @Test
    public void testPrimaryKeyTableReadsEachSelectedPartition() throws Exception {
        List<Map<TableBucket, Long>> leased = mockLease(Map.of());
        new Expectations() {
            {
                admin.getLatestKvSnapshots(PATH, P1.getPartitionName());
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, 1001L,
                        mapOf(1L, null, null), mapOf(2L, null, null)));
                admin.getLatestKvSnapshots(PATH, P2.getPartitionName());
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, 1002L,
                        mapOf(null, null, null), mapOf(null, null, null)));
                admin.listOffsets(PATH, anyString, (Collection<Integer>) any,
                        withInstanceOf(OffsetSpec.LatestSpec.class));
                result = offsets(2L, 0L, 1L);
            }
        };

        List<SourceSplitBase> splits = planner(List.of(P1, P2)).planPrimaryKeyTable(newLease());

        Assertions.assertEquals(List.of(Map.of(bucket(1001L, 0), 1L)), leased);
        Assertions.assertEquals(6, splits.size());
        assertSnapshotLogSplit(splits.get(0), 1001L, 0, 1L, 2L, 2L);
        assertSnapshotLogSplit(splits.get(5), 1002L, 2, -1L, LogScanner.EARLIEST_OFFSET, 1L);
        Assertions.assertEquals(P2.getPartitionName(), splits.get(5).getPartitionName());
    }

    @Test
    public void testPrimaryKeyTableFailsWhenASnapshotIsCleanedUpBeforeItIsLeased() {
        mockLease(Map.of(bucket(null, 2), 6L));
        new Expectations() {
            {
                admin.getLatestKvSnapshots(PATH);
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, null,
                        mapOf(5L, null, 6L), mapOf(10L, null, 7L)));
            }
        };

        StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> planner(null).planPrimaryKeyTable(newLease()));
        Assertions.assertTrue(error.getMessage().contains("Retry the query"), error.getMessage());
        new Verifications() {
            {
                admin.listOffsets(PATH, (Collection<Integer>) any, (OffsetSpec) any);
                times = 0;
            }
        };
    }

    @Test
    public void testPrimaryKeyTableFailsWhenFlussReturnsNoLatestOffset() {
        mockLease(Map.of());
        new Expectations() {
            {
                admin.getLatestKvSnapshots(PATH);
                result = CompletableFuture.completedFuture(new KvSnapshots(TABLE_ID, null,
                        mapOf(5L, null, 6L), mapOf(10L, null, 7L)));
                admin.listOffsets(PATH, (Collection<Integer>) any, withInstanceOf(OffsetSpec.LatestSpec.class));
                result = new ListOffsetsResult(Map.of(0, CompletableFuture.completedFuture(12L),
                        2, CompletableFuture.completedFuture(7L)));
            }
        };

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> planner(null).planPrimaryKeyTable(newLease()));
        Assertions.assertEquals("Fluss returned no latest offset for " + bucket(null, 1), error.getMessage());
    }

    @Test
    public void testLogTableStartsAtTheEarliestRetainedOffset() throws Exception {
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(PATH);
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
                // Bucket 0 has expired up to offset 3, and all of bucket 2 has expired, with no writes since.
                admin.listOffsets(PATH, (Collection<Integer>) any, withInstanceOf(OffsetSpec.EarliestSpec.class));
                result = offsets(3L, 0L, 7L);
                admin.listOffsets(PATH, (Collection<Integer>) any, withInstanceOf(OffsetSpec.LatestSpec.class));
                result = offsets(5L, 4L, 7L);
            }
        };

        List<SourceSplitBase> splits = planner(null).planLogTable();

        // A split that starts before the retained log would wait forever for records that no longer exist.
        Assertions.assertEquals(2, splits.size());
        assertLogSplit(splits.get(0), null, 0, 3L, 5L);
        assertLogSplit(splits.get(1), null, 1, 0L, 4L);
    }

    @Test
    public void testLogTableStartsAtTheLakeSnapshotOffsets() throws Exception {
        Map<TableBucket, Long> lakeOffsets = Map.of(bucket(null, 0), 3L, bucket(null, 1), 4L);
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(PATH);
                result = CompletableFuture.completedFuture(new LakeSnapshot(9L, lakeOffsets));
                admin.listOffsets(PATH, List.of(2), withInstanceOf(OffsetSpec.EarliestSpec.class));
                result = new ListOffsetsResult(Map.of(2, CompletableFuture.completedFuture(1L)));
                admin.listOffsets(PATH, (Collection<Integer>) any, withInstanceOf(OffsetSpec.LatestSpec.class));
                result = offsets(5L, 4L, 7L);
            }
        };

        List<SourceSplitBase> splits = planner(null).planLogTable();

        // Bucket 1 is fully tiered; only bucket 2, which has no lake offset, needs its earliest offset.
        Assertions.assertEquals(2, splits.size());
        assertLogSplit(splits.get(0), null, 0, 3L, 5L);
        assertLogSplit(splits.get(1), null, 2, 1L, 7L);
    }

    @Test
    public void testLogTableReadsEachSelectedPartition() throws Exception {
        new Expectations() {
            {
                admin.getReadableLakeSnapshot(PATH);
                result = CompletableFuture.failedFuture(new LakeTableSnapshotNotExistException("no snapshot"));
                admin.listOffsets(PATH, P1.getPartitionName(), (Collection<Integer>) any,
                        withInstanceOf(OffsetSpec.EarliestSpec.class));
                result = offsets(0L, 0L, 0L);
                admin.listOffsets(PATH, P1.getPartitionName(), (Collection<Integer>) any,
                        withInstanceOf(OffsetSpec.LatestSpec.class));
                result = offsets(2L, 0L, 1L);
            }
        };

        List<SourceSplitBase> splits = planner(List.of(P1)).planLogTable();

        Assertions.assertEquals(2, splits.size());
        assertLogSplit(splits.get(0), 1001L, 0, 0L, 2L);
        assertLogSplit(splits.get(1), 1001L, 2, 0L, 1L);
        Assertions.assertEquals(P1.getPartitionName(), splits.get(0).getPartitionName());
    }

    private FlussOnlySplitPlanner planner(List<PartitionInfo> partitions) {
        return new FlussOnlySplitPlanner(admin, PATH, TABLE_INFO, partitions);
    }

    private FlussKvSnapshotLease newLease() {
        return new FlussKvSnapshotLease(admin, "starrocks-q1", 60_000L);
    }

    private List<Map<TableBucket, Long>> mockLease(Map<TableBucket, Long> unavailable) {
        List<Map<TableBucket, Long>> leased = new ArrayList<>();
        new Expectations() {
            {
                admin.createKvSnapshotLease(anyString, anyLong);
                result = kvSnapshotLease;
                minTimes = 0;
                kvSnapshotLease.acquireSnapshots(withCapture(leased));
                result = CompletableFuture.completedFuture(new AcquireKvSnapshotLeaseResult(unavailable));
                minTimes = 0;
            }
        };
        return leased;
    }

    private static ListOffsetsResult offsets(Long... offsets) {
        Map<Integer, CompletableFuture<Long>> futures = new HashMap<>();
        for (int bucket = 0; bucket < offsets.length; bucket++) {
            futures.put(bucket, CompletableFuture.completedFuture(offsets[bucket]));
        }
        return new ListOffsetsResult(futures);
    }

    private static Map<Integer, Long> mapOf(Long bucket0, Long bucket1, Long bucket2) {
        Map<Integer, Long> map = new HashMap<>();
        map.put(0, bucket0);
        map.put(1, bucket1);
        map.put(2, bucket2);
        return map;
    }

    private static TableBucket bucket(Long partitionId, int bucket) {
        return new TableBucket(TABLE_ID, partitionId, bucket);
    }

    private static PartitionInfo partition(long partitionId, String dt) {
        return new PartitionInfo(partitionId,
                new ResolvedPartitionSpec(Lists.newArrayList("dt"), Lists.newArrayList(dt)), null, 3);
    }

    private static void assertSnapshotLogSplit(SourceSplitBase split, Long partitionId, int bucket, long snapshotId,
                                               long logStartingOffset, long logStoppingOffset) {
        HybridSnapshotLogSplit snapshotLogSplit = split.asHybridSnapshotLogSplit();
        Assertions.assertEquals(bucket(partitionId, bucket), snapshotLogSplit.getTableBucket());
        Assertions.assertEquals(snapshotId, snapshotLogSplit.getSnapshotId());
        Assertions.assertEquals(logStartingOffset, snapshotLogSplit.getLogStartingOffset());
        Assertions.assertEquals(logStoppingOffset, snapshotLogSplit.getLogStoppingOffset().orElse(-1L));
        Assertions.assertTrue(snapshotLogSplit.isBatch());
    }

    private static void assertLogSplit(SourceSplitBase split, Long partitionId, int bucket,
                                       long startingOffset, long stoppingOffset) {
        LogSplit logSplit = split.asLogSplit();
        Assertions.assertEquals(bucket(partitionId, bucket), logSplit.getTableBucket());
        Assertions.assertEquals(startingOffset, logSplit.getStartingOffset());
        Assertions.assertEquals(stoppingOffset, logSplit.getStoppingOffset().orElse(-1L));
    }
}
