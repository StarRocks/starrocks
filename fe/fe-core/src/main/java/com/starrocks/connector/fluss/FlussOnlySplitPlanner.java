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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Plans a bounded read of a Fluss table from Fluss alone, without the lake, as Fluss's own
 * FlussOnlyBatchSplitGenerator does. A primary-key table is read as the latest KV snapshot of each
 * bucket merged with the changelog after it. A log table is read from its offset in the readable
 * lake snapshot, or from its earliest retained offset, up to the latest offset.
 */
final class FlussOnlySplitPlanner {
    private final Admin admin;
    private final TablePath tablePath;
    private final TableInfo tableInfo;
    // Real earliest offsets: a split that starts before an expired log never ends.
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    // The selected partitions, or null for an unpartitioned table.
    private final Collection<PartitionInfo> partitions;

    FlussOnlySplitPlanner(Admin admin, TablePath tablePath, TableInfo tableInfo, Collection<PartitionInfo> partitions) {
        this.admin = admin;
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.bucketOffsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath, true);
        this.partitions = partitions;
    }

    /** One split per bucket of a primary-key table; the KV snapshots it reads are leased with {@code lease}. */
    List<SourceSplitBase> planPrimaryKeyTable(FlussKvSnapshotLease lease) throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (partitions == null) {
            addSnapshotLogSplits(splits, admin.getLatestKvSnapshots(tablePath).get(), null, lease);
        } else {
            for (PartitionInfo partition : partitions) {
                String partitionName = partition.getPartitionName();
                addSnapshotLogSplits(splits, admin.getLatestKvSnapshots(tablePath, partitionName).get(),
                        partitionName, lease);
            }
        }
        return splits;
    }

    /** Bounded log splits for a $rt read of a log table. */
    List<SourceSplitBase> planLogTable() throws Exception {
        Map<TableBucket, Long> lakeSnapshotOffsets = readableLakeSnapshotOffsets();
        List<SourceSplitBase> splits = new ArrayList<>();
        if (partitions == null) {
            addLogSplits(splits, null, null, tableInfo.getNumBuckets(), lakeSnapshotOffsets);
        } else {
            for (PartitionInfo partition : partitions) {
                addLogSplits(splits, partition.getPartitionId(), partition.getPartitionName(),
                        partition.getBucketCount(), lakeSnapshotOffsets);
            }
        }
        return splits;
    }

    // As Fluss's Flink source does: list the latest snapshots, lease them, then read the latest offsets after them.
    private void addSnapshotLogSplits(List<SourceSplitBase> splits, KvSnapshots snapshots, String partitionName,
                                      FlussKvSnapshotLease lease) throws Exception {
        Set<TableBucket> unavailable = lease.acquire(snapshotIds(snapshots));
        if (!unavailable.isEmpty()) {
            throw new StarRocksConnectorException("Fluss KV snapshots of table %s were cleaned up before " +
                    "they could be leased (buckets %s). Retry the query.", tablePath, unavailable);
        }
        List<Integer> buckets = new ArrayList<>(snapshots.getBucketIds());
        Collections.sort(buckets);
        Map<Integer, Long> stoppingOffsets = bucketOffsetsRetriever.latestOffsets(partitionName, buckets);
        for (int bucket : buckets) {
            TableBucket tableBucket = new TableBucket(snapshots.getTableId(), snapshots.getPartitionId(), bucket);
            OptionalLong snapshotId = snapshots.getSnapshotId(bucket);
            long logStartingOffset = LogScanner.EARLIEST_OFFSET;
            if (snapshotId.isPresent()) {
                OptionalLong logOffset = snapshots.getLogOffset(bucket);
                checkState(logOffset.isPresent(), "Fluss KV snapshot of %s has no log offset", tableBucket);
                logStartingOffset = logOffset.getAsLong();
            }
            splits.add(new HybridSnapshotLogSplit(tableBucket, partitionName,
                    snapshotId.orElse(TableBucketSnapshot.NO_SNAPSHOT_ID), 0, false,
                    logStartingOffset, offset(stoppingOffsets, tableBucket, "latest"), true));
        }
    }

    private static Map<TableBucket, Long> snapshotIds(KvSnapshots snapshots) {
        Map<TableBucket, Long> snapshotIds = new HashMap<>();
        for (int bucket : snapshots.getBucketIds()) {
            snapshots.getSnapshotId(bucket).ifPresent(snapshotId -> snapshotIds.put(
                    new TableBucket(snapshots.getTableId(), snapshots.getPartitionId(), bucket), snapshotId));
        }
        return snapshotIds;
    }

    private Map<TableBucket, Long> readableLakeSnapshotOffsets() throws Exception {
        try {
            return admin.getReadableLakeSnapshot(tablePath).get().getTableBucketsOffset();
        } catch (Exception e) {
            if (ExceptionUtils.stripExecutionException(e) instanceof LakeTableSnapshotNotExistException) {
                return Collections.emptyMap();
            }
            throw e;
        }
    }

    private void addLogSplits(List<SourceSplitBase> splits, Long partitionId, String partitionName, int numBuckets,
                              Map<TableBucket, Long> lakeSnapshotOffsets) {
        List<Integer> buckets = IntStream.range(0, numBuckets).boxed().collect(Collectors.toList());
        List<Integer> bucketsWithoutLakeOffset = buckets.stream()
                .filter(bucket -> !lakeSnapshotOffsets.containsKey(
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket)))
                .collect(Collectors.toList());
        Map<Integer, Long> earliestOffsets = bucketsWithoutLakeOffset.isEmpty()
                ? Map.of() : bucketOffsetsRetriever.earliestOffsets(partitionName, bucketsWithoutLakeOffset);
        Map<Integer, Long> stoppingOffsets = bucketOffsetsRetriever.latestOffsets(partitionName, buckets);
        for (int bucket : buckets) {
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucket);
            Long lakeOffset = lakeSnapshotOffsets.get(tableBucket);
            long startingOffset = lakeOffset != null ? lakeOffset : offset(earliestOffsets, tableBucket, "earliest");
            long stoppingOffset = offset(stoppingOffsets, tableBucket, "latest");
            // Skip buckets with nothing to read, as LakeSplitGenerator does for the log part of a union read.
            if (startingOffset < stoppingOffset) {
                splits.add(new LogSplit(tableBucket, partitionName, startingOffset, stoppingOffset));
            }
        }
    }

    private static long offset(Map<Integer, Long> offsets, TableBucket tableBucket, String kind) {
        Long offset = offsets.get(tableBucket.getBucket());
        checkState(offset != null, "Fluss returned no %s offset for %s", kind, tableBucket);
        return offset;
    }
}
