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

import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.share.iceberg.IcebergPartitionStatsLookup;
import com.starrocks.connector.share.iceberg.PartitionStatsPlan;
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.IcebergPartitionStatsHelper;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


// Fast path for IcebergCatalog.getPartitions: when a PartitionStatisticsFile is reachable from the
// target snapshot's ancestor chain, build Partition map from PartitionStats (read directly when
// statsFile.snapshotId() == target, or via in-memory incremental merge of manifests added between
// the stats snapshot and target).
//
// Returns Optional.empty() to signal the caller to fall back to the existing PartitionsTable scan.
// Bit-equivalence: for the same target snapshot, both paths produce identical Partition
// (modifiedTime, version, specId) tuples, so cached results from either path remain interchangeable
// without explicit invalidation when the kill switch toggles.
public final class IcebergPartitionStatsProvider {
    private IcebergPartitionStatsProvider() {
    }

    public static Optional<Map<String, Partition>> tryRead(Table nativeTable, long snapshotId) {
        // Iceberg's PartitionStatsHandler refuses to write stats for unpartitioned tables
        // ("Table must be partitioned", PartitionStatsHandler.java:207), and no engine in the
        // ecosystem writes them elsewhere. Even when a historical partitioned spec leaves a
        // stale stats file behind, the unified partition type would let us read it but the
        // resulting numbers cannot represent the now-unpartitioned table without aggregation.
        // Bail out so the caller's PartitionsTable scan handles unpartitioned uniformly.
        if (nativeTable.spec().isUnpartitioned()) {
            return Optional.empty();
        }
        boolean enabled = ConnectContext.getSessionVariableOrDefault().enableIcebergPartitionStats();
        PartitionStatsPlan plan = IcebergPartitionStatsLookup.plan(nativeTable, snapshotId, enabled);
        if (plan.mode() == PartitionStatsPlan.Mode.NONE) {
            return Optional.empty();
        }
        Types.StructType partitionType = Partitioning.partitionType(nativeTable);
        try {
            PartitionMap<PartitionStats> statsMap = IcebergPartitionStatsHelper.readPartitionStatsFileAsMap(
                    nativeTable, plan.statsFile().path(), partitionType);
            if (plan.mode() == PartitionStatsPlan.Mode.INCREMENTAL) {
                PartitionMap<PartitionStats> delta = IcebergPartitionStatsHelper.computeStatsFromManifests(
                        nativeTable, plan.incrementalManifests(), partitionType);
                IcebergPartitionStatsHelper.mergeIncrementalStats(statsMap, delta);
            }
            return convertToPartitionMap(nativeTable, statsMap);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<Map<String, Partition>> convertToPartitionMap(
            Table nativeTable, PartitionMap<PartitionStats> statsMap) {
        Map<String, Partition> result = new HashMap<>(statsMap.size());

        for (PartitionStats stats : statsMap.values()) {
            PartitionSpec spec = nativeTable.specs().get(stats.specId());
            if (spec == null) {
                // Missing spec means stats reference a spec that is no longer in the table metadata;
                // fail the whole fast path so the existing PartitionsTable scan handles the table
                // consistently (partial fast-path results would be silently incorrect).
                return Optional.empty();
            }

            // PartitionStats.lastUpdatedAt() is epoch millis, but Partition.getModifiedTimeUnit() is
            // MICROSECONDS and the PartitionsTable scan path surfaces last_updated_at in micros. Convert
            // so both paths stay bit-equivalent.
            long modifiedTime =
                    stats.lastUpdatedAt() != null ? TimeUnit.MILLISECONDS.toMicros(stats.lastUpdatedAt()) : -1L;
            long version = fingerprint(
                    stats.dataRecordCount(),
                    stats.dataFileCount(),
                    stats.totalDataFileSizeInBytes(),
                    stats.positionDeleteRecordCount(),
                    stats.positionDeleteFileCount(),
                    stats.equalityDeleteRecordCount(),
                    stats.equalityDeleteFileCount());

            String partitionName = PartitionUtil.convertIcebergPartitionToPartitionName(
                    nativeTable, spec, stats.partition());
            result.put(partitionName, new Partition(modifiedTime, version, stats.specId()));
        }
        return Optional.of(result);
    }

    // Shared fingerprint formula. Must stay bit-equivalent with IcebergCatalog.getPartitionVersion
    // so the PartitionsTable scan path and the stats path produce identical Partition.version for
    // the same snapshot. Inputs are exactly the 7 numbers exposed in both PartitionsTable rows and
    // org.apache.iceberg.PartitionStats; dvCount / totalRecordCount intentionally excluded for
    // backward compat with persisted MV BasePartitionInfo.version.
    public static long fingerprint(long recordCount, long fileCount, long totalSize,
                                   long posDeleteRecordCount, long posDeleteFileCount,
                                   long eqDeleteRecordCount, long eqDeleteFileCount) {
        return (long) Objects.hash(recordCount, fileCount, totalSize,
                posDeleteRecordCount, posDeleteFileCount,
                eqDeleteRecordCount, eqDeleteFileCount) & 0x7FFFFFFFL;
    }
}
