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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// Shared lookup helpers for iceberg partition statistics files used by both the
// $iceberg_partitions_table BE scanner routing (IcebergMetadata.buildPartitionStatsMetaSpec)
// and the FE-side IcebergPartitionStatsProvider used in IcebergCatalog.getPartitions.
public final class IcebergPartitionStatsLookup {
    private IcebergPartitionStatsLookup() {
    }

    // Plans how to satisfy a "partitions at snapshot X" request using a partition statistics file
    // when one is reachable. Shared between BE routing (which turns the plan into a split bean)
    // and the FE provider (which executes the plan in-process). Reasons attached to NONE plans
    // are surfaced via PartitionStatsPlan.reason() — callers wanting diagnostics inspect them; the
    // FE provider stays silent by design.
    public static PartitionStatsPlan plan(Table table, long snapshotId, boolean enabled) {
        if (!enabled) {
            return PartitionStatsPlan.none(PartitionStatsPlan.REASON_DISABLED_BY_SESSION);
        }
        long target = snapshotId;
        if (target == -1) {
            Snapshot current = table.currentSnapshot();
            if (current == null) {
                return PartitionStatsPlan.none(PartitionStatsPlan.REASON_NO_CURRENT_SNAPSHOT);
            }
            target = current.snapshotId();
        }
        if (table.snapshot(target) == null) {
            return PartitionStatsPlan.none(PartitionStatsPlan.REASON_MISSING_TARGET_SNAPSHOT);
        }
        PartitionStatisticsFile statsFile = latestStatsFile(table, target);
        if (statsFile == null) {
            return PartitionStatsPlan.none(PartitionStatsPlan.REASON_NO_STATS_FILE);
        }
        if (statsFile.snapshotId() == target) {
            return PartitionStatsPlan.base(statsFile, target);
        }
        List<ManifestFile> incremental = collectIncrementalManifests(table, statsFile.snapshotId(), target);
        if (incremental == null) {
            return PartitionStatsPlan.none(PartitionStatsPlan.REASON_MISSING_SNAPSHOT_LINEAGE);
        }
        return PartitionStatsPlan.incremental(statsFile, target, incremental);
    }

    // Iceberg's PartitionStatsHandler.latestStatsFile(table) only returns the file for the current
    // snapshot; we need the latest file reachable from a specific snapshot in the ancestor chain
    // (e.g. when answering $iceberg_partitions_table at a time-travel snapshot, or when MV refresh
    // reads partitions at a pinned snapshot).
    public static PartitionStatisticsFile latestStatsFile(Table table, long snapshotId) {
        List<PartitionStatisticsFile> files = table.partitionStatisticsFiles();
        if (files.isEmpty()) {
            return null;
        }
        Map<Long, PartitionStatisticsFile> byId =
                files.stream().collect(Collectors.toMap(PartitionStatisticsFile::snapshotId, f -> f));
        for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshotId, table::snapshot)) {
            PartitionStatisticsFile match = byId.get(ancestor.snapshotId());
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    // Returns manifests added strictly between statsSnapshotId (exclusive) and targetSnapshotId
    // (inclusive), or null if the lineage cannot be reconstructed (stats snapshot missing,
    // ancestor chain empty). Caller treats null as "fallback to manifest scan".
    public static List<ManifestFile> collectIncrementalManifests(
            Table table, long statsSnapshotId, long targetSnapshotId) {
        Snapshot baseSnapshot = table.snapshot(statsSnapshotId);
        if (baseSnapshot == null) {
            return null;
        }

        List<ManifestFile> incrementalManifests = new ArrayList<>();
        Set<String> seenPaths = new HashSet<>();
        Iterable<Snapshot> snapshots =
                SnapshotUtil.ancestorsBetween(targetSnapshotId, statsSnapshotId, table::snapshot);
        boolean sawSnapshot = false;
        for (Snapshot snapshot : snapshots) {
            if (snapshot == null) {
                continue;
            }
            sawSnapshot = true;
            for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
                if (manifest.snapshotId() == snapshot.snapshotId() && seenPaths.add(manifest.path())) {
                    incrementalManifests.add(manifest);
                }
            }
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                if (manifest.snapshotId() == snapshot.snapshotId() && seenPaths.add(manifest.path())) {
                    incrementalManifests.add(manifest);
                }
            }
        }
        if (!sawSnapshot) {
            return null;
        }
        return incrementalManifests;
    }
}
