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

package org.apache.iceberg;

import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public final class PartitionStatsScanHelper {
    private static final Logger LOG = LogManager.getLogger(PartitionStatsScanHelper.class);

    private PartitionStatsScanHelper() {}

    public static PartitionMap<PartitionStats> computeStatsFromManifests(
            Table table, List<ManifestFile> manifests, Types.StructType partitionType, boolean incremental) {
        long startMs = System.currentTimeMillis();
        ScanMetrics metrics = new ScanMetrics();
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        for (ManifestFile manifest : manifests) {
            mergePartitionMap(collectStatsForManifest(table, manifest, partitionType, incremental, metrics), statsMap);
        }
        LOG.debug("Iceberg partitions stats scan manifests. manifests={}, entries={}, live_entries={}, added_entries={}, "
                        + "deleted_entries={}, incremental={}, elapsed_ms={}",
                manifests.size(), metrics.entries, metrics.liveEntries, metrics.addedEntries,
                metrics.deletedEntries, incremental, System.currentTimeMillis() - startMs);
        return statsMap;
    }

    private static PartitionMap<PartitionStats> collectStatsForManifest(
            Table table, ManifestFile manifest, Types.StructType partitionType, boolean incremental, ScanMetrics metrics) {
        List<String> projection = BaseScan.scanColumns(manifest.content());
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
            PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
            int specId = manifest.partitionSpecId();
            PartitionSpec spec = table.specs().get(specId);
            PartitionData keyTemplate = new PartitionData(partitionType);

            for (ManifestEntry<?> entry : reader.entries()) {
                metrics.entries++;
                ContentFile<?> file = entry.file();
                StructLike coercedPartition =
                        PartitionUtil.coercePartition(partitionType, spec, file.partition());
                StructLike key = keyTemplate.copyFor(coercedPartition);
                Snapshot snapshot = table.snapshot(entry.snapshotId());
                PartitionStats stats =
                        statsMap.computeIfAbsent(
                                specId,
                                ((PartitionData) file.partition()).copy(),
                                () -> new PartitionStats(key, specId));
                if (snapshot == null) {
                    // Preserve snapshotId for change detection even if the snapshot is expired.
                    Long currentSnapshotId = stats.lastUpdatedSnapshotId();
                    long entrySnapshotId = entry.snapshotId();
                    if (currentSnapshotId == null || currentSnapshotId < entrySnapshotId) {
                        stats.set(11, entrySnapshotId);
                    }
                }
                if (entry.isLive()) {
                    metrics.liveEntries++;
                    if (!incremental || entry.status() == ManifestEntry.Status.ADDED) {
                        metrics.addedEntries++;
                        stats.liveEntry(file, snapshot);
                    }
                } else {
                    metrics.deletedEntries++;
                    if (incremental) {
                        stats.deletedEntryForIncrementalCompute(file, snapshot);
                    } else {
                        stats.deletedEntry(snapshot);
                    }
                }
            }

            return statsMap;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class ScanMetrics {
        long entries;
        long liveEntries;
        long addedEntries;
        long deletedEntries;
    }

    private static void mergePartitionMap(
            PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap) {
        fromMap.forEach(
                (key, value) ->
                        toMap.merge(
                                key,
                                value,
                                (existingEntry, newEntry) -> {
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));
    }
}
