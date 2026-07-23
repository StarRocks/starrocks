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

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

// Lives in org.apache.iceberg to access ManifestReader.entries() and BaseScan.scanColumns(),
// which are package-private in iceberg 1.10. Lives in the shared hadoop-ext module so both the
// FE provider (IcebergPartitionStatsProvider) and the BE scanner
// (com.starrocks.connector.iceberg.IcebergPartitionsTableScanner) read stats and merge
// incremental manifests through one implementation. Apache iceberg's own equivalent
// (PartitionStatsHandler.computeAndMergeStatsIncremental + partitionDataToRecord) is
// private/package-private in 1.10, so we mirror it here.
public final class IcebergPartitionStatsHelper {
    private IcebergPartitionStatsHelper() {
    }

    // Loads a PartitionStatisticsFile into a PartitionMap keyed by (specId, GenericRecord
    // partition). Wraps apache PartitionStatsHandler.readPartitionStatsFile (which only returns
    // a CloseableIterable) and resolves the stats schema for the table's current format version.
    public static PartitionMap<PartitionStats> readPartitionStatsFileAsMap(
            Table table, String statsFilePath, Types.StructType partitionType) {
        Schema schema = PartitionStatsHandler.schema(partitionType, TableUtil.formatVersion(table));
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        try (CloseableIterable<PartitionStats> iter =
                     PartitionStatsHandler.readPartitionStatsFile(schema, table.io().newInputFile(statsFilePath))) {
            for (PartitionStats stats : iter) {
                statsMap.put(stats.specId(), stats.partition(), stats);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return statsMap;
    }

    public static PartitionMap<PartitionStats> computeStatsFromManifests(
            Table table, List<ManifestFile> manifests, Types.StructType partitionType) {
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        for (ManifestFile manifest : manifests) {
            mergePartitionMap(collectStatsForManifest(table, manifest, partitionType), statsMap);
        }
        return statsMap;
    }

    // Merges an incremental stats map (delta) into base. Base map was populated from a stats file
    // through PartitionStatsHandler.readPartitionStatsFile and so its partition keys are
    // GenericRecord; delta map was built from manifest entries and its keys are PartitionData.
    // GenericRecord.equals(PartitionData) is false, so a raw merge would create duplicate entries
    // for the same logical partition and downstream aggregation would drop one set of counters.
    // Normalize the delta key to GenericRecord before merging.
    public static void mergeIncrementalStats(
            PartitionMap<PartitionStats> base, PartitionMap<PartitionStats> delta) {
        delta.forEach((key, value) -> base.merge(
                Pair.of(key.first(), partitionDataToRecord((PartitionData) key.second())),
                value,
                (existing, fresh) -> {
                    existing.appendStats(fresh);
                    return existing;
                }));
    }

    public static GenericRecord partitionDataToRecord(PartitionData data) {
        GenericRecord record = GenericRecord.create(data.getPartitionType());
        for (int index = 0; index < record.size(); index++) {
            record.set(index, data.get(index));
        }
        return record;
    }

    private static PartitionMap<PartitionStats> collectStatsForManifest(
            Table table, ManifestFile manifest, Types.StructType partitionType) {
        List<String> projection = BaseScan.scanColumns(manifest.content());
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
            PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
            int specId = manifest.partitionSpecId();
            PartitionSpec spec = table.specs().get(specId);
            PartitionData keyTemplate = new PartitionData(partitionType);

            for (ManifestEntry<?> entry : reader.entries()) {
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
                if (entry.isLive()) {
                    if (entry.status() == ManifestEntry.Status.ADDED) {
                        stats.liveEntry(file, snapshot);
                    }
                } else {
                    stats.deletedEntryForIncrementalCompute(file, snapshot);
                }
            }

            return statsMap;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
