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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

public class RewriteManifestsProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "rewrite_manifests";
    private static final int MAX_MANIFEST_CLUSTERS = 100;
    private static final int NULL_PARTITION_CLUSTER = -1;

    private static final RewriteManifestsProcedure INSTANCE = new RewriteManifestsProcedure();

    public static RewriteManifestsProcedure getInstance() {
        return INSTANCE;
    }

    private RewriteManifestsProcedure() {
        super(
                PROCEDURE_NAME,
                Collections.emptyList(),
                IcebergTableOperation.REWRITE_MANIFESTS
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (!args.isEmpty()) {
            throw new StarRocksConnectorException(
                    "invalid args. rewrite_manifests operation does not support any arguments");
        }

        Table icebergTable = context.table();
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            return null;
        }

        long manifestCount = 0;
        long totalManifestsSize = 0;
        try (CloseableIterable<ManifestFile> manifests =
                IcebergUtil.readManifests(currentSnapshot, icebergTable.io())) {
            for (ManifestFile manifest : manifests) {
                if (manifest.content() == ManifestContent.DATA) {
                    manifestCount++;
                    totalManifestsSize += manifest.length();
                }
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Unable to read manifests for snapshot " + currentSnapshot.snapshotId(), e);
        }
        if (manifestCount == 0) {
            return null;
        }

        long manifestTargetSizeBytes = PropertyUtil.propertyAsLong(
                icebergTable.properties(), MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        if (manifestTargetSizeBytes <= 0) {
            manifestTargetSizeBytes = MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
        }

        if (manifestCount == 1 && totalManifestsSize < manifestTargetSizeBytes) {
            return null;
        }

        // Having too many open manifest writers can potentially cause OOM on the coordinator
        long targetManifestClusters = Math.min(
                ((totalManifestsSize + manifestTargetSizeBytes - 1) / manifestTargetSizeBytes),
                MAX_MANIFEST_CLUSTERS);

        Types.StructType partitionType = icebergTable.spec().partitionType();
        Map<Integer, PartitionSpec> specsById = icebergTable.specs();

        // Assign each distinct top-level partition value an order-preserving bucket so that
        // sorted-adjacent partitions land in the same manifest. This keeps each manifest's
        // partition min/max range narrow (effective manifest pruning at read time) while bounding
        // the number of concurrently open writers.
        NavigableMap<Object, Integer> partitionValueToCluster;
        if (icebergTable.spec().isPartitioned() && targetManifestClusters > 1) {
            partitionValueToCluster = buildClusteredPartitionValues(
                    icebergTable, currentSnapshot, (int) targetManifestClusters);
        } else {
            partitionValueToCluster = Collections.emptyNavigableMap();
        }

        RewriteManifests rewriteManifests = context.transaction().rewriteManifests();

        rewriteManifests
                .clusterBy(file -> {
                    if (partitionValueToCluster.isEmpty()) {
                        return 0;
                    }
                    PartitionSpec fileSpec = specsById.get(file.specId());
                    if (fileSpec == null) {
                        // Unknown spec: the partition spec evolved under a concurrent commit. Evolution
                        // is rare, so abort and let the next maintenance run re-cluster against the new
                        // spec rather than guessing an order for it.
                        throw new StarRocksConnectorException(
                                "rewrite_manifests aborted: the table's partition spec changed under a " +
                                        "concurrent commit; it will be retried on the next maintenance run");
                    }
                    // Coerce into the current spec's partition type so files written under an older
                    // partition spec still cluster correctly.
                    StructLike partition =
                            PartitionUtil.coercePartition(partitionType, fileSpec, file.partition());
                    Object value = partition.get(0, Object.class);
                    if (value == null) {
                        return NULL_PARTITION_CLUSTER;
                    }
                    Integer cluster = partitionValueToCluster.get(value);
                    if (cluster != null) {
                        return cluster;
                    }
                    // Value not seen up front (typically a new partition added by a concurrent write to
                    // the same spec). Slot it next to its sort-order neighbor.
                    Map.Entry<Object, Integer> neighbor = partitionValueToCluster.floorEntry(value);
                    if (neighbor == null) {
                        neighbor = partitionValueToCluster.firstEntry();
                    }
                    return neighbor.getValue();
                })
                .commit();
        return null;
    }

    // Collect the distinct top-level partition values across all data manifests of the snapshot,
    // sort them, and assign each a bucket in [0, targetClusters) grouping sorted-adjacent values
    // into the same bucket. Clustering is limited to the top-level partition field because it is
    // usually the most selective for read-time filters and keeps the distinct-value set small.
    private static NavigableMap<Object, Integer> buildClusteredPartitionValues(
            Table table, Snapshot snapshot, int targetClusters) {
        PartitionSpec spec = table.spec();
        Type.PrimitiveType firstFieldType =
                spec.partitionType().fields().get(0).type().asPrimitiveType();
        Comparator<Object> comparator = partitionValueComparator(firstFieldType);
        Types.StructType partitionType = spec.partitionType();
        Map<Integer, PartitionSpec> specsById = table.specs();
        FileIO io = table.io();
        List<ManifestFile> dataManifests = snapshot.dataManifests(io);
        Iterable<CloseableIterable<DataFile>> perManifest = Iterables.transform(
                dataManifests,
                manifest -> (CloseableIterable<DataFile>)
                        ManifestFiles.read(manifest, io, specsById).select(ImmutableList.of("partition")));

        // Comparator-based dedup mirrors Iceberg's own equality for partition value types.
        NavigableSet<Object> uniqueValues = new TreeSet<>(comparator);
        try (CloseableIterable<DataFile> dataFiles = CloseableIterable.concat(perManifest)) {
            for (DataFile file : dataFiles) {
                StructLike partition = PartitionUtil.coercePartition(
                        partitionType, specsById.get(file.specId()), file.partition());
                Object value = partition.get(0, Object.class);
                if (value != null) {
                    uniqueValues.add(value);
                }
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Unable to read manifests while clustering partitions for snapshot " + snapshot.snapshotId(), e);
        }

        if (uniqueValues.isEmpty()) {
            return Collections.emptyNavigableMap();
        }

        NavigableMap<Object, Integer> valueToCluster = new TreeMap<>(comparator);
        int index = 0;
        int size = uniqueValues.size();
        for (Object value : uniqueValues) {
            valueToCluster.put(value, (int) ((long) index * targetClusters / size));
            index++;
        }
        return valueToCluster;
    }

    @SuppressWarnings("unchecked")
    private static Comparator<Object> partitionValueComparator(Type.PrimitiveType type) {
        return (Comparator<Object>) (Comparator<?>) Comparators.forType(type);
    }
}
