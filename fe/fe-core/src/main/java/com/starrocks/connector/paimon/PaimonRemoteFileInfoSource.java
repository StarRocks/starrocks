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

package com.starrocks.connector.paimon;

import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/** Demand-driven file planning. Only execution, after applying WHERE, may stop this source at LIMIT. */
final class PaimonRemoteFileInfoSource implements RemoteFileInfoSource {
    private final Table table;
    private final List<Predicate> predicates;
    private final int[] projection;
    private final long snapshotId;
    private final boolean fullSnapshot;
    private Iterator<Split> splits;
    private boolean closed;
    private long plannedFiles;
    private long plannedBytes;
    private long plannedSplits;
    private final PaimonMetricRegistry metrics = new PaimonMetricRegistry();

    PaimonRemoteFileInfoSource(Table table, List<Predicate> predicates, int[] projection,
                              long snapshotId, boolean fullSnapshot) {
        this.table = table;
        this.predicates = predicates;
        this.projection = projection;
        this.snapshotId = snapshotId;
        this.fullSnapshot = fullSnapshot;
    }

    private Iterator<Split> plan() {
        if (table instanceof AppendOnlyFileStoreTable append && fullSnapshot) {
            if (snapshotId < 0) {
                return Collections.emptyIterator();
            }
            CoreOptions options = append.coreOptions();
            // File-by-file splits are safe only when files do not require cross-file merging.
            // Keep SDK authorization, special startup modes and read-protection tags on its native path.
            if (!options.deletionVectorsEnabled() && !options.dataEvolutionEnabled() && !options.queryAuthEnabled()
                    && !options.scanPlanSortPartition() && options.scanPlanAutoTagTimeRetained() == null
                    && (options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT
                    || options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT_FULL)) {
                SnapshotReader reader = append.newSnapshotReader().withSnapshot(snapshotId).withMode(ScanMode.ALL)
                        .withReadType(table.rowType().project(projection)).withMetricRegistry(metrics);
                if (!predicates.isEmpty()) {
                    reader.withFilter(PredicateBuilder.and(predicates));
                }
                if (options.scanBucket() != null) {
                    reader.withBucket(options.scanBucket());
                }
                // TODO: Replace this adapter with the SDK's lazy split API when available. The SDK's
                // readFileIterator batches ADD manifests and merges DELETE entries before yielding files.
                // No raw-row LIMIT is passed: residual predicates may reject arbitrarily many files.
                Iterator<ManifestEntry> files = reader.readFileIterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return files.hasNext();
                    }

                    @Override
                    public Split next() {
                        ManifestEntry entry = files.next();
                        return DataSplit.builder().withSnapshot(snapshotId).withPartition(entry.partition())
                                .withBucket(entry.bucket()).withTotalBuckets(entry.totalBuckets())
                                .withBucketPath(reader.pathFactory().bucketPath(entry.partition(), entry.bucket()).toString())
                                .withDataFiles(List.of(entry.file())).rawConvertible(true).isStreaming(false).build();
                    }
                };
            }
        }
        // PK/merge-on-read and special tables still use SDK split grouping, but ranges are dispatched on demand.
        return table.newReadBuilder().withFilter(predicates).withProjection(projection).newScan().plan().splits().iterator();
    }

    @Override
    public boolean hasMoreOutput() {
        if (closed) {
            return false;
        }
        if (splits == null) {
            splits = plan();
        }
        boolean hasNext = splits.hasNext();
        recordManifestMetrics();
        return hasNext;
    }

    @Override
    public RemoteFileInfo getOutput() {
        if (!hasMoreOutput()) {
            throw new NoSuchElementException();
        }
        Split split = splits.next();
        plannedSplits++;
        if (split instanceof DataSplit dataSplit) {
            plannedFiles += dataSplit.dataFiles().size();
            plannedBytes += dataSplit.dataFiles().stream().mapToLong(file -> file.fileSize()).sum();
        }
        String prefix = "Paimon.plan." + table.name() + ".";
        Tracers.record(EXTERNAL, prefix + "resultedDataFilesNum", String.valueOf(plannedFiles));
        Tracers.record(EXTERNAL, prefix + "resultedDataFilesSize", plannedBytes + " B");
        Tracers.record(EXTERNAL, prefix + "resultSplitsNum", String.valueOf(plannedSplits));
        RemoteFileInfo result = new RemoteFileInfo();
        result.setFiles(List.of(PaimonRemoteFileDesc.createPaimonRemoteFileDesc(
                new PaimonSplitsInfo(predicates, List.of(split)))));
        return result;
    }

    private void recordManifestMetrics() {
        if (metrics.getMetricGroup() != null) {
            String prefix = "Paimon.plan." + table.name() + ".";
            Tracers.record(EXTERNAL, prefix + "manifestNumReadFromRemote",
                    String.valueOf(((Gauge<?>) metrics.getMetrics().get(ScanMetrics.MANIFEST_MISSED_CACHE)).getValue()));
            Tracers.record(EXTERNAL, prefix + "manifestNumReadFromCache",
                    String.valueOf(((Gauge<?>) metrics.getMetrics().get(ScanMetrics.MANIFEST_HIT_CACHE)).getValue()));
        }
    }

    @Override
    public void close() {
        closed = true;
        // The SDK iterator opens/closes manifest readers within each batch; retain no completed batches.
        splits = null;
    }
}
