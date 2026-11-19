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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;

import java.util.Iterator;
import java.util.List;

/** Temporary Paimon 2.0 compatibility for bounded manifest scheduling on append-only LIMIT scans. */
final class PaimonScan {
    private PaimonScan() {
    }

    static InnerTableScan create(Table table, List<Predicate> predicates, int[] projection, Integer limit) {
        if (limit == null || limit <= 0 || !(table instanceof AppendOnlyFileStoreTable)
                || ((AppendOnlyFileStoreTable) table).coreOptions().deletionVectorsEnabled()
                || ((AppendOnlyFileStoreTable) table).coreOptions().dataEvolutionEnabled()) {
            ReadBuilder builder = table.newReadBuilder().withFilter(predicates).withProjection(projection);
            if (limit != null) {
                builder = builder.withLimit(limit);
            }
            return (InnerTableScan) builder.newScan();
        }

        AppendOnlyFileStoreTable appendTable = (AppendOnlyFileStoreTable) table;
        // Keep the SDK's batch scan, including snapshot selection and query authorization.
        InnerTableScan scan = appendTable.newScan(ignored -> newSnapshotReader(appendTable));
        if (!predicates.isEmpty()) {
            scan.withFilter(PredicateBuilder.and(predicates));
        }
        return scan.withReadType(table.rowType().project(projection)).withLimit(limit);
    }

    private static SnapshotReader newSnapshotReader(AppendOnlyFileStoreTable table) {
        SnapshotReader reader = table.newSnapshotReader();
        CoreOptions options = table.coreOptions();
        TableSchema schema = table.schema();
        BucketSelectConverter bucketSelector = new BucketSelectConverter(table.bucketMode(), options.bucketFunctionType(),
                schema.logicalRowType().notNull(), schema.logicalPartitionType(), schema.logicalBucketKeyType());
        AppendOnlyFileStoreScan scan = new AppendOnlyFileStoreScan(reader.manifestsReader(), bucketSelector,
                reader.snapshotManager(), table.schemaManager(), schema, table.store().manifestFileFactory(),
                options.scanManifestParallelism(), options.fileIndexReadEnabled(), false, false) {
            @Override
            public Iterator<ManifestEntry> readManifestEntries(List<ManifestFileMeta> manifests, boolean useSequential) {
                // TODO: Remove this adapter after upgrading to a Paimon release containing
                // https://github.com/apache/paimon/pull/10153. Paimon 2.0 eagerly schedules every ADD
                // manifest before applying LIMIT. Batching bounds that work; DELETE merging and row
                // counting still belong to the SDK. Auth/data filters can disable the pushed limit.
                return super.readManifestEntries(manifests,
                        useSequential || (limit != null && limit > 0 && inputFilter == null));
            }
        };
        // Deletion vectors are excluded above, so no deletion-vector metadata cache is needed.
        return new SnapshotReaderImpl(scan, schema, options, reader.snapshotManager(), reader.changelogManager(),
                reader.splitGenerator(), (fileScan, predicate) -> ((AppendOnlyFileStoreScan) fileScan).withFilter(predicate),
                reader.pathFactory(), table.name(), reader.indexFileHandler(), null);
    }
}
