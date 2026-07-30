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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.ReplayConnectorMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Table.TableType.ICEBERG;

/**
 * A {@link ConnectorMetadata} that serves iceberg external-catalog tables during ReplayFromDump. It
 * synthesizes a native iceberg table from the declared column schema so the table can be created and planned
 * entirely offline, with no iceberg catalog / metastore / object store access.
 *
 * <p>Row count and per-column statistics come from the dump (served by {@link ReplayConnectorMetadata}).
 * For a partitioned table the dump also carries the partition spec (transforms) and partition names; this
 * rebuilds the real {@link PartitionSpec} and appends one {@link DataFile} per partition so the native
 * {@code planFiles} reproduces partition pruning ({@code partitions=X/Y}) exactly, driven by the same
 * predicate the query carries.
 */
public class ReplayIcebergCatalogMetadata extends ReplayConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(ReplayIcebergCatalogMetadata.class);

    public ReplayIcebergCatalogMetadata(String catalogName) {
        super(catalogName);
    }

    public void registerTable(String dbName, String tableName, List<Column> columns, long rowCount,
                              Map<String, ColumnStatistic> columnStats, List<String> partitionTransforms,
                              List<String> partitionNames, Map<String, Long> partitionRowCounts) throws IOException {
        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);
        PartitionSpec spec = buildPartitionSpec(schema, partitionTransforms);
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        File baseDir = Files.createTempDirectory("replay_iceberg_").toFile();
        // TestTables keeps a process-global registry keyed by the native table name, so two dumps that scan a
        // same-named table (replayed back-to-back in one JVM, or two same-named tables in different dbs) would
        // collide with AlreadyExistsException. Give the backing native table a unique name; the StarRocks-facing
        // db/table names passed to MockIcebergTable stay as declared.
        String nativeName = tableName + "_" + tableId;
        TestTables.TestTable nativeTable = TestTables.create(
                new File(baseDir, dbName + "_" + nativeName), nativeName, schema, spec, 1);
        // For a partitioned table, append one DataFile per captured partition so native planFiles can prune.
        // Batch them into a single commit -- per-partition commits would be O(partitions) and slow.
        if (!spec.isUnpartitioned() && partitionNames != null && !partitionNames.isEmpty()) {
            // Give each partition's synthesized DataFile its real record count (from the dump's per-partition
            // table_row_count, i.e. Iceberg $partitions.record_count) so the reconstructed table faithfully
            // mirrors the source -- the per-file counts sum back to the true table total instead of an
            // arbitrary even split. (FE scan cardinality comes from getTableStatistics, not these counts; they
            // reach BE only as a per-split record-count hint.) Even split is the fallback for legacy dumps that
            // carry no per-partition counts.
            long evenSplit = Math.max(1L, rowCount / partitionNames.size());
            AppendFiles append = nativeTable.newAppend();
            int idx = 0;
            for (String partitionName : partitionNames) {
                long recordCount = partitionRowCounts == null
                        ? evenSplit : partitionRowCounts.getOrDefault(partitionName, evenSplit);
                try {
                    DataFile file = DataFiles.builder(nativeTable.spec())
                            .withPath("/replay/data-" + (idx++) + ".parquet")
                            .withFileSizeInBytes(Math.max(1L, recordCount))
                            .withPartitionPath(partitionName)
                            .withRecordCount(recordCount)
                            .build();
                    append.appendFile(file);
                } catch (Exception e) {
                    // A partition name written under an older, evolved partition spec need not parse against
                    // the single current spec rebuilt above (e.g. an old "dt_month=..." path over a table now
                    // partitioned by identity "dt"). Skip that partition -- it simply won't contribute to
                    // pruning/counts -- rather than failing the whole replay. Faithful spec-evolution support
                    // would need the dump to carry each partition's spec id; tracked as a follow-up.
                    LOG.warn("replay: skipping iceberg partition '{}' incompatible with the current spec of {}.{}: {}",
                            partitionName, dbName, tableName, e.getMessage());
                }
            }
            append.commit();
        }
        MockIcebergTable table = new MockIcebergTable(tableId, tableName, catalogName, null,
                dbName, tableName, columns, nativeTable, Maps.newHashMap(), "");
        register(dbName, tableName, table, rowCount, columnStats, partitionNames);
    }

    // Rebuild the iceberg PartitionSpec from the captured transform strings (as produced by
    // IcebergTable.getPartitionColumnNamesWithTransform): a bare column name is an identity transform, and
    // fn(col)/fn(col, n) map to the day/month/year/hour/bucket/truncate builders. An unrecognized transform
    // leaves the table unpartitioned (a faithful degradation, never a wrong plan).
    @VisibleForTesting
    static PartitionSpec buildPartitionSpec(Schema schema, List<String> transforms) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        if (transforms == null) {
            return builder.build();
        }
        for (String transform : transforms) {
            String t = transform.trim().replace("`", "");
            int lp = t.indexOf('(');
            try {
                if (lp < 0) {
                    builder.identity(t);
                    continue;
                }
                String fn = t.substring(0, lp).trim().toLowerCase();
                String args = t.substring(lp + 1, t.lastIndexOf(')')).trim();
                String[] parts = args.split(",");
                String col = parts[0].trim().replace("`", "");
                switch (fn) {
                    case "identity":
                        builder.identity(col);
                        break;
                    case "year":
                        builder.year(col);
                        break;
                    case "month":
                        builder.month(col);
                        break;
                    case "day":
                        builder.day(col);
                        break;
                    case "hour":
                        builder.hour(col);
                        break;
                    case "bucket":
                        builder.bucket(col, Integer.parseInt(parts[1].trim()));
                        break;
                    case "truncate":
                        builder.truncate(col, Integer.parseInt(parts[1].trim()));
                        break;
                    default:
                        return PartitionSpec.builderFor(schema).build();
                }
            } catch (Exception e) {
                return PartitionSpec.builderFor(schema).build();
            }
        }
        return builder.build();
    }

    @Override
    public Table.TableType getTableType() {
        return ICEBERG;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        org.apache.iceberg.Table nativeTable = ((IcebergTable) table).getNativeTable();
        Expression predicate = toIcebergPredicate(params.getPredicate(), nativeTable.schema());
        List<RemoteFileInfo> result = Lists.newArrayList();
        try (CloseableIterable<FileScanTask> tasks = nativeTable.newScan().filter(predicate).planFiles()) {
            for (FileScanTask task : tasks) {
                result.add(new IcebergRemoteFileInfo(task));
            }
        } catch (Exception e) {
            // fall back to what we have; an empty list simply yields no pruning rather than a wrong plan.
        }
        return result;
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        return new RemoteFileInfoDefaultSource(getRemoteFiles(table, params));
    }

    private static Expression toIcebergPredicate(ScalarOperator predicate, Schema schema) {
        if (predicate == null) {
            return Expressions.alwaysTrue();
        }
        try {
            Expression expr = new ScalarOperatorToIcebergExpr().convertStrict(
                    Collections.singletonList(predicate),
                    new ScalarOperatorToIcebergExpr.IcebergContext(schema.asStruct()));
            return expr == null ? Expressions.alwaysTrue() : expr;
        } catch (Exception e) {
            return Expressions.alwaysTrue();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList(new Partition(100L));
    }

    @Override
    public TvrVersionRange getTableVersionRange(String dbName, Table table,
                                                Optional<ConnectorTableVersion> startVersion,
                                                Optional<ConnectorTableVersion> endVersion) {
        return TvrTableSnapshot.of(TvrVersion.of(1L));
    }
}
