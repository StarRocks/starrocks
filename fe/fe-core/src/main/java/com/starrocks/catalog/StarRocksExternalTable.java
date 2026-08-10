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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.starrocks.StarRocksRemoteTableStats;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

public class StarRocksExternalTable extends Table {
    // Connection settings (fe http url, credentials, transport, timeouts) are intentionally NOT
    // stored here: the table object would snapshot them at resolve time and go stale when the
    // catalog is altered. Build the client from the catalog's current properties via
    // StarRocksFeClient.fromCatalog(getCatalogName()) at use time instead.
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final long schemaVersion;
    // Remote table id; the incarnation marker in getUUID().
    private final long remoteTableId;
    private final List<String> partitionColumnNames;
    // Whole-table physical row count from remote getTable metadata; 0 = unknown.
    private final long tableRowCount;
    // Memoizing supplier over the connector's statistics snapshot cache. The
    // table object lives for the duration of the query that resolved it, so
    // memoizing here pins one snapshot generation for both partition pruning
    // and statistics derivation of that query. Null supplier (or a null result)
    // means statistics are unavailable and callers degrade to tableRowCount.
    private final Supplier<StarRocksRemoteTableStats.Snapshot> statsSnapshotSupplier;
    private volatile boolean statsSnapshotResolved;
    private volatile StarRocksRemoteTableStats.Snapshot statsSnapshot;

    public StarRocksExternalTable(long id, String catalogName, String databaseName, String tableName,
                                  List<Column> schema, long schemaVersion, long remoteTableId,
                                  List<String> partitionColumnNames, long tableRowCount,
                                  Supplier<StarRocksRemoteTableStats.Snapshot> statsSnapshotSupplier) {
        super(id, tableName, TableType.STARROCKS, schema);
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schemaVersion = schemaVersion;
        this.remoteTableId = remoteTableId;
        this.partitionColumnNames = partitionColumnNames == null ?
                Collections.emptyList() : ImmutableList.copyOf(partitionColumnNames);
        this.tableRowCount = tableRowCount;
        this.statsSnapshotSupplier = statsSnapshotSupplier;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return databaseName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    public long getSchemaVersion() {
        return schemaVersion;
    }

    public long getRemoteTableId() {
        return remoteTableId;
    }

    /**
     * Identity of this table for the connector statistics framework, which is the only
     * consumer of external-table UUIDs: {@code StatisticsUtils.getTableByUUID} splits the
     * value on '.', requires exactly four segments, re-resolves catalog.db.table through
     * MetadataMgr and then demands that the resolved table reports the same UUID.
     * <p>
     * The inherited {@link Table#getUUID()} (the local numeric id) satisfies none of that:
     * this connector mints a fresh id from CONNECTOR_ID_GENERATOR on every getTable(), so
     * the same remote table gets a different id per resolution and the value is not even
     * parseable -- every lookup died on the four-segment precondition with a bare
     * IllegalStateException, and the async connector-stats cache then retried each
     * unresolvable key forever.
     * <p>
     * The remote table id is the incarnation marker: it is unique within the remote cluster
     * and never reused, so it survives ALTER (schema evolution does not make it a different
     * table) while a dropped-and-recreated table gets a fresh one and cannot inherit the
     * previous incarnation's statistics. The remote create time would not do: it has
     * second granularity, so a drop and recreate inside the same second repeats it.
     */
    @Override
    public String getUUID() {
        return String.join(".", escapeSegment(catalogName), escapeSegment(databaseName),
                escapeSegment(tableName), Long.toString(remoteTableId));
    }

    /**
     * Table and database names may legally contain '.' (FeNameFormat only rejects NUL), which
     * would push the UUID past four segments and bring back the very IllegalStateException
     * this identity exists to avoid. Replace it, the way {@code PaimonTable.getUUID()} does
     * for its own segment. The mapping is deliberately lossy: nothing reads a name back out
     * of a UUID, and an escaped name that happens to collide with a real table cannot be
     * mistaken for it, because the trailing remote table id makes getTableByUUID's equality
     * check fail. Such a table simply resolves to "no statistics" instead of throwing.
     */
    private static String escapeSegment(String segment) {
        return segment.replace(".", "_");
    }

    public long getTableRowCount() {
        return tableRowCount;
    }

    /**
     * One snapshot generation pinned per table object (= per query). May be
     * null: statistics disabled, remote endpoint unavailable, or fetch failed.
     */
    public StarRocksRemoteTableStats.Snapshot getStatsSnapshot() {
        if (!statsSnapshotResolved) {
            synchronized (this) {
                if (!statsSnapshotResolved) {
                    statsSnapshot = statsSnapshotSupplier == null ? null : statsSnapshotSupplier.get();
                    statsSnapshotResolved = true;
                }
            }
        }
        return statsSnapshot;
    }

    @Override
    public List<Column> getPartitionColumns() {
        if (partitionColumnNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<Column> columns = new ArrayList<>(partitionColumnNames.size());
        for (String name : partitionColumnNames) {
            for (Column column : fullSchema) {
                if (column.getName().toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
                    columns.add(column);
                    break;
                }
            }
        }
        return columns;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        // Callers (e.g. removePartitionPredicate) mutate the returned list.
        return new ArrayList<>(partitionColumnNames);
    }

    @Override
    public boolean isUnPartitioned() {
        return partitionColumnNames.isEmpty();
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(getId(), TTableType.STARROCKS_TABLE, fullSchema.size(), 0, name, databaseName);
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
