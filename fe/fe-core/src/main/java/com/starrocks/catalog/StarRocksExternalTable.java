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
                                  List<Column> schema, long schemaVersion) {
        this(id, catalogName, databaseName, tableName, schema, schemaVersion,
                Collections.emptyList(), 0, null);
    }

    public StarRocksExternalTable(long id, String catalogName, String databaseName, String tableName,
                                  List<Column> schema, long schemaVersion,
                                  List<String> partitionColumnNames, long tableRowCount,
                                  Supplier<StarRocksRemoteTableStats.Snapshot> statsSnapshotSupplier) {
        super(id, tableName, TableType.STARROCKS, schema);
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schemaVersion = schemaVersion;
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
