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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CacheUpdateProcessor;
import com.starrocks.connector.DatabaseTableName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class OdpsCacheUpdateProcessor implements CacheUpdateProcessor {
    private static final Logger LOG = LogManager.getLogger(OdpsCacheUpdateProcessor.class);

    private final Odps odps;
    private final String catalogName;
    private final LoadingCache<String, Set<String>> tableNameCache;
    private final LoadingCache<OdpsTableName, OdpsTable> tableCache;
    private final LoadingCache<OdpsTableName, List<Partition>> partitionCache;

    public OdpsCacheUpdateProcessor(String catalogName,
                                    Odps odps,
                                    LoadingCache<String, Set<String>> tableNameCache,
                                    LoadingCache<OdpsTableName, OdpsTable> tableCache,
                                    LoadingCache<OdpsTableName, List<Partition>> partitionCache) {
        this.catalogName = catalogName;
        this.odps = odps;
        this.tableNameCache = tableNameCache;
        this.tableCache = tableCache;
        this.partitionCache = partitionCache;
    }

    @Override
    public void refreshTable(String dbName, Table table, boolean onlyCachedPartitions) {
        if (!(table instanceof OdpsTable odpsTable)) {
            LOG.warn("Table {} is not an OdpsTable, skip refresh", table.getName());
            return;
        }

        OdpsTableName odpsTableName = OdpsUtils.getOdpsTableName(odpsTable);

        // Update table cache
        com.aliyun.odps.Table oTable = OdpsUtils.getOdpsTable(odps, odpsTable);
        OdpsTable updateTable = new OdpsTable(catalogName, oTable);
        tableCache.invalidate(odpsTableName);
        tableCache.put(odpsTableName, updateTable);

        // If table is partitioned, refresh partition cache
        if (!table.isUnPartitioned()) {
            List<Partition> oPartitions = OdpsUtils.getOdpsTablePartitions(odps, odpsTable);
            partitionCache.invalidate(odpsTableName);
            partitionCache.put(odpsTableName, oPartitions);
        }

        LOG.info("Refreshed table {}.{} in catalog {}", dbName, table.getName(), catalogName);
    }

    @Override
    public Set<DatabaseTableName> getCachedTableNames() {
        Set<DatabaseTableName> cachedTableNames = Sets.newHashSet();
        try {
            // Get all cached database names from tableNameCache
            Set<String> dbNames = tableNameCache.asMap().keySet();
            for (String dbName : dbNames) {
                Set<String> tableNames = tableNameCache.getIfPresent(dbName);
                if (tableNames != null) {
                    for (String tableName : tableNames) {
                        cachedTableNames.add(DatabaseTableName.of(dbName, tableName));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get cached table names for catalog {}", catalogName, e);
        }
        return cachedTableNames;
    }

    @Override
    public void refreshTableBackground(Table table, boolean onlyCachedPartitions, ExecutorService executor) {
        if (!(table instanceof OdpsTable odpsTable)) {
            LOG.warn("Table {} is not an OdpsTable, skip background refresh", table.getName());
            return;
        }

        OdpsTableName odpsTableName = OdpsUtils.getOdpsTableName(odpsTable);

        // Submit background refresh task
        executor.submit(() -> {
            try {
                com.aliyun.odps.Table oTable = OdpsUtils.getOdpsTable(odps, odpsTable);
                // Reload table from cache
                OdpsTable updateTable = new OdpsTable(catalogName, oTable);
                // Invalidate table cache
                tableCache.invalidate(odpsTableName);
                tableCache.put(odpsTableName, updateTable);

                LOG.info("Background refreshed table {}.{} in catalog {}",
                        odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName(), catalogName);
            } catch (Exception e) {
                LOG.error("Failed to background refresh table {}.{} in catalog {}",
                        odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName(), catalogName, e);
            }
        });
    }

    public void refreshPartition(Table table, List<String> partitionNames) {
        if (!(table instanceof OdpsTable odpsTable)) {
            LOG.warn("Table {} is not an OdpsTable, skip partition refresh", table.getName());
            return;
        }

        OdpsTableName odpsTableName = OdpsUtils.getOdpsTableName(odpsTable);

        List<Partition> refreshed = OdpsUtils.getOdpsTablePartitionsByNames(odps, odpsTableName, partitionNames);

        Set<String> refreshedSpecs = refreshed.stream()
                .map(p -> p.getPartitionSpec().toString(false, true))
                .collect(Collectors.toSet());

        List<Partition> existing = partitionCache.getIfPresent(odpsTableName);
        List<Partition> merged = new ArrayList<>();
        if (existing != null) {
            for (Partition p : existing) {
                if (!refreshedSpecs.contains(p.getPartitionSpec().toString(false, true))) {
                    merged.add(p);
                }
            }
        }
        merged.addAll(refreshed);

        partitionCache.put(odpsTableName, merged);
        LOG.debug("Refreshed {} partition(s) for table {}.{} in catalog {}",
                refreshed.size(), odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName(), catalogName);
    }
}

