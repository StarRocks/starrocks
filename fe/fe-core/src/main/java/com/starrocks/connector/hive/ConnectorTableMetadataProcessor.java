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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.conf.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.connector.iceberg.CachingIcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ConnectorTableMetadataProcessor extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ConnectorTableMetadataProcessor.class);

    private final Set<BaseTableInfo> registeredTableInfos = Sets.newConcurrentHashSet();

    private final Map<String, CacheUpdateProcessor> cacheUpdateProcessors = new ConcurrentHashMap<>();

    private final ExecutorService refreshRemoteFileExecutor;
    private final Map<String, IcebergCatalog> cachingIcebergCatalogs = new ConcurrentHashMap<>();

    public void registerTableInfo(BaseTableInfo tableInfo) {
        registeredTableInfos.add(tableInfo);
    }

    public void registerCacheUpdateProcessor(String catalogName, CacheUpdateProcessor cache) {
        LOG.info("register to update {} metadata cache in the ConnectorTableMetadataProcessor", catalogName);
        cacheUpdateProcessors.put(catalogName, cache);
    }

    public void unRegisterCacheUpdateProcessor(String catalogName) {
        LOG.info("unregister to update {} metadata cache in the ConnectorTableMetadataProcessor", catalogName);
        cacheUpdateProcessors.remove(catalogName);
    }

    public void registerCachingIcebergCatalog(String catalogName, IcebergCatalog icebergCatalog) {
        LOG.info("register to caching iceberg catalog on {} in the ConnectorTableMetadataProcessor", catalogName);
        cachingIcebergCatalogs.put(catalogName, icebergCatalog);
    }

    public void unRegisterCachingIcebergCatalog(String catalogName) {
        LOG.info("unregister to caching iceberg catalog on {} in the ConnectorTableMetadataProcessor", catalogName);
        cachingIcebergCatalogs.remove(catalogName);
    }

    public ConnectorTableMetadataProcessor() {
        super(ConnectorTableMetadataProcessor.class.getName(), Config.background_refresh_metadata_interval_millis);
        refreshRemoteFileExecutor = Executors.newFixedThreadPool(Config.background_refresh_file_metadata_concurrency,
                new ThreadFactoryBuilder().setNameFormat("background-refresh-remote-files-%d").build());
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_hms_events_incremental_sync && Config.enable_background_refresh_resource_table_metadata) {
            refreshResourceHiveTable();
        }

        refreshRegisteredTable();

        if (Config.enable_background_refresh_connector_metadata) {
            refreshCatalogTable();
            refreshIcebergCachingCatalog();
        }
    }

    private void refreshCatalogTable() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> catalogNames = Lists.newArrayList(cacheUpdateProcessors.keySet());
        for (String catalogName : catalogNames) {
            CacheUpdateProcessor updateProcessor = cacheUpdateProcessors.get(catalogName);
            if (updateProcessor == null) {
                LOG.error("Failed to get cacheUpdateProcessor by catalog {}.", catalogName);
                continue;
            }

            for (HiveTableName cachedTableName : updateProcessor.getCachedTableNames()) {
                String dbName = cachedTableName.getDatabaseName();
                String tableName = cachedTableName.getTableName();
                Table table;
                try {
                    table = metadataMgr.getTable(catalogName, dbName, tableName);
                } catch (Exception e) {
                    LOG.warn("can't get table of {}.{}.{}，msg: ", catalogName, dbName, tableName, e);
                    continue;
                }
                if (table == null) {
                    LOG.warn("{}.{}.{} not exist", catalogName, dbName, tableName);
                    continue;
                }
                try {
                    updateProcessor.refreshTableBackground(table, true, refreshRemoteFileExecutor);
                } catch (Exception e) {
                    LOG.warn("refresh {}.{}.{} meta store info failed, msg : ", catalogName, dbName,
                            tableName, e);
                    continue;
                }
                LOG.info("refresh table {}.{}.{} success", catalogName, dbName, tableName);
            }
            LOG.info("refresh connector metadata {} finished", catalogName);
        }
    }

    private void refreshIcebergCachingCatalog() {
        List<String> catalogNames = Lists.newArrayList(cachingIcebergCatalogs.keySet());
        for (String catalogName : catalogNames) {
            CachingIcebergCatalog icebergCatalog = (CachingIcebergCatalog) cachingIcebergCatalogs.get(catalogName);
            if (icebergCatalog == null) {
                LOG.error("Failed to get cachingIcebergCatalog by catalog {}.", catalogName);
                continue;
            }
            LOG.info("Start to refresh iceberg caching catalog {}", catalogName);
            icebergCatalog.refreshCatalog();
            LOG.info("Finish to refresh iceberg caching catalog {}", catalogName);
        }
    }

    private void refreshRegisteredTable() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<BaseTableInfo> registeredTableInfoList = Lists.newArrayList(registeredTableInfos);
        for (BaseTableInfo registeredTableInfo : registeredTableInfoList) {
            LOG.info("Start to refresh registered table {}.{}.{} metadata in the background",
                    registeredTableInfo.getCatalogName(), registeredTableInfo.getDbName(),
                    registeredTableInfo.getTableName());
            try {
                Table registeredTable = registeredTableInfo.getTable();
                if (registeredTable == null) {
                    LOG.warn("Table {}.{}.{} not exist",  registeredTableInfo.getCatalogName(),
                            registeredTableInfo.getDbName(), registeredTableInfo.getTableName());
                    continue;
                }
                if (!registeredTable.isHiveTable()) {
                    continue;
                }
                metadataMgr.refreshTable(registeredTableInfo.getCatalogName(), registeredTableInfo.getDbName(),
                        registeredTable, Lists.newArrayList(), false);

            } catch (Exception e) {
                LOG.error("Background refresh table metadata failed on {}.{}.{}", registeredTableInfo.getCatalogName(),
                        registeredTableInfo.getDbName(), registeredTableInfo.getTableName(), e);
            }
            LOG.info("Refresh registered table {}.{}.{} metadata success",
                    registeredTableInfo.getCatalogName(), registeredTableInfo.getDbName(),
                    registeredTableInfo.getTableName());
        }
    }

    private void refreshResourceHiveTable() {
        LOG.info("Start to refresh hive external table metadata in the background");
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MetadataMgr metadataMgr = gsm.getMetadataMgr();
        List<Database> databases = gsm.getDbIds().stream()
                .map(gsm::getDb)
                .filter(Objects::nonNull)
                .filter(db -> !db.isSystemDatabase())
                .collect(Collectors.toList());
        for (Database db : databases) {
            List<HiveTable> tables = db.getTables().stream()
                    .filter(tbl -> tbl.getType() == Table.TableType.HIVE)
                    .map(tbl -> (HiveTable) tbl)
                    .collect(Collectors.toList());
            for (HiveTable table : tables) {
                try {
                    LOG.info("Start to refresh hive external table metadata on {}.{} of StarRocks and {}.{} of hive " +
                            "in the background", db.getFullName(), table.getName(), table.getDbName(), table.getTableName());
                    // we didn't use db locks to prevent background tasks from affecting the query.
                    // So we need to check if the table to be refreshed exists.
                    if (db.getTable(table.getId()) != null) {
                        metadataMgr.refreshTable(table.getCatalogName(), db.getFullName(),
                                table, Lists.newArrayList(), false);
                    }
                } catch (Exception e) {
                    LOG.error("Background refresh hive metadata failed on {}.{}", db.getFullName(), table.getName(), e);
                }
            }
        }
        LOG.info("This round of background refresh hive external table metadata is over");
    }
}
