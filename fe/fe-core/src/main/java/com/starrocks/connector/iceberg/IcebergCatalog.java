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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterViewStmt;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.catalog.IcebergView.STARROCKS_DIALECT;
import static com.starrocks.connector.iceberg.IcebergApiConverter.buildViewProperties;
import static com.starrocks.connector.iceberg.IcebergApiConverter.convertDbNameToNamespace;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static org.apache.iceberg.StarRocksIcebergTableScan.newTableScanContext;

public interface IcebergCatalog extends MemoryTrackable {
    Logger DEFAULT_LOGGER = LogManager.getLogger(IcebergCatalog.class);
    String EMPTY_PARTITION_NAME = "";

    default Logger getLogger() {
        return DEFAULT_LOGGER;
    }

    IcebergCatalogType getIcebergCatalogType();

    List<String> listAllDatabases(ConnectContext context);

    default void createDB(ConnectContext context, String dbName, Map<String, String> properties) {
    }

    default void dropDB(ConnectContext context, String dbName) throws MetaNotFoundException {
    }

    Database getDB(ConnectContext context, String dbName);

    List<String> listTables(ConnectContext context, String dbName);

    default boolean createTable(ConnectContext context,
                                String dbName,
                                String tableName,
                                Schema schema,
                                PartitionSpec partitionSpec,
                                String location,
                                SortOrder sortOrder,
                                Map<String, String> properties) {
        return false;
    }

    default boolean dropTable(ConnectContext context, String dbName, String tableName, boolean purge) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping tables");
    }

    void renameTable(ConnectContext context, String dbName, String tblName, String newTblName) throws StarRocksConnectorException;

    Table getTable(ConnectContext context, String dbName, String tableName) throws StarRocksConnectorException;

    default boolean tableExists(ConnectContext context, String dbName, String tableName) throws StarRocksConnectorException {
        try {
            getTable(context, dbName, tableName);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    default boolean createView(ConnectContext context, String catalogName, ConnectorViewDefinition connectorViewDefinition,
                               boolean replace) {
        return createViewDefault(context, connectorViewDefinition.getDatabaseName(), connectorViewDefinition, replace);
    }

    default boolean createViewDefault(ConnectContext context, String catalogName, ConnectorViewDefinition definition,
                                      boolean replace) {
        Schema schema = IcebergApiConverter.toIcebergApiSchema(definition.getColumns());
        Namespace ns = convertDbNameToNamespace(definition.getDatabaseName());
        ViewBuilder viewBuilder = getViewBuilder(context, TableIdentifier.of(ns, definition.getViewName()));
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery(STARROCKS_DIALECT, definition.getInlineViewDef())
                .withDefaultNamespace(ns)
                .withDefaultCatalog(definition.getCatalogName())
                .withProperties(buildViewProperties(definition, catalogName))
                .withLocation(defaultTableLocation(context, ns, definition.getViewName()));

        if (replace) {
            try {
                viewBuilder.createOrReplace();
            } catch (RESTException re) {
                DEFAULT_LOGGER.error("Failed to create view using Iceberg Catalog, for dbName {} viewName {}",
                        definition.getDatabaseName(), definition.getViewName(), re);
                throw new StarRocksConnectorException("Failed to create view using Iceberg Catalog",
                        new RuntimeException("Failed to create view using Iceberg Catalog, exception: " + re.getMessage(), re));
            }
        } else {
            viewBuilder.create();
        }

        return true;
    }

    default ViewBuilder getViewBuilder(ConnectContext context, TableIdentifier identifier) {
        throw new StarRocksConnectorException("This catalog doesn't support creating/alter views");
    }

    default boolean alterView(ConnectContext context, View currentView, ConnectorViewDefinition connectorViewDefinition) {
        return alterViewDefault(context, currentView, connectorViewDefinition);
    }

    default boolean alterViewDefault(ConnectContext context, View currentView, ConnectorViewDefinition definition) {

        Namespace ns = convertDbNameToNamespace(definition.getDatabaseName());
        ViewBuilder viewBuilder = getViewBuilder(context, TableIdentifier.of(ns, definition.getViewName()));
        Map<String, String> properties = currentView.properties();
        Map<String, String> alterProperties = definition.getProperties();

        boolean isAlterProperties = alterProperties != null && !alterProperties.isEmpty();
        if (isAlterProperties) {
            properties = Maps.newHashMap(properties);
            properties.putAll(alterProperties);
        }

        Schema schema = isAlterProperties ? currentView.schema() :
                IcebergApiConverter.toIcebergApiSchema(definition.getColumns());
        ViewVersion currentViewVersion = currentView.currentVersion();

        viewBuilder = viewBuilder.withSchema(schema)
                .withDefaultNamespace(currentViewVersion.defaultNamespace())
                .withDefaultCatalog(currentViewVersion.defaultCatalog())
                .withProperties(properties)
                .withLocation(currentView.location());

        for (ViewRepresentation viewRepresentation : currentViewVersion.representations()) {
            if (!(viewRepresentation instanceof SQLViewRepresentation sqlViewRepresentation)) {
                throw new StarRocksConnectorException("Only support SQL view representation, do not support [{}] type view",
                        viewRepresentation.type());
            }
            if (definition.getAlterDialectType() != AlterViewStmt.AlterDialectType.MODIFY ||
                    !sqlViewRepresentation.dialect().equals(STARROCKS_DIALECT)) {
                viewBuilder = viewBuilder.withQuery(sqlViewRepresentation.dialect(), sqlViewRepresentation.sql());
            }
        }

        if (definition.getInlineViewDef() != null) {
            viewBuilder = viewBuilder.withQuery(STARROCKS_DIALECT, definition.getInlineViewDef());
        }
        viewBuilder.createOrReplace();

        return true;
    }

    default boolean dropView(ConnectContext context, String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping views");
    }

    default View getView(ConnectContext context, String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't loading iceberg view");
    }

    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

    default void refreshTable(String dbName, String tableName, ConnectContext ctx, ExecutorService refreshExecutor) {
    }

    default void invalidatePartitionCache(String dbName, String tableName) {
    }

    default void invalidateCache(String dbName, String tableName) {
    }

    default StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext srScanContext) {
        return new StarRocksIcebergTableScan(
                table,
                table.schema(),
                newTableScanContext(table, srScanContext),
                srScanContext);
    }

    default String defaultTableLocation(ConnectContext context, Namespace ns, String tableName) {
        Map<String, String> properties = loadNamespaceMetadata(context, ns);
        String databaseLocation = properties.get(LOCATION_PROPERTY);
        checkArgument(databaseLocation != null, "location must be set for %s.%s", ns, tableName);

        if (databaseLocation.endsWith("/")) {
            return databaseLocation + tableName;
        } else {
            return databaseLocation + "/" + tableName;
        }
    }

    default Map<String, String> loadNamespaceMetadata(ConnectContext context, Namespace ns) {
        return new HashMap<>();
    }

    default Map<String, Long> estimateCount() {
        return new HashMap<>();
    }

    // --------------- partition APIs ---------------
    default Map<String, Partition> getPartitions(IcebergTable icebergTable, long snapshotId, ExecutorService executorService) {
        Table nativeTable = icebergTable.getNativeTable();
        Map<String, Partition> partitionMap = Maps.newHashMap();
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils.
                createMetadataTableInstance(nativeTable, MetadataTableType.PARTITIONS);
        TableScan tableScan = partitionsTable.newScan();
        if (snapshotId != -1) {
            tableScan = tableScan.useSnapshot(snapshotId);
        }
        if (executorService != null) {
            tableScan = tableScan.planWith(executorService);
        }
        Logger logger = getLogger();

        // TODO: ideally we should know if table is partitioned under a snapshotId.
        // but currently we just did it in a very wild way.
        if (nativeTable.spec().isUnpartitioned()) {
            Partition partition = null;
            try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
                for (FileScanTask task : tasks) {
                    // partitionsTable Table schema :
                    // record_count,
                    // file_count,
                    // total_data_file_size_in_bytes,
                    // position_delete_record_count,
                    // position_delete_file_count,
                    // equality_delete_record_count,
                    // equality_delete_file_count,
                    // last_updated_at,
                    // last_updated_snapshot_id
                    try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                        for (StructLike row : rows) {
                            // Get the last updated time of the table according to the table schema
                            // last_updated_at can be null if the referenced snapshot has been expired.
                            // Use Long wrapper to avoid NPE during auto-unboxing.
                            long lastUpdated = getPartitionLastUpdatedTime(icebergTable, row, 7,
                                    EMPTY_PARTITION_NAME, snapshotId);
                            partition = new Partition(lastUpdated);
                            break;
                        }
                    }
                }
                if (partition == null) {
                    long tableLastestSnapshotTime = getTableLastestSnapshotTime(icebergTable, logger);
                    logger.warn("The unpartitioned table [{}] has no partitions in PartitionsTable, " +
                            "using {} as last updated time", nativeTable.name(), tableLastestSnapshotTime);
                    partition = new Partition(tableLastestSnapshotTime);
                }
                partitionMap.put(EMPTY_PARTITION_NAME, partition);
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + nativeTable.name(), e);
            }
        } else {
            // For partition table, we need to get all partitions from PartitionsTable.
            try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
                for (FileScanTask task : tasks) {
                    // partitionsTable Table schema :
                    // partition,
                    // spec_id,
                    // record_count,
                    // file_count,
                    // total_data_file_size_in_bytes,
                    // position_delete_record_count,
                    // position_delete_file_count,
                    // equality_delete_record_count,
                    // equality_delete_file_count,
                    // last_updated_at,
                    // last_updated_snapshot_id
                    try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                        for (StructLike row : rows) {
                            // Get the partition data/spec id/last updated time according to the table schema
                            StructProjection partitionData = row.get(0, StructProjection.class);
                            int specId = row.get(1, Integer.class);
                            PartitionSpec spec = nativeTable.specs().get(specId);

                            String partitionName =
                                    PartitionUtil.convertIcebergPartitionToPartitionName(nativeTable, spec, partitionData);
                            long lastUpdated =
                                    getPartitionLastUpdatedTime(icebergTable, row, 9, partitionName, snapshotId);
                            Partition partition = new Partition(lastUpdated, specId);
                            partitionMap.put(partitionName, partition);
                        }
                    }
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + nativeTable.name(), e);
            }
        }
        return partitionMap;
    }

    private long getPartitionLastUpdatedTime(IcebergTable icebergTable, StructLike row,
                                             int columnIndex, String partitionName,
                                             long snapshotId) {
        Table nativeTable = icebergTable.getNativeTable();
        Logger logger = getLogger();
        // last_updated_at can be null if the referenced snapshot has been expired.
        // Use Long wrapper to avoid NPE during auto-unboxing.
        long lastUpdated = -1;
        if (row != null) {
            try {
                Long lastUpdatedWrapper = row.get(columnIndex, Long.class);
                if (lastUpdatedWrapper != null) {
                    lastUpdated = lastUpdatedWrapper;
                }
            } catch (Exception e) {
                logger.error("Failed to get last_updated_at for partition [{}] of table [{}] " +
                                "under snapshot [{}]", partitionName, nativeTable.name(), snapshotId, e);
            }
        }
        if (lastUpdated ==  -1) {
            // Fallback to current snapshot's timestamp if last_updated_at is null due to snapshot expiration.
            lastUpdated = getTableLastestSnapshotTime(icebergTable, logger);
            logger.warn("The table [{}] last_updated_at is null (snapshot [{}] may have been expired), " +
                    "using current snapshot timestamp: {}", nativeTable.name(), snapshotId, lastUpdated);
        }
        return lastUpdated;
    }

    private long getTableLastestSnapshotTime(IcebergTable icebergTable,
                                             Logger logger) {
        Table nativeTable = icebergTable.getNativeTable();
        Snapshot snapshot = nativeTable.currentSnapshot();
        if (snapshot == null) {
            logger.warn("The table [{}] has no current snapshot, using -1 as last updated time",
                    nativeTable.name());
            return -1;
        }
        return snapshot.timestampMillis();
    }

    default List<String> listPartitionNames(IcebergTable icebergTable,
                                            ConnectorMetadatRequestContext requestContext,
                                            ExecutorService executorService) {
        Table nativeTable = icebergTable.getNativeTable();

        // Call public method so subclasses can override and optimize this method.
        Map<String, Partition> partitionMap = getPartitions(icebergTable, requestContext.getSnapshotId(), executorService);
        if (nativeTable.spec().isUnpartitioned()) {
            return List.of();
        } else {
            return new ArrayList<>(partitionMap.keySet());
        }
    }

    default List<Partition> getPartitionsByNames(IcebergTable icebergTable,
                                                 ExecutorService executorService,
                                                 List<String> partitionNames) {
        Table nativeTable = icebergTable.getNativeTable();
        long snapshotId = -1;
        if (nativeTable.currentSnapshot() != null) {
            snapshotId = nativeTable.currentSnapshot().snapshotId();
        }

        // Call public method so subclasses can override and optimize this method.
        Map<String, Partition> partitionMap = getPartitions(icebergTable, snapshotId, executorService);
        if (nativeTable.spec().isUnpartitioned()) {
            return List.of(partitionMap.get(EMPTY_PARTITION_NAME));
        } else {
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            partitionNames.forEach(partitionName -> partitions.add(partitionMap.get(partitionName)));
            return partitions.build();
        }
    }
}
