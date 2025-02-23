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
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.sql.ast.AlterViewStmt;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
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

    List<String> listAllDatabases();

    default void createDB(String dbName, Map<String, String> properties) {
    }

    default void dropDB(String dbName) throws MetaNotFoundException {
    }

    Database getDB(String dbName);

    List<String> listTables(String dbName);

    default boolean createTable(String dbName,
                                String tableName,
                                Schema schema,
                                PartitionSpec partitionSpec,
                                String location,
                                Map<String, String> properties) {
        return false;
    }

    default boolean dropTable(String dbName, String tableName, boolean purge) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping tables");
    }

    void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException;

    Table getTable(String dbName, String tableName) throws StarRocksConnectorException;

    default boolean tableExists(String dbName, String tableName) throws StarRocksConnectorException {
        try {
            getTable(dbName, tableName);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    default boolean createView(String catalogName, ConnectorViewDefinition connectorViewDefinition, boolean replace) {
        return createViewDefault(connectorViewDefinition.getDatabaseName(), connectorViewDefinition, replace);
    }

    default boolean createViewDefault(String catalogName, ConnectorViewDefinition definition, boolean replace) {
        Schema schema = IcebergApiConverter.toIcebergApiSchema(definition.getColumns());
        Namespace ns = convertDbNameToNamespace(definition.getDatabaseName());
        ViewBuilder viewBuilder = getViewBuilder(TableIdentifier.of(ns, definition.getViewName()));
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery(STARROCKS_DIALECT, definition.getInlineViewDef())
                .withDefaultNamespace(ns)
                .withDefaultCatalog(definition.getCatalogName())
                .withProperties(buildViewProperties(definition, catalogName))
                .withLocation(defaultTableLocation(ns, definition.getViewName()));

        if (replace) {
            viewBuilder.createOrReplace();
        } else {
            viewBuilder.create();
        }

        return true;
    }

    default ViewBuilder getViewBuilder(TableIdentifier identifier) {
        throw new StarRocksConnectorException("This catalog doesn't support creating/alter views");
    }

    default boolean alterView(View currentView, ConnectorViewDefinition connectorViewDefinition) {
        return alterViewDefault(currentView, connectorViewDefinition);
    }

    default boolean alterViewDefault(View currentView, ConnectorViewDefinition definition) {

        Namespace ns = convertDbNameToNamespace(definition.getDatabaseName());
        ViewBuilder viewBuilder = getViewBuilder(TableIdentifier.of(ns, definition.getViewName()));
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

    default boolean dropView(String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping views");
    }

    default View getView(String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't loading iceberg view");
    }

    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

    default void refreshTable(String dbName, String tableName, ExecutorService refreshExecutor) {
    }

    default void invalidatePartitionCache(String dbName, String tableName) {
    }

    default void invalidateCache(String dbName, String tableName) {
    }

    default StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext srScanContext) {
        return new StarRocksIcebergTableScan(
                table,
                table.schema(),
                newTableScanContext(table),
                srScanContext);
    }

    default String defaultTableLocation(Namespace ns, String tableName) {
        Map<String, String> properties = loadNamespaceMetadata(ns);
        String databaseLocation = properties.get(LOCATION_PROPERTY);
        checkArgument(databaseLocation != null, "location must be set for %s.%s", ns, tableName);

        if (databaseLocation.endsWith("/")) {
            return databaseLocation + tableName;
        } else {
            return databaseLocation + "/" + tableName;
        }
    }

    default Map<String, String> loadNamespaceMetadata(Namespace ns) {
        return new HashMap<>();
    }

    default Map<String, Long> estimateCount() {
        return new HashMap<>();
    }

    default List<Pair<List<Object>, Long>> getSamples() {
        return new ArrayList<>();
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
                    CloseableIterable<StructLike> rows = task.asDataTask().rows();
                    for (StructLike row : rows) {
                        // Get the last updated time of the table according to the table schema
                        long lastUpdated = -1;
                        try {
                            lastUpdated = row.get(7, Long.class);
                        } catch (NullPointerException e) {
                            // It is a known issue but we do not hanle it right now. If the refresh frequency of 
                            // the materialized view is very high, an excessive number of error logs will be printed. 
                            // Therefore, only brief logs are printed now.
                            logger.error("The table [{}] snapshot [{}] has been expired", nativeTable.name(), snapshotId);
                        }
                        partition = new Partition(lastUpdated);
                        break;
                    }
                }
                if (partition == null) {
                    partition = new Partition(-1);
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
                    CloseableIterable<StructLike> rows = task.asDataTask().rows();
                    for (StructLike row : rows) {
                        // Get the partition data/spec id/last updated time according to the table schema
                        StructProjection partitionData = row.get(0, StructProjection.class);
                        int specId = row.get(1, Integer.class);
                        PartitionSpec spec = nativeTable.specs().get(specId);

                        String partitionName =
                                PartitionUtil.convertIcebergPartitionToPartitionName(spec, partitionData);

                        long lastUpdated = -1;
                        try {
                            lastUpdated = row.get(9, Long.class);
                        } catch (NullPointerException e) {
                            logger.error("The table [{}.{}] snapshot [{}] has been expired", nativeTable.name(), partitionName,
                                    snapshotId, e);
                        }
                        Partition partition = new Partition(lastUpdated);
                        partitionMap.put(partitionName, partition);
                    }
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + nativeTable.name(), e);
            }
        }
        return partitionMap;
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
