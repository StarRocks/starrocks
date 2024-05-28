// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.toHivePartitionName;

public class DatabricksUnityMetastore implements IMetastore {
    private static final Logger LOG = LogManager.getLogger(DatabricksUnityMetastore.class);
    public static final String DATABRICKS_HOST = "databricks.host";
    public static final String DATABRICKS_TOKEN = "databricks.token";
    public static final String DATABRICKS_CATALOG_NAME = "databricks.catalog.name";

    private final String catalogName;
    private final String databricksCatalogName;

    private final WorkspaceClient workspaceClient;
    private final HdfsEnvironment hdfsEnvironment;

    public DatabricksUnityMetastore(String catalogName, String databricksCatalogName,
                                    WorkspaceClient workspaceClient,
                                    HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.databricksCatalogName = databricksCatalogName;
        this.workspaceClient = workspaceClient;
        this.hdfsEnvironment = hdfsEnvironment;
    }

    public List<String> getAllDatabaseNames() {
        List<String> dbNames = Lists.newArrayList();
        try {
            dbNames = Streams.stream(workspaceClient.schemas().list(databricksCatalogName).iterator()).
                    map(SchemaInfo::getName).collect(Collectors.toList());
        } catch (NullPointerException e) {
            LOG.warn("Null pointer exception when get all databases from {} catalog", databricksCatalogName);
        } catch (Exception e) {
            LOG.error("Catalog {} get all databases failed", databricksCatalogName, e);
            throw e;
        }
        return dbNames;
    }

    public List<String> getAllTableNames(String dbName) {
        List<String> tableNames = Lists.newArrayList();
        try {
            tableNames = Streams.stream(workspaceClient.tables().list(databricksCatalogName, dbName).iterator()).
                    filter(tableInfo -> tableInfo.getTableType().equals(TableType.MANAGED)).
                    map(TableInfo::getName).collect(Collectors.toList());
        } catch (NullPointerException e) {
            // empty database will throw null pointer exception, catch here and return empty list
            LOG.warn("Null pointer exception when get all tables from {}.{}", databricksCatalogName, dbName);
        } catch (Exception e) {
            LOG.error("Database {}.{} get all tables failed", databricksCatalogName, dbName, e);
            throw e;
        }
        return tableNames;
    }

    public Database getDb(String dbName) {
        SchemaInfo schemaInfo = workspaceClient.schemas().get(databricksCatalogName + "." + dbName);
        if (schemaInfo == null) {
            throw new StarRocksConnectorException("Databricks database [%s] doesn't exist", dbName);
        }
        return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), schemaInfo.getName(),
                schemaInfo.getStorageLocation());
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        String fullName = Joiner.on(".").join(databricksCatalogName, dbName, tableName);
        TableInfo tableInfo = workspaceClient.tables().get(fullName);
        if (tableInfo == null) {
            return null;
        }
        if (!tableInfo.getTableType().equals(TableType.MANAGED)) {
            return null;
        }
        String path = tableInfo.getStorageLocation();
        long createTime = tableInfo.getCreatedAt();
        return new MetastoreTable(dbName, tableName, path, createTime);
    }

    public DeltaLakeTable getTable(String dbName, String tblName) {
        try {
            String fullName = Joiner.on(".").join(databricksCatalogName, dbName, tblName);
            TableInfo tableInfo = workspaceClient.tables().get(fullName);
            if (tableInfo == null) {
                return null;
            }
            if (!tableInfo.getTableType().equals(TableType.MANAGED)) {
                return null;
            }
            String path = tableInfo.getStorageLocation();
            long createTime = tableInfo.getCreatedAt();
            return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, tblName, path,
                    hdfsEnvironment.getConfiguration(), createTime);
        } catch (Exception e) {
            LOG.error("Failed to get table {}.{}.{}", catalogName, dbName, tblName, e);
            return null;
        }
    }

    public boolean tableExists(String dbName, String tblName) {
        String fullName = Joiner.on(".").join(databricksCatalogName, dbName, tblName);
        TableInfo tableInfo = workspaceClient.tables().get(fullName);
        return tableInfo != null;
    }

    public List<String> getPartitionKeys(String dbName, String tblName) {
        DeltaLakeTable deltaLakeTable = getTable(dbName, tblName);
        if (deltaLakeTable == null) {
            LOG.error("Table {}.{}.{} doesn't exist", catalogName, dbName, tblName);
            return Lists.newArrayList();
        }

        List<String> partitionKeys = Lists.newArrayList();
        Engine deltaEngine = deltaLakeTable.getDeltaEngine();
        List<String> partitionColumnNames = deltaLakeTable.getPartitionColumnNames();

        ScanBuilder scanBuilder = deltaLakeTable.getDeltaSnapshot().getScanBuilder(deltaEngine);
        Scan scan = scanBuilder.build();
        try (CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = scan.getScanFiles(deltaEngine)) {
            while (scanFilesAsBatches.hasNext()) {
                FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();

                try (CloseableIterator<Row> scanFileRows = scanFileBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        Map<String, String> partitionValueMap = InternalScanFileUtils.getPartitionValues(scanFileRow);
                        List<String> partitionValues =
                                partitionColumnNames.stream().map(partitionValueMap::get).collect(
                                        Collectors.toList());
                        String partitionName = toHivePartitionName(partitionColumnNames, partitionValues);
                        partitionKeys.add(partitionName);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get partition keys for table {}.{}.{}", catalogName, dbName, tblName, e);
            throw new StarRocksConnectorException(String.format("Failed to get partition keys for table %s.%s.%s",
                    catalogName, dbName, tblName), e);
        }

        return partitionKeys;
    }
}