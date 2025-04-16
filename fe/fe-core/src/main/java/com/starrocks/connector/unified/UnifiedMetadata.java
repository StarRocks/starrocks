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

package com.starrocks.connector.unified;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TSinkCommitInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.catalog.Table.TableType.DELTALAKE;
import static com.starrocks.catalog.Table.TableType.HIVE;
import static com.starrocks.catalog.Table.TableType.HUDI;
import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.catalog.Table.TableType.KUDU;
import static com.starrocks.catalog.Table.TableType.PAIMON;
import static java.util.Objects.requireNonNull;

public class UnifiedMetadata implements ConnectorMetadata {
    public static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    public static final String SPARK_TABLE_PROVIDER_KEY = "spark.sql.sources.provider";
    public static final String DELTA_LAKE_PROVIDER = "delta";
    public static final String PAIMON_STORAGE_HANDLER_KEY = "storage_handler";
    public static final String PAIMON_STORAGE_HANDLER_VALUE = "org.apache.paimon.hive.PaimonStorageHandler";

    public static boolean isIcebergTable(Map<String, String> properties) {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(properties.get(ICEBERG_TABLE_TYPE_NAME));
    }

    public static boolean isDeltaLakeTable(Map<String, String> properties) {
        return DELTA_LAKE_PROVIDER.equalsIgnoreCase(properties.get(SPARK_TABLE_PROVIDER_KEY));
    }

    public static boolean isPaimonTable(Map<String, String> properties) {
        return PAIMON_STORAGE_HANDLER_VALUE.equalsIgnoreCase(properties.get(PAIMON_STORAGE_HANDLER_KEY));
    }

    private final Map<Table.TableType, ConnectorMetadata> metadataMap;
    private final HiveMetadata hiveMetadata; // used to determine table type

    public UnifiedMetadata(Map<Table.TableType, ConnectorMetadata> metadataMap) {
        requireNonNull(metadataMap, "metadataMap is null");
        checkArgument(metadataMap.containsKey(HIVE), "metadataMap does not have hive metadata");
        this.metadataMap = metadataMap;
        this.hiveMetadata = (HiveMetadata) metadataMap.get(HIVE);
    }

    private Table.TableType getTableType(String dbName, String tblName) {
        Table table = hiveMetadata.getTable(new ConnectContext(), dbName, tblName);
        if (table == null || table.isHiveView()) {
            return HIVE; // use hive metadata by default
        }
        if (table.isHudiTable()) {
            return HUDI;
        }
        if (isIcebergTable(table.getProperties())) {
            return ICEBERG;
        }
        if (isDeltaLakeTable(table.getProperties())) {
            return DELTALAKE;
        }
        if (isPaimonTable(table.getProperties())) {
            return PAIMON;
        }
        if (table.isKuduTable()) {
            return KUDU;
        }
        return HIVE;
    }

    private Table.TableType getTableType(Table table) {
        return table.getType();
    }

    private ConnectorMetadata metadataOfTable(String dbName, String tblName) {
        Table.TableType type = getTableType(dbName, tblName);
        return metadataMap.get(type);
    }

    private ConnectorMetadata metadataOfTable(Table table) {
        Table.TableType type = getTableType(table);
        if (table.isHiveView()) {
            type = HIVE;
        }
        return metadataMap.get(type);
    }

    @Override
    public Table.TableType getTableType() {
        return HIVE;
    }

    @Override
    public TableVersionRange getTableVersionRange(String dbName, Table table,
                                                  Optional<ConnectorTableVersion> startVersion,
                                                  Optional<ConnectorTableVersion> endVersion) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getTableVersionRange(dbName, table, startVersion, endVersion);
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return hiveMetadata.listDbNames(context);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return hiveMetadata.listTableNames(context, dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, ConnectorMetadatRequestContext requestContext) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNames(databaseName, tableName, requestContext);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNamesByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tblName);
        return metadata.getTable(context, dbName, tblName);
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        return hiveMetadata.tableExists(context, dbName, tblName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getRemoteFiles(table, params);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getRemoteFilesAsync(table, params);
    }

    @Override
    public SerializedMetaSpec getSerializedMetaSpec(String dbName, String tableName,
                                                    long snapshotId, String serializedPredicate, MetadataTableType type) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tableName);
        return metadata.getSerializedMetaSpec(dbName, tableName, snapshotId, serializedPredicate, type);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit,
                                         TableVersionRange version) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getTableStatistics(session, table, columns, partitionKeys, predicate, limit, version);
    }

    @Override
    public boolean prepareMetadata(MetaPreparationItem item, Tracers tracers, ConnectContext connectContext) {
        ConnectorMetadata metadata = metadataOfTable(item.getTable());
        return metadata.prepareMetadata(item, tracers, connectContext);
    }

    @Override
    public void clear() {
        metadataMap.forEach((k, v) -> v.clear());
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
        hiveMetadata.createDb(dbName);
    }

    @Override
    public boolean dbExists(ConnectContext context, String dbName) {
        return hiveMetadata.dbExists(context, dbName);
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        hiveMetadata.createDb(dbName, properties);
    }

    @Override
    public void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        hiveMetadata.dropDb(context, dbName, isForceDrop);
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        return hiveMetadata.getDb(context, name);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        requireNonNull(stmt.getEngineName(), "engine name is null");
        Table.TableType type = Table.TableType.deserialize(stmt.getEngineName().toUpperCase());
        return metadataMap.get(type).createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(stmt.getDbName(), stmt.getTableName());
        metadata.dropTable(stmt);
    }

    @Override
    public void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos, String branch) {
        ConnectorMetadata metadata = metadataOfTable(dbName, table);
        metadata.finishSink(dbName, table, commitInfos, branch);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hiveMetadata.getCloudConfiguration();
    }
}