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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
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
import static java.util.Objects.requireNonNull;

public class UnifiedMetadata implements ConnectorMetadata {
    public static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    public static final String SPARK_TABLE_PROVIDER_KEY = "spark.sql.sources.provider";
    public static final String DELTA_LAKE_PROVIDER = "delta";

    static boolean isIcebergTable(Map<String, String> properties) {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(properties.get(ICEBERG_TABLE_TYPE_NAME));
    }

    static boolean isDeltaLakeTable(Map<String, String> properties) {
        return DELTA_LAKE_PROVIDER.equalsIgnoreCase(properties.get(SPARK_TABLE_PROVIDER_KEY));
    }

    private final Map<Table.TableType, ConnectorMetadata> metadataMap;
    private final HiveMetadata hiveMetadata; // criterion to determine table type

    public UnifiedMetadata(Map<Table.TableType, ConnectorMetadata> metadataMap) {
        requireNonNull(metadataMap, "metadataMap is null");
        checkArgument(metadataMap.containsKey(HIVE), "metadataMap does not have hive metadata");
        this.metadataMap = metadataMap;
        this.hiveMetadata = (HiveMetadata) metadataMap.get(HIVE);
    }

    private Table.TableType getTableType(String dbName, String tblName) {
        Table table = hiveMetadata.getTable(dbName, tblName);
        if (table == null) {
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
        return metadataMap.get(type);
    }

    @Override
    public List<String> listDbNames() {
        return hiveMetadata.listDbNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return hiveMetadata.listTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNames(databaseName, tableName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNamesByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tblName);
        return metadata.getTable(dbName, tblName);
    }

    @Override
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tblName);
        return metadata.getMaterializedViewIndex(dbName, tblName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys, long snapshotId,
                                                   ScalarOperator predicate, List<String> fieldNames) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getRemoteFileInfos(table, partitionKeys, snapshotId, predicate, fieldNames);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getTableStatistics(session, table, columns, partitionKeys, predicate);
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
    public boolean dbExists(String dbName) {
        return hiveMetadata.dbExists(dbName);
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        hiveMetadata.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        hiveMetadata.dropDb(dbName, isForceDrop);
    }

    @Override
    public Database getDb(long dbId) {
        return hiveMetadata.getDb(dbId);
    }

    @Override
    public Database getDb(String name) {
        return hiveMetadata.getDb(name);
    }

    @Override
    public List<Long> getDbIds() {
        return hiveMetadata.getDbIds();
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        if (stmt.getEngineName() == null) {
            throw new DdlException("create table in unified catalog requires a using engine clause");
        }

        Table.TableType type = Table.TableType.deserialize(stmt.getEngineName());
        return metadataMap.get(type).createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(stmt.getDbName(), stmt.getTableName());
        metadata.dropTable(stmt);
    }

    @Override
    public void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
        ConnectorMetadata metadata = metadataOfTable(dbName, table);
        metadata.finishSink(dbName, table, commitInfos);
    }

    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        ConnectorMetadata metadata = metadataOfTable(stmt.getTbl().getDb(), stmt.getTbl().getTbl());
        metadata.alterTable(stmt);
    }

    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.renameTable(db, table, tableRenameClause);
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.alterTableComment(db, table, clause);
    }

    @Override
    public void truncateTable(TruncateTableStmt stmt) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(stmt.getDbName(), stmt.getTblName());
        metadata.truncateTable(stmt);
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        ConnectorMetadata metadata = metadataOfTable(db.getFullName(), tableName);
        metadata.addPartitions(db, tableName, addPartitionClause);
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.dropPartition(db, table, clause);
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause clause) throws DdlException {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.renamePartition(db, table, clause);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt) throws AnalysisException, DdlException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException, AnalysisException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void cancelRefreshMaterializedView(String dbName, String mvName) throws DdlException, MetaNotFoundException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        throw new DdlException("unsupported operation in unified connector");
    }

    @Override
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        throw new DdlException("unsupported operation in unified connector");
    }
}