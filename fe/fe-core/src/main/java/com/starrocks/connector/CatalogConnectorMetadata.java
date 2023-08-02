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

package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.catalog.system.information.InfoSchemaDb.isInfoSchemaDb;
import static java.util.Objects.requireNonNull;

// CatalogConnectorMetadata provides a uniform interface to provide normal tables and information schema tables.
// Most of its method implementations delegate to the wrapped metadata.
// Only used for connector metadata of external catalog.
public class CatalogConnectorMetadata implements ConnectorMetadata {
    private final ConnectorMetadata metadata;
    private final InfoSchemaDb infoSchemaDb;

    public static CatalogConnectorMetadata wrapInfoSchema(ConnectorMetadata metadata, InfoSchemaDb infoSchemaDb) {
        return new CatalogConnectorMetadata(metadata, infoSchemaDb);
    }

    public CatalogConnectorMetadata(ConnectorMetadata metadata, InfoSchemaDb infoSchemaDb) {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(infoSchemaDb, "infoSchemaDb is null");
        checkArgument(!(metadata instanceof CatalogConnectorMetadata),
                "wrapped metadata is CatalogConnectorMetadata");
        this.metadata = metadata;
        this.infoSchemaDb = infoSchemaDb;
    }

    @Override
    public List<String> listDbNames() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        return builder.addAll(metadata.listDbNames())
                .add(InfoSchemaDb.DATABASE_NAME)
                .build();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        if (isInfoSchemaDb(dbName)) {
            return infoSchemaDb.getTables().stream().map(Table::getName).collect(Collectors.toList());
        }
        return metadata.listTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        return metadata.listPartitionNames(databaseName, tableName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        return metadata.listPartitionNamesByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        if (isInfoSchemaDb(dbName)) {
            return infoSchemaDb.getTable(tblName);
        }
        return metadata.getTable(dbName, tblName);
    }

    @Override
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        return metadata.getMaterializedViewIndex(dbName, tblName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys, long snapshotId,
                                                   ScalarOperator predicate, List<String> fieldNames) {
        return metadata.getRemoteFileInfos(table, partitionKeys, snapshotId, predicate, fieldNames);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return metadata.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate) {
        return metadata.getTableStatistics(session, table, columns, partitionKeys, predicate);
    }

    @Override
    public void clear() {
        metadata.clear();
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        metadata.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
        metadata.createDb(dbName);
    }

    @Override
    public boolean dbExists(String dbName) {
        return isInfoSchemaDb(dbName) || metadata.dbExists(dbName);
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        metadata.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        metadata.dropDb(dbName, isForceDrop);
    }

    @Override
    public Database getDb(long dbId) {
        if (dbId == SystemId.INFORMATION_SCHEMA_DB_ID) {
            return infoSchemaDb;
        }
        return metadata.getDb(dbId);
    }

    @Override
    public Database getDb(String name) {
        if (isInfoSchemaDb(name)) {
            return infoSchemaDb;
        }
        return metadata.getDb(name);
    }

    @Override
    public List<Long> getDbIds() {
        List<Long> dbIds = metadata.getDbIds();
        dbIds.add(infoSchemaDb.getId());
        return dbIds;
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return metadata.createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        metadata.dropTable(stmt);
    }

    @Override
    public void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
        metadata.finishSink(dbName, table, commitInfos);
    }

    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        metadata.alterTable(stmt);
    }

    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        metadata.renameTable(db, table, tableRenameClause);
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        metadata.alterTableComment(db, table, clause);
    }

    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        metadata.truncateTable(truncateTableStmt);
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        metadata.createTableLike(stmt);
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        metadata.addPartitions(db, tableName, addPartitionClause);
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        metadata.dropPartition(db, table, clause);
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        metadata.renamePartition(db, table, renameClause);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt) throws AnalysisException, DdlException {
        metadata.createMaterializedView(stmt);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {
        metadata.createMaterializedView(statement);
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        metadata.dropMaterializedView(stmt);
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException, AnalysisException {
        metadata.alterMaterializedView(stmt);
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        return metadata.refreshMaterializedView(refreshMaterializedViewStatement);
    }

    @Override
    public void cancelRefreshMaterializedView(String dbName, String mvName) throws DdlException, MetaNotFoundException {
        metadata.cancelRefreshMaterializedView(dbName, mvName);
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        metadata.createView(stmt);
    }

    @Override
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        metadata.alterView(stmt);
    }
}
