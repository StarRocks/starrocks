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
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.credential.CloudConfiguration;
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
import static com.starrocks.catalog.system.information.InfoSchemaDb.isInfoSchemaDb;
import static java.util.Objects.requireNonNull;

// CatalogConnectorMetadata provides a uniform interface to provide normal tables and information schema tables.
// The database name/id is used to route request to specific metadata.
public class CatalogConnectorMetadata implements ConnectorMetadata {
    private final ConnectorMetadata normal;
    private final ConnectorMetadata informationSchema;

    public CatalogConnectorMetadata(ConnectorMetadata normal, ConnectorMetadata informationSchema) {
        requireNonNull(normal, "metadata is null");
        requireNonNull(informationSchema, "infoSchemaDb is null");
        checkArgument(informationSchema instanceof InformationSchemaMetadata);
        this.normal = normal;
        this.informationSchema = informationSchema;
    }

    private ConnectorMetadata metadataOfDb(String dBName) {
        if (isInfoSchemaDb(dBName)) {
            return informationSchema;
        }
        return normal;
    }

    @Override
    public List<String> listDbNames() {
        return ImmutableList.<String>builder()
                .addAll(this.normal.listDbNames())
                .addAll(this.informationSchema.listDbNames())
                .build();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        ConnectorMetadata metadata = metadataOfDb(dbName);
        return metadata.listTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        return normal.listPartitionNames(databaseName, tableName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        return normal.listPartitionNamesByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfDb(dbName);
        return metadata.getTable(dbName, tblName);
    }

    @Override
    public Table getSystemTable(String dbName, String tblName, String tableTypeString) {
        ConnectorMetadata metadata = metadataOfDb(dbName);
        return metadata.getSystemTable(dbName, tblName, tableTypeString);
    }

    @Override
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        return normal.getMaterializedViewIndex(dbName, tblName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys, long snapshotId,
                                                   ScalarOperator predicate, List<String> fieldNames, long limit) {
        return normal.getRemoteFileInfos(table, partitionKeys, snapshotId, predicate, fieldNames, limit);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return normal.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit) {
        return normal.getTableStatistics(session, table, columns, partitionKeys, predicate, limit);
    }

    @Override
    public List<PartitionKey> getPrunedPartitions(Table table, ScalarOperator predicate, long limit) {
        return normal.getPrunedPartitions(table, predicate, limit);
    }

    @Override
    public void clear() {
        normal.clear();
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        normal.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
        normal.createDb(dbName);
    }

    @Override
    public boolean dbExists(String dbName) {
        ConnectorMetadata metadata = metadataOfDb(dbName);
        return metadata.dbExists(dbName);
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        normal.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        normal.dropDb(dbName, isForceDrop);
    }

    @Override
    public Database getDb(String name) {
        ConnectorMetadata metadata = metadataOfDb(name);
        return metadata.getDb(name);
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return normal.createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        normal.dropTable(stmt);
    }

    @Override
    public void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
        normal.finishSink(dbName, table, commitInfos);
    }

    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        normal.alterTable(stmt);
    }

    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        normal.renameTable(db, table, tableRenameClause);
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        normal.alterTableComment(db, table, clause);
    }

    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        normal.truncateTable(truncateTableStmt);
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        normal.createTableLike(stmt);
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        normal.addPartitions(db, tableName, addPartitionClause);
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        normal.dropPartition(db, table, clause);
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        normal.renamePartition(db, table, renameClause);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt) throws AnalysisException, DdlException {
        normal.createMaterializedView(stmt);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {
        normal.createMaterializedView(statement);
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        normal.dropMaterializedView(stmt);
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException, AnalysisException {
        normal.alterMaterializedView(stmt);
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        return normal.refreshMaterializedView(refreshMaterializedViewStatement);
    }

    @Override
    public void cancelRefreshMaterializedView(String dbName, String mvName) throws DdlException, MetaNotFoundException {
        normal.cancelRefreshMaterializedView(dbName, mvName);
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        normal.createView(stmt);
    }

    @Override
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        normal.alterView(stmt);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return normal.getCloudConfiguration();
    }
}
