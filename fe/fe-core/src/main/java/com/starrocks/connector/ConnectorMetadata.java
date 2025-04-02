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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ConnectorMetadata {
    /**
     * Use connector type as a hint of table type.
     * Caveat: there are exceptions that hive connector may have non-hive(e.g. iceberg) tables.
     */
    default Table.TableType getTableType() {
        throw new StarRocksConnectorException("This connector doesn't support getting table type");
    }

    /**
     * List all database names of connector
     *
     * @return a list of string containing all database names of connector
     */
    default List<String> listDbNames(ConnectContext context) {
        return Lists.newArrayList();
    }

    /**
     * List all table names of the database specific by `dbName`
     *
     * @param dbName - the string of which all table names are listed
     * @return a list of string containing all table names of `dbName`
     */
    default List<String> listTableNames(ConnectContext context, String dbName) {
        return Lists.newArrayList();
    }

    /**
     * Return all partition names of the table.
     *
     * @param databaseName   the name of the database
     * @param tableName      the name of the table
     * @param requestContext request context
     * @return a list of partition names
     */
    default List<String> listPartitionNames(String databaseName, String tableName,
                                            ConnectorMetadatRequestContext requestContext) {
        return Lists.newArrayList();
    }

    /**
     * Return partial partition names of the table using partitionValues to filter.
     *
     * @param databaseName    the name of the database
     * @param tableName       the name of the table
     * @param partitionValues the partition value to filter
     * @return a list of partition names
     */
    default List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                   List<Optional<String>> partitionValues) {
        return Lists.newArrayList();
    }

    /**
     * Get Table descriptor for the table specific by `dbName`.`tblName`
     *
     * @param dbName  - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    default Table getTable(ConnectContext context, String dbName, String tblName) {
        return null;
    }

    default TableVersionRange getTableVersionRange(String dbName, Table table,
                                                   Optional<ConnectorTableVersion> startVersion,
                                                   Optional<ConnectorTableVersion> endVersion) {
        return TableVersionRange.empty();
    }

    default boolean tableExists(ConnectContext context, String dbName, String tblName) {
        return listTableNames(context, dbName).contains(tblName);
    }

    /**
     * It is mainly used to generate ScanRange for scheduling.
     * There are two ways of current connector table.
     * 1. Get the remote files information from hdfs or s3 according to table or partition.
     * 2. Get file scan tasks for iceberg/deltalake metadata by query predicate.
     * <p>
     * There is an implicit contract here:
     * the order of remote file information in the list, must be identical to order of partition keys in params.
     *
     * @return the remote file information of the query to scan.
     */
    default List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        return Lists.newArrayList();
    }

    default RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        return RemoteFileInfoDefaultSource.EMPTY;
    }

    default List<PartitionInfo> getRemotePartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList();
    }

    /**
     * Get table meta serialized specification
     *
     * @param dbName
     * @param tableName
     * @param snapshotId
     * @param serializedPredicate serialized predicate string of lake format expression
     * @param metadataTableType   metadata table type
     * @return table meta serialized specification
     */
    default SerializedMetaSpec getSerializedMetaSpec(String dbName, String tableName, long snapshotId,
                                                     String serializedPredicate, MetadataTableType metadataTableType) {
        return null;
    }

    default List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList();
    }

    /**
     * Get statistics for the table.
     *
     * @param session           optimizer context
     * @param table
     * @param columns           selected columns
     * @param partitionKeys     selected partition keys
     * @param predicate         used to filter metadata for iceberg, etc
     * @param limit             scan limit if needed, default value is -1
     * @param tableVersionRange table version range in the query
     * @return the table statistics for the table.
     */
    default Statistics getTableStatistics(OptimizerContext session,
                                          Table table,
                                          Map<ColumnRefOperator, Column> columns,
                                          List<PartitionKey> partitionKeys,
                                          ScalarOperator predicate,
                                          long limit,
                                          TableVersionRange tableVersionRange) {
        return Statistics.builder().build();
    }

    default boolean prepareMetadata(MetaPreparationItem item, Tracers tracers, ConnectContext connectContext) {
        return true;
    }

    // return true if the connector has self info schema
    default boolean hasSelfInfoSchema() {
        return false;
    }

    /**
     * Clean the query level cache after the query.
     */
    default void clear() {
    }

    default void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
    }

    default void createDb(String dbName) throws DdlException, AlreadyExistsException {
        createDb(dbName, new HashMap<>());
    }

    default boolean dbExists(ConnectContext context, String dbName) {
        return listDbNames(context).contains(dbName.toLowerCase(Locale.ROOT));
    }

    default void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        throw new StarRocksConnectorException("This connector doesn't support creating databases");
    }

    default void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        throw new StarRocksConnectorException("This connector doesn't support dropping databases");
    }

    default Database getDb(long dbId) {
        return null;
    }

    default Database getDb(ConnectContext context, String name) {
        return null;
    }

    default List<Long> getDbIds() {
        return Lists.newArrayList();
    }

    default boolean createTable(CreateTableStmt stmt) throws DdlException {
        throw new StarRocksConnectorException("This connector doesn't support creating tables");
    }

    default void dropTable(DropTableStmt stmt) throws DdlException {
        throw new StarRocksConnectorException("This connector doesn't support dropping tables");
    }

    default void dropTemporaryTable(String dbName, long tableId, String tableName, boolean isSetIfExists, boolean isForce)
            throws DdlException {
        throw new StarRocksConnectorException("This connector doesn't support dropping temporary tables");
    }

    default void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos, String branch) {
        throw new StarRocksConnectorException("This connector doesn't support sink");
    }

    default void abortSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
    }

    default void alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        throw new StarRocksConnectorException("This connector doesn't support alter table");
    }

    default void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
    }

    default void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
    }

    default void truncateTable(TruncateTableStmt truncateTableStmt, ConnectContext context) throws DdlException {
    }

    default void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
    }

    default void addPartitions(ConnectContext ctx, Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException {
    }

    default void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
    }

    default void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
    }

    default void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
    }

    default void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {
    }

    default void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
    }

    default void alterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException, AnalysisException {
    }

    default String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        return null;
    }

    default void cancelRefreshMaterializedView(CancelRefreshMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException {
    }

    default void createView(CreateViewStmt stmt) throws DdlException {
        throw new StarRocksConnectorException("This connector doesn't support create view");
    }

    default void alterView(AlterViewStmt stmt) throws StarRocksException {
        throw new StarRocksConnectorException("This connector doesn't support alter view");
    }

    default CloudConfiguration getCloudConfiguration() {
        throw new StarRocksConnectorException("This connector doesn't support getting cloud configuration");
    }

    default Set<DeleteFile> getDeleteFiles(IcebergTable icebergTable, Long snapshotId,
                                           ScalarOperator predicate, FileContent fileContent) {
        throw new StarRocksConnectorException("This connector doesn't support getting delete files");
    }

    default void shutdown() {
    }
}

