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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public interface ConnectorMetadata {
    /**
     * List all database names of connector
     *
     * @return a list of string containing all database names of connector
     */
    default List<String> listDbNames() {
        return Lists.newArrayList();
    }

    /**
     * List all table names of the database specific by `dbName`
     *
     * @param dbName - the string of which all table names are listed
     * @return a list of string containing all table names of `dbName`
     */
    default List<String> listTableNames(String dbName) {
        return Lists.newArrayList();
    }

    /**
     * Return all partition names of the table.
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @return a list of partition names
     */
    default List<String> listPartitionNames(String databaseName, String tableName) {
        return Lists.newArrayList();
    }

    /**
     * Return partial partition names of the table using partitionValues to filter.
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
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
     * @param dbName - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    default Table getTable(String dbName, String tblName) {
        return null;
    }

    /**
     * Get Table descriptor and materialized index for the materialized view index specific by `dbName`.`tblName`
     *
     * @param dbName - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    default Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        return null;
    }

    /**
     * It is mainly used to generate ScanRange for scheduling.
     * There are two ways of current connector table.
     * 1. Get the remote files information from hdfs or s3 according to table or partition.
     * 2. Get file scan tasks for iceberg metadata by query predicate.
     *
     * @param table
     * @param partitionKeys selected partition columns
     * @param snapshotId selected snapshot id
     * @param predicate used to filter metadata for iceberg, etc
     * @param fieldNames all selected columns (including partition columns)
     * @return the remote file information of the query to scan.
     */
    default List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                    long snapshotId, ScalarOperator predicate, List<String> fieldNames) {
        return Lists.newArrayList();
    }

    default List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList();
    }

    /**
     * Get statistics for the table.
     *
     * @param session optimizer context
     * @param table
     * @param columns selected columns
     * @param partitionKeys selected partition keys
     * @param predicate used to filter metadata for iceberg, etc
     * @return the table statistics for the table.
     */
    default Statistics getTableStatistics(OptimizerContext session,
                                          Table table,
                                          Map<ColumnRefOperator, Column> columns,
                                          List<PartitionKey> partitionKeys,
                                          ScalarOperator predicate) {
        return Statistics.builder().build();
    }

<<<<<<< HEAD
=======
    default boolean prepareMetadata(MetaPreparationItem item, Tracers tracers) {
        return true;
    }

    default List<PartitionKey> getPrunedPartitions(Table table, ScalarOperator predicate, long limit) {
        throw new StarRocksConnectorException("This connector doesn't support pruning partitions");
    }

>>>>>>> 26916e8ce9 ([Enhancement] parallel prepare  metadata (#40280))
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

    default boolean dbExists(String dbName) {
        return listDbNames().contains(dbName.toLowerCase(Locale.ROOT));
    }

    default void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        throw new StarRocksConnectorException("This connector doesn't support creating databases");
    }

    default void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        throw new StarRocksConnectorException("This connector doesn't support dropping databases");
    }

    default Database getDb(long dbId) {
        return null;
    }

    default Database getDb(String name) {
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

    default void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
        throw new StarRocksConnectorException("This connector doesn't support sink");
    }

    default void alterTable(AlterTableStmt stmt) throws UserException {
    }

    default void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
    }

    default void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
    }

    default void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
    }

    default void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
    }

    default void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
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

    default void cancelRefreshMaterializedView(String dbName, String mvName)
            throws DdlException, MetaNotFoundException {
    }

    default void createView(CreateViewStmt stmt) throws DdlException {
    }

    default void alterView(AlterViewStmt stmt) throws DdlException, UserException {
    }

    default CloudConfiguration getCloudConfiguration() {
        throw new StarRocksConnectorException("This connector doesn't support getting cloud configuration");
    }
}

