// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
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
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

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
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @return a list of partition names
     */
    default List<String> listPartitionNames(String databaseName, String tableName) {
        return Lists.newArrayList();
    }

    /**
     * Get Table descriptor for the table specific by `dbName`.`tblName`
     *
     * @param dbName  - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    default Table getTable(String dbName, String tblName) {
        return null;
    }

    /**
     * Get the remote file information from hdfs or s3. It is mainly used to generate ScanRange for scheduling.
     * @param table
     * @param partitionKeys selected columns
     * @return the remote file information of the query to scan.
     */
    default List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys) {
        return Lists.newArrayList();
    }

    default List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList();
    }

    /**
     * Get statistics for the table.
     * @param session optimizer context
     * @param table
     * @param columns selected columns
     * @param partitionKeys selected partition keys
     * @return the table statistics for the table.
     */
    default Statistics getTableStatistics(OptimizerContext session,
                                          Table table,
                                          List<ColumnRefOperator> columns,
                                          List<PartitionKey> partitionKeys) {
        return Statistics.builder().build();
    }

    /**
     * Clean the query level cache after the query.
     */
    default void clear() {
    }

    default void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
    }

    default void createDb(String dbName) throws DdlException, AlreadyExistsException {
    }

    default void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
    }

    default List<Long> getDbIds() {
        return Lists.newArrayList();
    }

    default Database getDb(String name) {
        return null;
    }

    default Database getDb(long dbId) {
        return null;
    }

    default void dropTable(DropTableStmt stmt) throws DdlException {
    }

    default void alterTable(AlterTableStmt stmt) throws UserException {
    }

    default boolean createTable(CreateTableStmt stmt) throws DdlException {
        return true;
    }

    default void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
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

<<<<<<< HEAD
    default String refreshMaterializedView(String dbName, String mvName, boolean force, PartitionRangeDesc range,
                                           int priority, boolean mergeRedundant, boolean isManual)
            throws DdlException, MetaNotFoundException {
        return null;
    }

    default String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement,
                                           int priority)
=======
    default String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
>>>>>>> 2.5.18
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

}

