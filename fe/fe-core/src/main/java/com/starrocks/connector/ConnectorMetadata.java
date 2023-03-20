// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;

import java.util.List;

public interface ConnectorMetadata {
    /**
     * List all database names of connector
     *
     * @return a list of string containing all database names of connector
     */
    default List<String> listDbNames() throws DdlException {
        return Lists.newArrayList();
    }

    /**
     * List all table names of the database specific by `dbName`
     *
     * @param dbName - the string of which all table names are listed
     * @return a list of string containing all table names of `dbName`
     */
    default List<String> listTableNames(String dbName) throws DdlException {
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

    default void createDb(CreateDbStmt stmt) throws DdlException {}

    default void dropDb(DropDbStmt stmt) throws DdlException {}

    default List<Long> getDbIds() {
        return Lists.newArrayList();
    }

    default Database getDb(String name) {
        return null;
    }

    default Database getDb(long dbId) {
        return null;
    }

    default void dropTable(DropTableStmt stmt) throws DdlException {}

    default void alterTable(AlterTableStmt stmt) throws UserException {}

    default boolean createTable(CreateTableStmt stmt) throws DdlException {
        return true;
    }

    default void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {}

    default void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {}

    default void createTableLike(CreateTableLikeStmt stmt) throws DdlException {}

    default void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {}

    default void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {}

    default void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {}

    default void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {}

    default void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {}

    default void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {}

    default void createView(CreateViewStmt stmt) throws DdlException {}

    default void alterView(AlterViewStmt stmt) throws DdlException, UserException {}

}

