package com.starrocks.spi;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
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
import com.starrocks.analysis.RefreshExternalTableStmt;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface Metadata {
    default void createDb(CreateDbStmt stmt) throws DdlException {
    }

    default void dropDb(DropDbStmt stmt) throws DdlException {
    }

    default Database getDb(long dbId) {
        return new Database();
    }

    default Database getDb(String name) {
        return new Database();
    }

    default List<String> listDatabaseNames() throws DdlException {
        return Lists.newArrayList();
    };

    default List<String> listTableNames(String dbName) {
        return Lists.newArrayList();
    }

    default Table getTable(String dbName, String tblName) throws AnalysisException {
        return null;
    }

    default void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
    }

    default void renameDatabase(AlterDatabaseRename stmt) throws DdlException {}

    default void createTable(CreateTableStmt stmt) throws DdlException {}

    default void createTableLike(CreateTableLikeStmt stmt) throws DdlException {}

    default void dropTable(DropTableStmt stmt) throws DdlException {}

    default void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {}

    default void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {}

    default void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {}

    default void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {}

    default List<Long> getDbIds() {
        return Lists.newArrayList();
    }

    default void alterTable(AlterTableStmt stmt) throws UserException {}

    default void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {}

    default void createView(CreateViewStmt stmt) throws DdlException {}

    default void alterView(AlterViewStmt stmt) throws DdlException, UserException {}

    default void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {}

    default void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {}

    default void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {}

    default void modifyTableDynamicPartition(Database db, Table table, Map<String, String> properties)
            throws DdlException {}

    default void changeDb(ConnectContext ctx, String qualifiedDb) throws DdlException {}

    default void refreshExternalTable(RefreshExternalTableStmt stmt) throws DdlException {}

}
