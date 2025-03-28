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

package com.starrocks.sql.common;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.external.starrocks.TableMetaSyncer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetaUtils {

    private static final Logger LOG = LogManager.getLogger(MVUtils.class);

    public static void checkCatalogExistAndReport(String catalogName) {
        if (catalogName == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, "");
        }
        if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
        }
    }

    public static void checkDbNullAndReport(Database db, String name) {
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, name);
        }
    }

    public static void checkNotSupportCatalog(String catalogName, String operation) {
        if (catalogName == null) {
            throw new SemanticException("Catalog is null");
        }
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return;
        }
        if (operation == null) {
            throw new SemanticException("operation is null");
        }

        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        if (catalog == null) {
            throw new SemanticException("Catalog %s is not found", catalogName);
        }

        if (!operation.equals("ALTER") && catalog.getType().equalsIgnoreCase("iceberg")) {
            throw new SemanticException("Table of iceberg catalog doesn't support [%s]", operation);
        }
    }

    // get table by tableName, unlike getTable, this interface is session aware,
    // which means if there is a temporary table with the same name,
    // use temporary table first, otherwise, treat it as a permanent table
    public static Table getSessionAwareTable(ConnectContext session, Database database, TableName tableName) {
        if (Strings.isNullOrEmpty(tableName.getCatalog())) {
            tableName.setCatalog(session.getCurrentCatalog());
        }
        if (database == null) {
            if (Strings.isNullOrEmpty(tableName.getCatalog())) {
                tableName.setCatalog(session.getCurrentCatalog());
            }
            database = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(session, tableName.getCatalog(), tableName.getDb());
            if (database == null) {
                throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
            }
        }

        Table table = session.getGlobalStateMgr().getMetadataMgr().getTemporaryTable(
                session.getSessionId(), tableName.getCatalog(), database.getId(), tableName.getTbl());
        if (table != null) {
            return table;
        }
        table = session.getGlobalStateMgr().getMetadataMgr().getTable(
                session, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.getTbl());
        }
        return table;
    }

    public static Map<String, Expr> parseColumnNameToDefineExpr(OriginStatement originStmt) {
        CreateMaterializedViewStmt stmt;

        try {
            List<StatementBase> stmts = SqlParser.parse(originStmt.originStmt, SqlModeHelper.MODE_DEFAULT);
            stmt = (CreateMaterializedViewStmt) stmts.get(originStmt.idx);
            stmt.setIsReplay(true);
            return stmt.parseDefineExprWithoutAnalyze(originStmt.originStmt);
        } catch (Exception e) {
            LOG.warn("error happens when parsing create materialized view stmt [{}] use new parser",
                    originStmt, e);
        }

        // suggestion
        LOG.warn("The materialized view [{}] has encountered compatibility problems. " +
                        "It is best to delete the materialized view and rebuild it to maintain the best compatibility.",
                originStmt.originStmt);
        return Maps.newConcurrentMap();
    }

    public static String genInsertLabel(TUniqueId executionId) {
        return "insert_" + DebugUtil.printId(executionId);
    }

    public static String genDeleteLabel(TUniqueId executionId) {
        return "delete_" + DebugUtil.printId(executionId);
    }

    public static String genUpdateLabel(TUniqueId executionId) {
        return "update_" + DebugUtil.printId(executionId);
    }

    public static ExternalOlapTable syncOLAPExternalTableMeta(ExternalOlapTable externalOlapTable) {
        ExternalOlapTable copiedTable = new ExternalOlapTable();
        externalOlapTable.copyOnlyForQuery(copiedTable);
        new TableMetaSyncer().syncTable(copiedTable);
        return copiedTable;
    }

    public static boolean isPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
        Database db = stateMgr.getLocalMetastore().getDb(dbId);
        if (db == null) {
            return false;
        }
        // lake table or lake materialized view
        OlapTable table = (OlapTable) stateMgr.getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            return table.getPartition(partitionId) != null;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    public static boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
        Database db = stateMgr.getLocalMetastore().getDb(dbId);
        if (db == null) {
            return false;
        }
        // lake table or lake materialized view
        OlapTable table = (OlapTable) stateMgr.getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            return false;
        }
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            return table.getPhysicalPartition(partitionId) != null;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    public static List<Column> getColumnsByColumnIds(Table table, List<ColumnId> ids) {
        return getColumnsByColumnIds(table.getIdToColumn(), ids);
    }

    public static List<Column> getColumnsByColumnIds(Map<ColumnId, Column> idToColumn, List<ColumnId> ids) {
        List<Column> result = new ArrayList<>(ids.size());
        for (ColumnId columnId : ids) {
            Column column = idToColumn.get(columnId);
            if (column == null) {
                throw new SemanticException(String.format("can not find column by column id: %s", columnId));
            }
            result.add(column);
        }
        return result;
    }

    public static Map<ColumnId, Column> buildIdToColumn(List<Column> schema) {
        Map<ColumnId, Column> result = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        for (Column column : schema) {
            result.put(column.getColumnId(), column);
        }
        return result;
    }

    public static List<String> getColumnNamesByColumnIds(Table table, List<ColumnId> columnIds) {
        return getColumnNamesByColumnIds(table.getIdToColumn(), columnIds);
    }

    public static List<String> getColumnNamesByColumnIds(Map<ColumnId, Column> idToColumn, List<ColumnId> columnIds) {
        List<String> names = new ArrayList<>(columnIds.size());
        for (ColumnId columnId : columnIds) {
            Column column = idToColumn.get(columnId);
            if (column == null) {
                throw new SemanticException(String.format("can not find column by column id: %s", columnId));
            }
            names.add(column.getName());
        }
        return names;
    }

    public static Column getColumnByColumnName(long dbId, long tableId, String columnName) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableId);
        }

        Column column = table.getColumn(columnName);
        if (column == null) {
            throw new SemanticException(String.format("can not find column by name: %s", columnName));
        }
        return column;
    }

    public static Map<String, Column> buildNameToColumn(List<Column> schema) {
        Map<String, Column> result = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : schema) {
            result.put(column.getName(), column);
        }
        return result;
    }

    public static String getColumnNameByColumnId(long dbId, long tableId, ColumnId columnId) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableId);
        }
        Column column = table.getColumn(columnId);
        if (column == null) {
            throw new SemanticException(String.format("can not find column by column id: %s", columnId));
        }
        return column.getName();
    }

    public static List<ColumnId> getColumnIdsByColumnNames(Table table, List<String> names) {
        List<ColumnId> columnIds = new ArrayList<>(names.size());
        for (String name : names) {
            Column column = table.getColumn(name);
            if (column == null) {
                throw new SemanticException(String.format("can not find column by name: %s, from table: %s",
                        name, table.getName()));
            }
            columnIds.add(column.getColumnId());
        }
        return columnIds;
    }
}
