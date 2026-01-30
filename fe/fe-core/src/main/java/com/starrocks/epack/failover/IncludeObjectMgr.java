// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IncludeObjectMgr {
    private static final Logger LOG = LogManager.getLogger(IncludeObjectMgr.class);

    private static class IncludeCatalog {
        @SerializedName(value = "catalogId")
        private final long catalogId;

        public IncludeCatalog(long catalogId) {
            this.catalogId = catalogId;
        }

        public long getCatalogId() {
            return catalogId;
        }
    }

    private static class IncludeDatabase {
        @SerializedName(value = "catalogId")
        private final long catalogId;

        @SerializedName(value = "databaseId")
        private final long databaseId;

        public IncludeDatabase(long catalogId, long databaseId) {
            this.catalogId = catalogId;
            this.databaseId = databaseId;
        }

        public long getCatalogId() {
            return catalogId;
        }

        public long getDatabaseId() {
            return databaseId;
        }
    }

    private static class IncludeTable {
        @SerializedName(value = "catalogId")
        private final long catalogId;

        @SerializedName(value = "databaseId")
        private final long databaseId;

        @SerializedName(value = "tableId")
        private final long tableId;

        public IncludeTable(long catalogId, long databaseId, long tableId) {
            this.catalogId = catalogId;
            this.databaseId = databaseId;
            this.tableId = tableId;
        }

        public long getCatalogId() {
            return catalogId;
        }

        public long getDatabaseId() {
            return databaseId;
        }

        public long getTableId() {
            return tableId;
        }
    }

    @SerializedName(value = "includeCatalogs")
    private final Map<Long, IncludeCatalog> includeCatalogs = Maps.newConcurrentMap();

    @SerializedName(value = "includeDatabases")
    private final Map<Long, IncludeDatabase> includeDatabases = Maps.newConcurrentMap();

    @SerializedName(value = "includeTables")
    private final Map<Long, IncludeTable> includeTables = Maps.newConcurrentMap();

    public IncludeObjectMgr() {

    }

    public IncludeObjectMgr(CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        List<String> includeCatalogs = stmt.getIncludeCatalogs();
        if (includeCatalogs != null) {
            addIncludeCatalogs(includeCatalogs);
        }

        List<DatabaseName> includeDatabases = stmt.getIncludeDatabases();
        if (includeDatabases != null) {
            addIncludeDatabases(includeDatabases);
        }

        List<TableName> includeTables = stmt.getIncludeTables();
        if (includeTables != null) {
            addIncludeTables(includeTables);
        }
    }

    public IncludeObjectMgr(IncludeObjectMgr other, AlterFailoverGroupSetStmt stmt) throws DdlException {
        List<String> includeCatalogs = stmt.getIncludeCatalogs();
        List<DatabaseName> includeDatabases = stmt.getIncludeDatabases();
        List<TableName> includeTables = stmt.getIncludeTables();

        if (includeCatalogs != null && !includeCatalogs.isEmpty() ||
                includeDatabases != null && !includeDatabases.isEmpty() ||
                includeTables != null && !includeTables.isEmpty()) {
            if (includeCatalogs != null) {
                addIncludeCatalogs(includeCatalogs);
            }
            if (includeDatabases != null) {
                addIncludeDatabases(includeDatabases);
            }
            if (includeTables != null) {
                addIncludeTables(includeTables);
            }
        } else {
            this.includeCatalogs.putAll(other.includeCatalogs);
            this.includeDatabases.putAll(other.includeDatabases);
            this.includeTables.putAll(other.includeTables);
        }
    }

    public IncludeObjectMgr(IncludeObjectMgr other, AlterFailoverGroupAddStmt stmt) throws DdlException {
        this.includeCatalogs.putAll(other.includeCatalogs);
        List<String> includeCatalogs = stmt.getIncludeCatalogs();
        if (includeCatalogs != null) {
            addIncludeCatalogs(includeCatalogs);
        }

        this.includeDatabases.putAll(other.includeDatabases);
        List<DatabaseName> includeDatabases = stmt.getIncludeDatabases();
        if (includeDatabases != null) {
            addIncludeDatabases(includeDatabases);
        }

        this.includeTables.putAll(other.includeTables);
        List<TableName> includeTables = stmt.getIncludeTables();
        if (includeTables != null) {
            addIncludeTables(includeTables);
        }
    }

    public IncludeObjectMgr(IncludeObjectMgr other, AlterFailoverGroupRemoveStmt stmt) throws DdlException {
        this.includeCatalogs.putAll(other.includeCatalogs);
        List<String> includeCatalogs = stmt.getIncludeCatalogs();
        if (includeCatalogs != null) {
            removeIncludeCatalogs(includeCatalogs);
        }

        this.includeDatabases.putAll(other.includeDatabases);
        List<DatabaseName> includeDatabases = stmt.getIncludeDatabases();
        if (includeDatabases != null) {
            removeIncludeDatabases(includeDatabases);
        }

        this.includeTables.putAll(other.includeTables);
        List<TableName> includeTables = stmt.getIncludeTables();
        if (includeTables != null) {
            removeIncludeTables(includeTables);
        }
    }

    private void addIncludeCatalogs(List<String> catalogNames) throws DdlException {
        for (String catalogName : catalogNames) {
            if (CatalogMgr.isExternalCatalog(catalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }

            boolean ret = addIncludeCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID);
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }
        }
    }

    private void removeIncludeCatalogs(List<String> catalogNames) throws DdlException {
        for (String catalogName : catalogNames) {
            if (CatalogMgr.isExternalCatalog(catalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }

            boolean ret = removeIncludeCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID);
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }
        }
    }

    private void addIncludeDatabases(List<DatabaseName> databaseNamess) throws DdlException {
        for (DatabaseName databaseName : databaseNamess) {
            if (CatalogMgr.isExternalCatalog(databaseName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getCatalog());
            }

            Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(databaseName.getDatabase());
            if (database == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, databaseName.getDatabase());
            }

            boolean ret = addIncludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, database.getId());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getDatabase());
            }
        }
    }

    private void removeIncludeDatabases(List<DatabaseName> databaseNamess) throws DdlException {
        for (DatabaseName databaseName : databaseNamess) {
            if (CatalogMgr.isExternalCatalog(databaseName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getCatalog());
            }

            Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(databaseName.getDatabase());
            if (database == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, databaseName.getDatabase());
            }

            boolean ret = removeIncludeDatabase(database.getId());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getDatabase());
            }
        }
    }

    private void addIncludeTables(List<TableName> tableNames) throws DdlException {
        for (TableName tableName : tableNames) {
            if (CatalogMgr.isExternalCatalog(tableName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getCatalog());
            }

            Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(tableName.getDb());
            if (database == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, tableName.getDb());
            }

            Table table = database.getTable(tableName.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
            }

            boolean ret = addIncludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                    database.getId(), table.getId());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getTbl());
            }
        }
    }

    private void removeIncludeTables(List<TableName> tableNames) throws DdlException {
        for (TableName tableName : tableNames) {
            if (CatalogMgr.isExternalCatalog(tableName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getCatalog());
            }

            Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(tableName.getDb());
            if (database == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, tableName.getDb());
            }

            Table table = database.getTable(tableName.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
            }

            boolean ret = removeIncludeTable(table.getId());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getTbl());
            }
        }
    }

    public boolean addIncludeCatalog(long catalogId) {
        IncludeCatalog includeCatalog = new IncludeCatalog(catalogId);
        IncludeCatalog previous = includeCatalogs.putIfAbsent(includeCatalog.getCatalogId(), includeCatalog);
        return previous == null;
    }

    public boolean addIncludeDatabase(long catalogId, long databaseId) {
        if (includeCatalogs.containsKey(catalogId)) {
            return false;
        }

        IncludeDatabase includeDatabase = new IncludeDatabase(catalogId, databaseId);
        IncludeDatabase previous = includeDatabases.putIfAbsent(includeDatabase.getDatabaseId(), includeDatabase);
        return previous == null;
    }

    public boolean addIncludeTable(long catalogId, long databaseId, long tableId) {
        if (includeCatalogs.containsKey(catalogId) || includeDatabases.containsKey(databaseId)) {
            return false;
        }

        IncludeTable includeTable = new IncludeTable(catalogId, databaseId, tableId);
        IncludeTable previous = includeTables.putIfAbsent(includeTable.getTableId(), includeTable);
        return previous == null;
    }

    public boolean removeIncludeCatalog(long catalogId) {
        return includeCatalogs.remove(catalogId) != null;
    }

    public boolean removeIncludeDatabase(long databaseId) {
        return includeDatabases.remove(databaseId) != null;
    }

    public boolean removeIncludeTable(long tableId) {
        return includeTables.remove(tableId) != null;
    }

    public boolean isIncludeTable(Database database, Table table) {
        if (database == null || table == null) {
            return false;
        }
        if (includeCatalogs.containsKey(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID)
                && !database.isSystemDatabase() && !database.isStatisticsDatabase()) {
            return true;
        }
        return includeDatabases.containsKey(database.getId()) || includeTables.containsKey(table.getId());
    }

    public Map<Catalog, Map<Long, Database>> getIncludeCatalogs() {
        Map<Catalog, Map<Long, Database>> catalogs = Maps.newHashMapWithExpectedSize(includeCatalogs.size());
        for (IncludeCatalog includeCatalog : includeCatalogs.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(includeCatalog.getCatalogId()));
            // Filter system database
            Map<Long, Database> normalDbs = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getIdToDb().entrySet().stream().filter(
                            entry -> !entry.getValue().isSystemDatabase() && !entry.getValue().isStatisticsDatabase())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            catalogs.put(null, normalDbs);
        }
        return catalogs;
    }

    public Map<Long, Database> getIncludeDatabases() {
        Map<Long, Database> databases = Maps.newHashMapWithExpectedSize(includeDatabases.size());
        List<Long> toRemovedDbs = Lists.newArrayList();
        for (IncludeDatabase includeDatabase : includeDatabases.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(includeDatabase.getCatalogId()));
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(includeDatabase.getDatabaseId());
            if (database == null) {
                LOG.warn("Database id = {} is not found, remove it from failover group",
                        includeDatabase.getDatabaseId());
                toRemovedDbs.add(includeDatabase.getDatabaseId());
                continue;
            }
            databases.put(database.getId(), database);
        }
        for (Long databaseId : toRemovedDbs) {
            removeIncludeDatabase(databaseId);
        }
        return databases;
    }

    public Map<Database, Map<Long, Table>> getIncludeTables() {
        Map<Database, Map<Long, Table>> tables = Maps.newHashMap();
        List<Long> toRemovedTables = Lists.newArrayList();
        for (IncludeTable includeTable : includeTables.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(includeTable.getCatalogId()));
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(includeTable.getDatabaseId());
            if (database == null) {
                LOG.warn("Table id = {} is not found, remove it from failover group", includeTable.getTableId());
                toRemovedTables.add(includeTable.getTableId());
                continue;
            }
            Table table = database.getTable(includeTable.getTableId());
            if (table == null) {
                LOG.warn("Table id = {} is not found, remove it from failover group", includeTable.getTableId());
                toRemovedTables.add(includeTable.getTableId());
                continue;
            }
            tables.computeIfAbsent(database, key -> Maps.newHashMap()).put(table.getId(), table);
        }
        for (Long tableId : toRemovedTables) {
            removeIncludeTable(tableId);
        }
        return tables;
    }

    public List<String> getIncludeTableNames() {
        List<String> includeTableNames = Lists.newArrayList();
        for (IncludeCatalog includeCatalog : includeCatalogs.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(includeCatalog.catalogId));
            includeTableNames.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME + ".*.*");
        }

        includeTableNames.addAll(getIncludeDatabases().values().stream()
                .map(db -> db.getFullName() + ".*")
                .collect(Collectors.toList()));

        for (Map.Entry<Database, Map<Long, Table>> entry : getIncludeTables().entrySet()) {
            includeTableNames.addAll(entry.getValue().values().stream()
                    .map(table -> entry.getKey().getFullName() + "." + table.getName())
                    .collect(Collectors.toList()));
        }
        return includeTableNames;
    }

    public ReplicatedObjectMeta toObjectMeta(String clusterToken) {
        ReplicatedObjectMeta objectMeta = new ReplicatedObjectMeta(clusterToken, GlobalStateMgr.getCurrentState(), this);

        for (Map.Entry<Catalog, Map<Long, Database>> entry : getIncludeCatalogs().entrySet()) {
            boolean ret = objectMeta.addCatalog(entry.getKey(), entry.getValue());
            Preconditions.checkState(ret);
        }

        for (Database database : getIncludeDatabases().values()) {
            boolean ret = objectMeta.addDatabase(null, database);
            Preconditions.checkState(ret);
        }

        for (Map.Entry<Database, Map<Long, Table>> entry : getIncludeTables().entrySet()) {
            for (Table table : entry.getValue().values()) {
                boolean ret = objectMeta.addTable(null, entry.getKey(), table);
                Preconditions.checkState(ret);
            }
        }

        return objectMeta;
    }

    public static IncludeObjectMgr fromPrimaryIncludeMgr(IncludeObjectMgr primaryIncludeMgr,
            ReplicatedObjectMap objectMap) {
        IncludeObjectMgr includeObjectMgr = new IncludeObjectMgr();
        if (primaryIncludeMgr == null || objectMap == null) {
            return includeObjectMgr;
        }

        for (IncludeCatalog includeCatalog : primaryIncludeMgr.includeCatalogs.values()) {
            if (!CatalogMgr.isInternalCatalog(includeCatalog.getCatalogId())) {
                LOG.warn("Ignore non-internal include catalog id {}", includeCatalog.getCatalogId());
                continue;
            }
            includeObjectMgr.addIncludeCatalog(includeCatalog.getCatalogId());
        }

        for (IncludeDatabase includeDatabase : primaryIncludeMgr.includeDatabases.values()) {
            if (includeObjectMgr.includeCatalogs.containsKey(includeDatabase.getCatalogId())) {
                continue;
            }
            Long localDatabaseId = objectMap.getLocalDatabaseId(includeDatabase.getDatabaseId());
            if (localDatabaseId == null) {
                LOG.warn("Failed to map include database id {} from primary", includeDatabase.getDatabaseId());
                continue;
            }
            includeObjectMgr.addIncludeDatabase(includeDatabase.getCatalogId(), localDatabaseId);
        }

        for (IncludeTable includeTable : primaryIncludeMgr.includeTables.values()) {
            Long localDatabaseId = objectMap.getLocalDatabaseId(includeTable.getDatabaseId());
            Long localTableId = objectMap.getLocalTableId(includeTable.getTableId());
            if (includeObjectMgr.includeCatalogs.containsKey(includeTable.getCatalogId()) ||
                    (localDatabaseId != null && includeObjectMgr.includeDatabases.containsKey(localDatabaseId))) {
                continue;
            }
            if (localDatabaseId == null || localTableId == null) {
                LOG.warn("Failed to map include table id {} or database id {} from primary",
                        includeTable.getTableId(), includeTable.getDatabaseId());
                continue;
            }
            includeObjectMgr.addIncludeTable(includeTable.getCatalogId(), localDatabaseId, localTableId);
        }

        return includeObjectMgr;
    }
}
