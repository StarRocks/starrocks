// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ReplicatedObjectMgr {
    private static final Logger LOG = LogManager.getLogger(ReplicatedObjectMgr.class);

    private static class CatalogInfo {

        @SerializedName(value = "catalogId")
        private long catalogId;

        public CatalogInfo(long catalogId) {
            this.catalogId = catalogId;
        }

        public long getCatalogId() {
            return catalogId;
        }
    }

    private static class DatabaseInfo {
        @SerializedName(value = "catalogId")
        private long catalogId;

        @SerializedName(value = "databaseId")
        private long databaseId;

        public DatabaseInfo(long catalogId, long databaseId) {
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

    private static class TableInfo {
        @SerializedName(value = "catalogId")
        private long catalogId;

        @SerializedName(value = "databaseId")
        private long databaseId;

        @SerializedName(value = "tableId")
        private long tableId;

        public TableInfo(long catalogId, long databaseId, long tableId) {
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

    @SerializedName(value = "catalogInfos")
    private final Map<Long, CatalogInfo> catalogInfos = Maps.newConcurrentMap();

    @SerializedName(value = "databaseInfos")
    private final Map<Long, DatabaseInfo> databaseInfos = Maps.newConcurrentMap();

    @SerializedName(value = "tableInfos")
    private final Map<Long, TableInfo> tableInfos = Maps.newConcurrentMap();

    public ReplicatedObjectMgr() {

    }

    public ReplicatedObjectMgr(CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        List<String> catalogNames = stmt.getCatalogNames();
        if (catalogNames != null) {
            for (String catalogName : stmt.getCatalogNames()) {
                if (CatalogMgr.isExternalCatalog(catalogName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
                }

                boolean ret = addCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID);
                if (!ret) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
                }
            }
        }

        List<DatabaseName> databaseNames = stmt.getDatabaseNames();
        if (databaseNames != null) {
            for (DatabaseName databaseName : stmt.getDatabaseNames()) {
                if (CatalogMgr.isExternalCatalog(databaseName.getCatalog()) ||
                        catalogInfos.containsKey(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getCatalog());
                }

                Database database = GlobalStateMgr.getServingState().getDb(databaseName.getDatabase());
                if (database == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, databaseName.getDatabase());
                }

                boolean ret = addDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, database.getId());
                if (!ret) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getDatabase());
                }
            }
        }

        List<TableName> tableNames = stmt.getTableNames();
        if (tableNames != null) {
            for (TableName tableName : stmt.getTableNames()) {
                if (CatalogMgr.isExternalCatalog(tableName.getCatalog()) ||
                        catalogInfos.containsKey(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getCatalog());
                }

                Database database = GlobalStateMgr.getServingState().getDb(tableName.getDb());
                if (database == null || databaseInfos.containsKey(database.getId())) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, tableName.getDb());
                }

                Table table = database.getTable(tableName.getTbl());
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName.getTbl());
                }

                boolean ret = addTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, database.getId(), table.getId());
                if (!ret) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getTbl());
                }
            }
        }
    }

    public boolean addCatalog(long catalogId) {
        CatalogInfo catalogInfo = new CatalogInfo(catalogId);
        CatalogInfo previous = catalogInfos.putIfAbsent(catalogInfo.getCatalogId(), catalogInfo);
        return previous == null;
    }

    public boolean addDatabase(long catalogId, long databaseId) {
        DatabaseInfo databaseInfo = new DatabaseInfo(catalogId, databaseId);
        DatabaseInfo previous = databaseInfos.putIfAbsent(databaseInfo.getDatabaseId(), databaseInfo);
        return previous == null;
    }

    public boolean addTable(long catalogId, long databaseId, long tableId) {
        TableInfo tableInfo = new TableInfo(catalogId, databaseId, tableId);
        TableInfo previous = tableInfos.putIfAbsent(tableInfo.getTableId(), tableInfo);
        return previous == null;
    }

    public void clearObjectIndex() {
        // TODO
    }

    public ReplicatedObjectMeta saveToObjectMeta() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        ReplicatedObjectMeta.SystemMeta systemMeta = new ReplicatedObjectMeta.SystemMeta(
                globalStateMgr.getToken(), globalStateMgr.getClusterInfo());
        ReplicatedObjectMeta objectMeta = new ReplicatedObjectMeta(systemMeta);

        for (CatalogInfo catalogInfo : catalogInfos.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(catalogInfo.getCatalogId()));
            ConcurrentHashMap<Long, Database> databases = globalStateMgr.getIdToDb();
            // Filter system database
            Map<Long, Database> normalDbs = databases.entrySet().stream().filter(
                    entry -> entry.getKey() > GlobalStateMgr.NEXT_ID_INIT_VALUE &&
                            !entry.getValue().getFullName().equals(StatsConstants.STATISTICS_DB_NAME))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            boolean ret = objectMeta.addCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, normalDbs);
            Preconditions.checkState(ret);
        }

        for (DatabaseInfo databaseInfo : databaseInfos.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(databaseInfo.getCatalogId()));
            Database database = globalStateMgr.getDb(databaseInfo.getDatabaseId());
            if (database == null) {
                LOG.warn("Database id = {} in failover group is not found", databaseInfo.getDatabaseId());
                continue;
            }
            boolean ret = objectMeta.addDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database);
            Preconditions.checkState(ret);
        }

        for (TableInfo tableInfo : tableInfos.values()) {
            Preconditions.checkState(CatalogMgr.isInternalCatalog(tableInfo.getCatalogId()));
            Database database = globalStateMgr.getDb(tableInfo.getDatabaseId());
            if (database == null) {
                LOG.warn("Database id = {} in failover group is not found", tableInfo.getDatabaseId());
                continue;
            }
            Table table = database.getTable(tableInfo.getTableId());
            if (table == null) {
                LOG.warn("Table id = {} in failover group is not found", tableInfo.getTableId());
                continue;
            }
            boolean ret = objectMeta.addTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database, table);
            Preconditions.checkState(ret);
        }

        return objectMeta;
    }
}
