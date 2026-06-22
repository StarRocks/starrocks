// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;

import java.util.Objects;
import java.util.Optional;

public class TableUID {
    @SerializedName(value = "ci")
    private long catalogId;
    @SerializedName(value = "d")
    protected String databaseUUID;
    @SerializedName(value = "t")
    protected String tableUUID;

    protected TableUID(long catalogId, String databaseUUID, String tableUUID) {
        this.catalogId = catalogId;
        this.databaseUUID = databaseUUID;
        this.tableUUID = tableUUID;
    }

    protected TableUID(String databaseUUID, String tableUUID) {
        this.catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        this.databaseUUID = databaseUUID;
        this.tableUUID = tableUUID;
    }

    public TableName toTableName() {
        if (catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(Long.parseLong(this.databaseUUID));
            if (db == null) {
                return null;
            }
            Table table = db.getTable(Long.parseLong(this.tableUUID));
            if (table == null) {
                return null;
            }
            return new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db.getFullName(), table.getName());
        } else {
            Optional<Catalog> catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogById(catalogId);
            if (!catalog.isPresent()) {
                return null;
            }
            String dbName = ExternalCatalog.getDbNameFromUUID(databaseUUID);
            String tblName = ExternalCatalog.getTableNameFromUUID(tableUUID);
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(new ConnectContext(), catalog.get().getName(), dbName);
            if (db == null) {
                return null;
            }
            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(new ConnectContext(), catalog.get().getName(), dbName, tblName);
            if (table == null) {
                return null;
            }

            return new TableName(catalog.get().getName(), db.getFullName(), table.getName());
        }
    }

    public static TableUID generate(ConnectContext ctx, String catalogName, String dbName, String tableName) {
        long catalogId;

        // Default to internal_catalog when no catalog explicitly selected.
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)
                || CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new SemanticException("cannot find catalog: " + catalogName);
            }
            catalogId = catalog.getId();
        }

        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(ctx, catalogName, dbName);
        if (database == null) {
            throw new SemanticException("cannot find db: " + dbName);
        }
        String databaseUUID = database.getUUID();

        Table table = MetaUtils.getSessionAwareTable(
                ctx, database, new TableName(catalogName, dbName, tableName));
        String tblUUID = table.getUUID();
        return new TableUID(catalogId, databaseUUID, tblUUID);
    }

    public static TableUID generate(Database database, Table table) {
        return new TableUID(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, database.getUUID(), table.getUUID());
    }

    public long getCatalogId() {
        return catalogId;
    }

    public String getDatabaseUUID() {
        return databaseUUID;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public boolean validate() {
        Database db;
        if (catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(Long.parseLong(this.databaseUUID));
            if (db == null) {
                return false;
            }
            return GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTableIncludeRecycleBin(db, Long.parseLong(this.tableUUID)) != null;
        } else {
            Optional<Catalog> catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogById(catalogId);
            if (!catalog.isPresent()) {
                return false;
            }
            String dbName = ExternalCatalog.getDbNameFromUUID(databaseUUID);
            String tblName = ExternalCatalog.getTableNameFromUUID(tableUUID);
            db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(new ConnectContext(), catalog.get().getName(), dbName);
            if (db == null) {
                return false;
            }
            return GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(new ConnectContext(), catalog.get().getName(), dbName, tblName) != null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableUID tableUID = (TableUID) o;
        return catalogId == tableUID.catalogId && Objects.equals(databaseUUID, tableUID.databaseUUID) &&
                Objects.equals(tableUUID, tableUID.tableUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, databaseUUID, tableUUID);
    }

    public String toString() {
        return String.format("TableUID(catalogId=%d,databaseUUID=%s,tableUUID=%s)",
                catalogId, databaseUUID, tableUUID);
    }
}
