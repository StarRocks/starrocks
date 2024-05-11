// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.system.SystemInfoService;

import java.util.Map;

public class ReplicatedObjectMeta {
    public static class SystemMeta {
        private final String token;

        private final SystemInfoService systemInfoService;

        public SystemMeta(String token, SystemInfoService systemInfoService) {
            this.token = token;
            this.systemInfoService = systemInfoService;
        }

        public String getToken() {
            return token;
        }

        public SystemInfoService getSystemInfoService() {
            return systemInfoService;
        }
    }

    public static class CatalogMeta {
        // Null means default catalog
        private final Catalog catalog;

        private final Map<Long, Database> databases;

        public CatalogMeta(Catalog catalog, Map<Long, Database> databases) {
            this.catalog = catalog;
            this.databases = databases;
        }

        public boolean isInternalCatalog() {
            return catalog == null;
        }

        public Catalog getCatalog() {
            return catalog;
        }

        public Map<Long, Database> getDatabases() {
            return databases;
        }
    }

    public static class DatabaseMeta {
        private final Catalog catalog;

        private final Database database;

        public DatabaseMeta(Catalog catalog, Database database) {
            this.catalog = catalog;
            this.database = database;
        }

        public boolean isInternalCatalog() {
            return catalog == null;
        }

        public Catalog getCatalog() {
            return catalog;
        }

        public Database getDatabase() {
            return database;
        }
    }

    public static class TableMeta {
        private final Catalog catalog;

        private final Database database;

        private final Table table;

        public TableMeta(Catalog catalog, Database database, Table table) {
            this.catalog = catalog;
            this.database = database;
            this.table = table;
        }

        public boolean isInternalCatalog() {
            return catalog == null;
        }

        public Catalog getCatalog() {
            return catalog;
        }

        public Database getDatabase() {
            return database;
        }

        public Table getTable() {
            return table;
        }
    }

    private final SystemMeta systemMeta;

    private final Map<Long, CatalogMeta> catalogMetas = Maps.newConcurrentMap();

    private final Map<Long, DatabaseMeta> databaseMetas = Maps.newConcurrentMap();

    private final Map<Long, TableMeta> tableMetas = Maps.newConcurrentMap();

    public ReplicatedObjectMeta(SystemMeta systemMeta) {
        this.systemMeta = systemMeta;
    }

    public SystemMeta getSystemMeta() {
        return systemMeta;
    }

    public Map<Long, CatalogMeta> getCatalogMetas() {
        return catalogMetas;
    }

    public Map<Long, DatabaseMeta> getDatabaseMetas() {
        return databaseMetas;
    }

    public Map<Long, TableMeta> getTableMetas() {
        return tableMetas;
    }

    public boolean addCatalog(Catalog catalog, Map<Long, Database> databases) {
        CatalogMeta catalogMeta = new CatalogMeta(catalog, databases);
        long catalogId = catalog != null ? catalog.getId() : InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        CatalogMeta previous = catalogMetas.putIfAbsent(catalogId, catalogMeta);
        return previous == null;
    }

    public boolean addCatalog(CatalogMeta catalogMeta) {
        Catalog catalog = catalogMeta.getCatalog();
        long catalogId = catalog != null ? catalog.getId() : InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        CatalogMeta previous = catalogMetas.putIfAbsent(catalogId, catalogMeta);
        return previous == null;
    }

    public boolean addDatabase(Catalog catalog, Database database) {
        DatabaseMeta databaseMeta = new DatabaseMeta(catalog, database);
        DatabaseMeta previous = databaseMetas.putIfAbsent(database.getId(), databaseMeta);
        return previous == null;
    }

    public boolean addDatabase(DatabaseMeta databaseMeta) {
        DatabaseMeta previous = databaseMetas.putIfAbsent(databaseMeta.getDatabase().getId(), databaseMeta);
        return previous == null;
    }

    public boolean addTable(Catalog catalog, Database database, Table table) {
        TableMeta tableMeta = new TableMeta(catalog, database, table);
        TableMeta previous = tableMetas.putIfAbsent(table.getId(), tableMeta);
        return previous == null;
    }

    public boolean addTable(TableMeta tableMeta) {
        TableMeta previous = tableMetas.putIfAbsent(tableMeta.getTable().getId(), tableMeta);
        return previous == null;
    }
}
