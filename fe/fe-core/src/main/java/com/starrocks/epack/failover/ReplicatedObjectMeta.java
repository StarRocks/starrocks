// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.system.SystemInfoService;

import java.util.Map;

public class ReplicatedObjectMeta {
    public static class SystemMeta {
        private String token;

        private SystemInfoService systemInfoService;

        public SystemMeta(String token, SystemInfoService systemInfoService) {
            this.token = token;
            this.systemInfoService = systemInfoService;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public SystemInfoService getSystemInfoService() {
            return systemInfoService;
        }
    }

    public static class CatalogMeta {
        private long catalogId;

        private String catalogName;

        private Map<Long, Database> databases;

        public CatalogMeta(long catalogId, String catalogName, Map<Long, Database> databases) {
            this.catalogId = catalogId;
            this.catalogName = catalogName;
            this.databases = databases;
        }

        public long getCatalogId() {
            return catalogId;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public Map<Long, Database> getDatabases() {
            return databases;
        }
    }

    public static class DatabaseMeta {
        private long catalogId;

        private String catalogName;

        private Database database;

        public DatabaseMeta(long catalogId, String catalogName, Database database) {
            this.catalogId = catalogId;
            this.catalogName = catalogName;
            this.database = database;
        }

        public long getCatalogId() {
            return catalogId;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public Database getDatabase() {
            return database;
        }
    }

    public static class TableMeta {
        private long catalogId;

        private String catalogName;

        private long databaseId;

        private String databaseName;

        private Table table;

        public TableMeta(long catalogId, String catalogName, Database database, Table table) {
            this.catalogId = catalogId;
            this.catalogName = catalogName;
            this.databaseId = database.getId();
            this.databaseName = database.getFullName();
            this.table = table;
        }

        public long getCatalogId() {
            return catalogId;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public long getDatabaseId() {
            return databaseId;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public Table getTable() {
            return table;
        }
    }

    private SystemMeta systemMeta;

    private final Map<Long, CatalogMeta> catalogMetas = Maps.newConcurrentMap();

    private final Map<Long, DatabaseMeta> databaseMetas = Maps.newConcurrentMap();

    private final Map<Long, TableMeta> tableMetas = Maps.newConcurrentMap();

    ReplicatedObjectMeta(SystemMeta systemMeta) {
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

    public boolean addCatalog(long catalogId, String catalogName, Map<Long, Database> databases) {
        CatalogMeta catalogMeta = new CatalogMeta(catalogId, catalogName, databases);
        CatalogMeta previous = catalogMetas.putIfAbsent(catalogId, catalogMeta);
        return previous == null;
    }

    public boolean addCatalog(CatalogMeta catalogMeta) {
        CatalogMeta previous = catalogMetas.putIfAbsent(catalogMeta.getCatalogId(), catalogMeta);
        return previous == null;
    }

    public boolean addDatabase(long catalogId, String catalogName, Database database) {
        DatabaseMeta databaseMeta = new DatabaseMeta(catalogId, catalogName, database);
        DatabaseMeta previous = databaseMetas.putIfAbsent(database.getId(), databaseMeta);
        return previous == null;
    }

    public boolean addDatabase(DatabaseMeta databaseMeta) {
        DatabaseMeta previous = databaseMetas.putIfAbsent(databaseMeta.getDatabase().getId(), databaseMeta);
        return previous == null;
    }

    public boolean addTable(long catalogId, String catalogName, Database database, Table table) {
        TableMeta tableMeta = new TableMeta(catalogId, catalogName, database, table);
        TableMeta previous = tableMetas.putIfAbsent(table.getId(), tableMeta);
        return previous == null;
    }

    public boolean addTable(TableMeta tableMeta) {
        TableMeta previous = tableMetas.putIfAbsent(tableMeta.getTable().getId(), tableMeta);
        return previous == null;
    }
}
