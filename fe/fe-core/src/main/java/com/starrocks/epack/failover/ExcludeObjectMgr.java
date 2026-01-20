// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ExcludeObjectMgr {
    private static final Logger LOG = LogManager.getLogger(ExcludeObjectMgr.class);

    private static class ExcludeCatalog {
        @SerializedName(value = "catalogName")
        private final String catalogName;

        public ExcludeCatalog(String catalogName) {
            this.catalogName = catalogName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            ExcludeCatalog other = (ExcludeCatalog) obj;
            return Objects.equals(catalogName, other.catalogName);
        }
    }

    private static class ExcludeDatabase {
        @SerializedName(value = "catalogName")
        private final String catalogName;

        @SerializedName(value = "databaseName")
        private final String databaseName;

        public ExcludeDatabase(String catalogName, String databaseName) {
            this.catalogName = catalogName;
            this.databaseName = databaseName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, databaseName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            ExcludeDatabase other = (ExcludeDatabase) obj;
            return Objects.equals(catalogName, other.catalogName) &&
                    Objects.equals(databaseName, other.databaseName);
        }
    }

    private static class ExcludeTable {
        @SerializedName(value = "catalogName")
        private final String catalogName;

        @SerializedName(value = "databaseName")
        private final String databaseName;

        @SerializedName(value = "tableName")
        private final String tableName;

        public ExcludeTable(String catalogName, String databaseName, String tableName) {
            this.catalogName = catalogName;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, databaseName, tableName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            ExcludeTable other = (ExcludeTable) obj;
            return Objects.equals(catalogName, other.catalogName) &&
                    Objects.equals(databaseName, other.databaseName) &&
                    Objects.equals(tableName, other.tableName);
        }
    }

    @SerializedName(value = "excludeCatalogs")
    private final Set<ExcludeCatalog> excludeCatalogs = Sets.newConcurrentHashSet();

    @SerializedName(value = "excludeDatabases")
    private final Set<ExcludeDatabase> excludeDatabases = Sets.newConcurrentHashSet();

    @SerializedName(value = "excludeTables")
    private final Set<ExcludeTable> excludeTables = Sets.newConcurrentHashSet();

    public ExcludeObjectMgr() {

    }

    public ExcludeObjectMgr(CreatePrimaryFailoverGroupStmt stmt) throws DdlException {
        List<String> excludeCatalogs = stmt.getExcludeCatalogs();
        if (excludeCatalogs != null) {
            addExcludeCatalogs(excludeCatalogs);
        }

        List<DatabaseName> excludeDatabases = stmt.getExcludeDatabases();
        if (excludeDatabases != null) {
            addExcludeDatabases(excludeDatabases);
        }

        List<TableName> excludeTables = stmt.getExcludeTables();
        if (excludeTables != null) {
            addExcludeTables(excludeTables);
        }
    }

    public ExcludeObjectMgr(ExcludeObjectMgr other, AlterFailoverGroupSetStmt stmt) throws DdlException {
        List<String> excludeCatalogs = stmt.getExcludeCatalogs();
        List<DatabaseName> excludeDatabases = stmt.getExcludeDatabases();
        List<TableName> excludeTables = stmt.getExcludeTables();

        if (excludeCatalogs != null && !excludeCatalogs.isEmpty() ||
                excludeDatabases != null && !excludeDatabases.isEmpty() ||
                excludeTables != null && !excludeTables.isEmpty()) {
            if (excludeCatalogs != null) {
                addExcludeCatalogs(excludeCatalogs);
            }
            if (excludeDatabases != null) {
                addExcludeDatabases(excludeDatabases);
            }
            if (excludeTables != null) {
                addExcludeTables(excludeTables);
            }
        } else {
            this.excludeCatalogs.addAll(other.excludeCatalogs);
            this.excludeDatabases.addAll(other.excludeDatabases);
            this.excludeTables.addAll(other.excludeTables);
        }
    }

    public ExcludeObjectMgr(ExcludeObjectMgr other, AlterFailoverGroupAddStmt stmt) throws DdlException {
        this.excludeCatalogs.addAll(other.excludeCatalogs);
        List<String> excludeCatalogs = stmt.getExcludeCatalogs();
        if (excludeCatalogs != null) {
            addExcludeCatalogs(excludeCatalogs);
        }

        this.excludeDatabases.addAll(other.excludeDatabases);
        List<DatabaseName> excludeDatabases = stmt.getExcludeDatabases();
        if (excludeDatabases != null) {
            addExcludeDatabases(excludeDatabases);
        }

        this.excludeTables.addAll(other.excludeTables);
        List<TableName> excludeTables = stmt.getExcludeTables();
        if (excludeTables != null) {
            addExcludeTables(excludeTables);
        }
    }

    public ExcludeObjectMgr(ExcludeObjectMgr other, AlterFailoverGroupRemoveStmt stmt) throws DdlException {
        this.excludeCatalogs.addAll(other.excludeCatalogs);
        List<String> excludeCatalogs = stmt.getExcludeCatalogs();
        if (excludeCatalogs != null) {
            removeExcludeCatalogs(excludeCatalogs);
        }

        this.excludeDatabases.addAll(other.excludeDatabases);
        List<DatabaseName> excludeDatabases = stmt.getExcludeDatabases();
        if (excludeDatabases != null) {
            removeExcludeDatabases(excludeDatabases);
        }

        this.excludeTables.addAll(other.excludeTables);
        List<TableName> excludeTables = stmt.getExcludeTables();
        if (excludeTables != null) {
            removeExcludeTables(excludeTables);
        }
    }

    private void addExcludeCatalogs(List<String> catalogNames) throws DdlException {
        for (String catalogName : catalogNames) {
            if (CatalogMgr.isExternalCatalog(catalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }

            boolean ret = addExcludeCatalog(catalogName);
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }
        }
    }

    private void removeExcludeCatalogs(List<String> catalogNames) throws DdlException {
        for (String catalogName : catalogNames) {
            if (CatalogMgr.isExternalCatalog(catalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }

            boolean ret = removeExcludeCatalog(catalogName);
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, catalogName);
            }
        }
    }

    private void addExcludeDatabases(List<DatabaseName> databaseNamess) throws DdlException {
        for (DatabaseName databaseName : databaseNamess) {
            if (CatalogMgr.isExternalCatalog(databaseName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getCatalog());
            }

            boolean ret = addExcludeDatabase(databaseName.getCatalog(), databaseName.getDatabase());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getDatabase());
            }
        }
    }

    private void removeExcludeDatabases(List<DatabaseName> databaseNamess) throws DdlException {
        for (DatabaseName databaseName : databaseNamess) {
            if (CatalogMgr.isExternalCatalog(databaseName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getCatalog());
            }

            boolean ret = removeExcludeDatabase(databaseName.getCatalog(), databaseName.getDatabase());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, databaseName.getDatabase());
            }
        }
    }

    private void addExcludeTables(List<TableName> tableNames) throws DdlException {
        for (TableName tableName : tableNames) {
            if (CatalogMgr.isExternalCatalog(tableName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getCatalog());
            }

            boolean ret = addExcludeTable(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getTbl());
            }
        }
    }

    private void removeExcludeTables(List<TableName> tableNames) throws DdlException {
        for (TableName tableName : tableNames) {
            if (CatalogMgr.isExternalCatalog(tableName.getCatalog())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getCatalog());
            }

            boolean ret = removeExcludeTable(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            if (!ret) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, tableName.getTbl());
            }
        }
    }

    public boolean addExcludeCatalog(String catalogName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        return excludeCatalogs.add(excludeCatalog);
    }

    public boolean addExcludeDatabase(String catalogName, String databaseName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        if (excludeCatalogs.contains(excludeCatalog)) {
            return false;
        }

        ExcludeDatabase excludeDatabase = new ExcludeDatabase(catalogName, databaseName);
        return excludeDatabases.add(excludeDatabase);
    }

    public boolean addExcludeTable(String catalogName, String databaseName, String tableName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        if (excludeCatalogs.contains(excludeCatalog)) {
            return false;
        }
        ExcludeDatabase excludeDatabase = new ExcludeDatabase(catalogName, databaseName);
        if (excludeDatabases.contains(excludeDatabase)) {
            return false;
        }

        ExcludeTable excludeTable = new ExcludeTable(catalogName, databaseName, tableName);
        return excludeTables.add(excludeTable);
    }

    public boolean removeExcludeCatalog(String catalogName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        return excludeCatalogs.remove(excludeCatalog);
    }

    public boolean removeExcludeDatabase(String catalogName, String databaseName) {
        ExcludeDatabase excludeDatabase = new ExcludeDatabase(catalogName, databaseName);
        return excludeDatabases.remove(excludeDatabase);
    }

    public boolean removeExcludeTable(String catalogName, String databaseName, String tableName) {
        ExcludeTable excludeTable = new ExcludeTable(catalogName, databaseName, tableName);
        return excludeTables.remove(excludeTable);
    }

    public boolean isExcludeCatalog(String catalogName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        return excludeCatalogs.contains(excludeCatalog);
    }

    public boolean isExcludeDatabase(String catalogName, String databaseName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        if (excludeCatalogs.contains(excludeCatalog)) {
            return true;
        }

        ExcludeDatabase excludeDatabase = new ExcludeDatabase(catalogName, databaseName);
        return excludeDatabases.contains(excludeDatabase);
    }

    public boolean isExcludeTable(String catalogName, String databaseName, String tableName) {
        ExcludeCatalog excludeCatalog = new ExcludeCatalog(catalogName);
        if (excludeCatalogs.contains(excludeCatalog)) {
            return true;
        }

        ExcludeDatabase excludeDatabase = new ExcludeDatabase(catalogName, databaseName);
        if (excludeDatabases.contains(excludeDatabase)) {
            return true;
        }

        ExcludeTable excludeTable = new ExcludeTable(catalogName, databaseName, tableName);
        return excludeTables.contains(excludeTable);
    }

    public List<String> getExcludeTableNames() {
        List<String> excludeTableNames = Lists.newArrayList();
        for (ExcludeCatalog excludeCatalog : excludeCatalogs) {
            excludeTableNames.add(excludeCatalog.getCatalogName() + ".*.*");
        }
        for (ExcludeDatabase excludeDatabase : excludeDatabases) {
            excludeTableNames.add(excludeDatabase.getDatabaseName() + ".*");
        }
        for (ExcludeTable excludeTable : excludeTables) {
            excludeTableNames.add(excludeTable.getDatabaseName() + "." + excludeTable.getTableName());
        }
        return excludeTableNames;
    }
}
