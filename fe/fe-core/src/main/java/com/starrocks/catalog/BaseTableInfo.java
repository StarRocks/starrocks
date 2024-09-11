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


package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.server.CatalogMgr;

/**
 * BaseTableInfo is used for MaterializedView persisted as a base table's meta info which can be an olap
 * table or an external table.
 */
public class BaseTableInfo {
    @SerializedName(value = "catalogName")
    private final String catalogName;

    @SerializedName(value = "dbId")
    private long dbId = -1;

    @SerializedName(value = "tableId")
    private long tableId = -1;

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "tableIdentifier")
    private String tableIdentifier;

    @SerializedName(value = "tableName")
    private String tableName;

    // used for olap table
    public BaseTableInfo(long dbId, String dbName, String tableName, long tableId) {
        this.catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        this.dbId = dbId;
        this.tableId = tableId;
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName),
                String.format("BaseTableInfo's dbName %s should not null", dbName));
        this.dbName = dbName;
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName),
                String.format("BaseTableInfo's tableName %s should not null", tableName));
        this.tableName = tableName;
    }

    // used for external table
    public BaseTableInfo(String catalogName, String dbName, String tableName, String tableIdentifier) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableIdentifier = tableIdentifier;
    }

    public String getTableInfoStr() {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Joiner.on(".").join(dbId, tableId);
        } else {
            return Joiner.on(".").join(catalogName, dbName, tableName);
        }
    }

    public String getDbInfoStr() {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return String.valueOf(dbId);
        } else {
            return Joiner.on(".").join(catalogName, dbName);
        }
    }

    public boolean isInternalCatalog() {
        return CatalogMgr.isInternalCatalog(catalogName);
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getDbName() {
        return this.dbName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getTableIdentifier() {
        return this.tableIdentifier == null ? String.valueOf(tableId) : this.tableIdentifier;
    }

    public long getDbId() {
        return this.dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    /**
     * Called when a table is renamed.
     * @param newTable the new table with the new table name
     */
    public void onTableRename(Table newTable, String oldTableName) {
        if (newTable == null) {
            return;
        }

        // only changes the table name if the old table name is the same as the current table name
        if (this.tableName != null && this.tableName.equals(oldTableName)) {
            if (newTable instanceof OlapTable) {
                this.tableId = newTable.getId();
            }
            this.tableName = newTable.getName();
        }
    }

    public String toString() {
        if (isInternalCatalog()) {
            return Joiner.on(".").join(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbId, tableId);
        } else {
            return Joiner.on(".").join(catalogName, dbName, tableIdentifier);
        }
    }

    public String getReadableString() {
        String dbName = getDbName();
        dbName = dbName != null ? dbName : "null";
        String tableName = getTableName();
        tableName = tableName != null ? tableName : "null";
        return catalogName + "." + dbName + "." + tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseTableInfo)) {
            return false;
        }
        BaseTableInfo that = (BaseTableInfo) o;
        return dbId == that.dbId && tableId == that.tableId &&
                Objects.equal(catalogName, that.catalogName) &&
                Objects.equal(dbName, that.dbName) &&
                Objects.equal(tableIdentifier, that.tableIdentifier) &&
                Objects.equal(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(catalogName, dbId, tableId, dbName, tableIdentifier, tableName);
    }
}