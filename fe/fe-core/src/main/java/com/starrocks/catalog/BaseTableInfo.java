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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import static com.starrocks.server.CatalogMgr.isInternalCatalog;

public class BaseTableInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableInfo.class);

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

    public BaseTableInfo(long dbId, String dbName, long tableId, String tableName) {
        this.catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        this.dbId = dbId;
        this.dbName = dbName;
        this.tableId = tableId;
        this.tableName = tableName;
    }

    public BaseTableInfo(long dbId, long tableId) {
        this(dbId, null, tableId, null);
    }

    public BaseTableInfo(String catalogName, String dbName, String tableIdentifier) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableIdentifier = tableIdentifier;
        this.tableName = tableIdentifier.split(":")[0];
    }

    public String getTableInfoStr() {
        if (isInternalCatalog(catalogName)) {
            return Joiner.on(".").join(dbId, tableId);
        } else {
            return Joiner.on(".").join(catalogName, dbName, tableName);
        }
    }

    public String getDbInfoStr() {
        if (isInternalCatalog(catalogName)) {
            return String.valueOf(dbId);
        } else {
            return Joiner.on(".").join(catalogName, dbName);
        }
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getDbName() {
        return this.dbName != null ? this.dbName : getDb().getFullName();
    }

    public String getTableName() {
        if (this.tableName != null) {
            return this.tableName;
        } else {
            Table table = getTable();
            return table == null ? null : table.getName();
        }
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
     * A checked version of getTable, which enforce checking existence of table
     *
     * @return the table if exists
     */
    public Optional<Table> mayGetTable() {
        return Optional.ofNullable(getTable());
    }

    /**
     * A checked version of getTable, which enforce checking the existence of table
     *
     * @return the table if exists
     */
    public Table getTableChecked() {
        Table table = getTable();
        if (table != null) {
            return table;
        }
        throw MaterializedViewExceptions.reportBaseTableNotExists(this);
    }

    @Deprecated
    public Table getTable() {
        if (isInternalCatalog(catalogName)) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                return null;
            } else {
                return db.getTable(tableId);
            }
        } else {
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                LOG.warn("catalog {} not exist", catalogName);
                return null;
            }
            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
            if (table == null) {
                LOG.warn("table {}.{}.{} not exist", catalogName, dbName, tableName);
                return null;
            }
            if (table.getTableIdentifier().equals(tableIdentifier)) {
                return table;
            }
            return null;
        }
    }

    public Database getDb() {
        if (isInternalCatalog(catalogName)) {
            return GlobalStateMgr.getCurrentState().getDb(dbId);
        } else {
            return GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        }
    }

    public String toString() {
        if (isInternalCatalog(catalogName)) {
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
