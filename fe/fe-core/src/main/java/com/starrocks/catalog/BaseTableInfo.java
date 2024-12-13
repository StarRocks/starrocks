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
<<<<<<< HEAD
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import static com.starrocks.server.CatalogMgr.isInternalCatalog;

public class BaseTableInfo {
    private static final Logger LOG = LogManager.getLogger(BaseTableInfo.class);

=======
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.server.CatalogMgr;

/**
 * BaseTableInfo is used for MaterializedView persisted as a base table's meta info which can be an olap
 * table or an external table.
 */
public class BaseTableInfo {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
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

=======
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

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    // used for external table
    public BaseTableInfo(String catalogName, String dbName, String tableName, String tableIdentifier) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableIdentifier = tableIdentifier;
    }

<<<<<<< HEAD
    public static BaseTableInfo fromTableName(TableName name, Table table) {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(name.getCatalog(), name.getDb());
        if (isInternalCatalog(name.getCatalog())) {
            return new BaseTableInfo(database.getId(), database.getFullName(), table.getId(), table.getName());
        } else {
            return new BaseTableInfo(name.getCatalog(), name.getDb(), table.getName(), table.getTableIdentifier());
        }
    }

    public String getTableInfoStr() {
        if (isInternalCatalog(catalogName)) {
=======
    public String getTableInfoStr() {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            return Joiner.on(".").join(dbId, tableId);
        } else {
            return Joiner.on(".").join(catalogName, dbName, tableName);
        }
    }

    public String getDbInfoStr() {
<<<<<<< HEAD
        if (isInternalCatalog(catalogName)) {
=======
        if (CatalogMgr.isInternalCatalog(catalogName)) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            return String.valueOf(dbId);
        } else {
            return Joiner.on(".").join(catalogName, dbName);
        }
    }

<<<<<<< HEAD
=======
    public boolean isInternalCatalog() {
        return CatalogMgr.isInternalCatalog(catalogName);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public String getCatalogName() {
        return this.catalogName;
    }

    public String getDbName() {
<<<<<<< HEAD
        return this.dbName != null ? this.dbName : getDb().getFullName();
    }

    public String getTableName() {
        if (this.tableName != null) {
            return this.tableName;
        } else {
            Table table = getTable();
            return table == null ? null : table.getName();
        }
=======
        return this.dbName;
    }

    public String getTableName() {
        return this.tableName;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
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

            if (tableIdentifier == null) {
                this.tableIdentifier = table.getTableIdentifier();
                return table;
            }

            if (tableIdentifier != null && table.getTableIdentifier().equals(tableIdentifier)) {
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
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    public String toString() {
<<<<<<< HEAD
        if (isInternalCatalog(catalogName)) {
=======
        if (isInternalCatalog()) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
}
=======
}
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
