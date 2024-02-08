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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.Optional;

/**
 * BaseTableInfo is used for MaterializedView persisted as a base table's meta info which can be an olap
 * table or an external table.
 */
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

    ///////// Methods to help to use {@code BaseTableInfo} which cannot be got from metadata directly. /////////

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

    /**
     * Get {@link Table} from {@code BaseTableInfo} which it can be an OlapTable or ExternalTable.
     * </p>
     * NOTE: Prefer {@link BaseTableInfo#getTableChecked()} or {@link BaseTableInfo#mayGetTable()}
     * because this method may return null if base table has already dropped or schema changed.
     * </p>
     * @return {@link Table} if BaseTableInfo is health otherwise null will be returned.
     */
    @Deprecated
    public Table getTable() {
        if (isInternalCatalog()) {
            // olap table
            Database db = GlobalStateMgr.getCurrentState().getDb(getDbId());
            if (db == null) {
                return null;
            }
            return db.getTable(getTableId());
        } else {
            // external table
            String catalogName = getCatalogName();
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                LOG.warn("catalog {} not exist", catalogName);
                return null;
            }

            // upgrade from 3.1 to 3.2, dbName/tableName maybe null after dbs or tables are dropped
            String dbName = getDbName();
            String tableName = getTableName();
            if (dbName == null || tableName == null) {
                return null;
            }
            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
            if (table == null) {
                LOG.warn("table {}.{}.{} not exist", catalogName, dbName, tableName);
                return null;
            }
            String tableIdentifier = getTableIdentifier();
            if (tableIdentifier != null && tableIdentifier.equals(table.getTableIdentifier())) {
                return table;
            }
            return null;
        }
    }

    /**
     * Get {@link Database} from {@code BaseTableInfo}.
     * @return {@link Database} if BaseTableInfo is health otherwise null will be returned.
     */
    public Database getDb() {
        if (isInternalCatalog()) {
            // olap table
            return GlobalStateMgr.getCurrentState().getDb(this.getDbId());
        } else {
            // external table
            return GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(this.getCatalogName(), this.getDbName());
        }
    }
}