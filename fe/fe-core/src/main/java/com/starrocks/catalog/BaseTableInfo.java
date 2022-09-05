// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

public class BaseTableInfo {

    @SerializedName(value = "isExternal")
    private boolean isExternal = false;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    // used for snapshot
    private OlapTable cachedBaseTable;

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean external) {
        isExternal = external;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public OlapTable getCachedBaseTable() {
        return cachedBaseTable;
    }

    public void setCachedBaseTable(OlapTable cachedBaseTable) {
        this.cachedBaseTable = cachedBaseTable;
    }
}
