// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

public class BaseTableInfo {

    @SerializedName(value = "isExternal")
    private boolean isExternal = false;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    // this use for snapshot
    private OlapTable baseTableCache;

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

    public OlapTable getBaseTableCache() {
        return baseTableCache;
    }

    public void setBaseTableCache(OlapTable baseTableCache) {
        this.baseTableCache = baseTableCache;
    }
}
