// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChangeMaterializedViewRefreshSchemeLog implements Writable {
    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshType")
    private MaterializedView.RefreshType refreshType;

    @SerializedName(value = "asyncRefreshContext")
    private MaterializedView.AsyncRefreshContext asyncRefreshContext;

    public ChangeMaterializedViewRefreshSchemeLog(MaterializedView materializedView) {
        this.id = materializedView.getId();
        this.dbId = materializedView.getDbId();
        this.refreshType = materializedView.getRefreshScheme().getType();
        this.asyncRefreshContext = materializedView.getRefreshScheme().getAsyncRefreshContext().copy();
    }

    public ChangeMaterializedViewRefreshSchemeLog() {
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public MaterializedView.RefreshType getRefreshType() {
        return refreshType;
    }

    public MaterializedView.AsyncRefreshContext getAsyncRefreshContext() {
        return asyncRefreshContext;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ChangeMaterializedViewRefreshSchemeLog read(DataInput in) throws IOException {
        try {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, ChangeMaterializedViewRefreshSchemeLog.class);
        } catch (Exception ex) {
            if (Config.ignore_materialized_view_error) {
                return new ChangeMaterializedViewRefreshSchemeLog();
            } else {
                throw ex;
            }
        }
    }
}