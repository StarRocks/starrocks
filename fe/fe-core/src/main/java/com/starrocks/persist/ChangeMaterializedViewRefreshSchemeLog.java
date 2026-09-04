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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class ChangeMaterializedViewRefreshSchemeLog implements Writable {
    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshType")
    private MaterializedViewRefreshType refreshType;

    @SerializedName(value = "asyncRefreshContext")
    private MaterializedView.AsyncRefreshContext asyncRefreshContext;

    // Wall-clock confirm time; unlike lastRefreshTime it is not recomputable from the version map, so it must be logged.
    @SerializedName(value = "lastFreshnessConfirmedAt")
    private long lastFreshnessConfirmedAt;

    // Boxed: null = the log predates this field (replay keeps the in-memory value); 0 = a real leader value.
    @SerializedName(value = "lastRefreshTime")
    private Long lastRefreshTime;
    @SerializedName(value = "lastExecutedRefreshMode")
    private MaterializedView.RefreshMode lastExecutedRefreshMode;
    @SerializedName(value = "lastRefreshModeReason")
    private MaterializedView.RefreshModeReason lastRefreshModeReason;
    @SerializedName(value = "lastRefreshModeReasonTable")
    private String lastRefreshModeReasonTable;

    public ChangeMaterializedViewRefreshSchemeLog(MaterializedView materializedView) {
        this(materializedView, materializedView.getRefreshScheme());
    }

    public ChangeMaterializedViewRefreshSchemeLog(MaterializedView materializedView,
                                                  MaterializedView.MvRefreshScheme refreshScheme) {
        this.id = materializedView.getId();
        this.dbId = materializedView.getDbId();
        this.refreshType = refreshScheme.getType();
        this.asyncRefreshContext = refreshScheme.getAsyncRefreshContext().copy();
        this.lastFreshnessConfirmedAt = refreshScheme.getLastFreshnessConfirmedAt();
        this.lastRefreshTime = refreshScheme.getLastRefreshTime();
        this.lastExecutedRefreshMode = refreshScheme.getLastExecutedRefreshMode();
        this.lastRefreshModeReason = refreshScheme.getLastRefreshModeReason();
        this.lastRefreshModeReasonTable = refreshScheme.getLastRefreshModeReasonTable();
    }

    public ChangeMaterializedViewRefreshSchemeLog() {
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public MaterializedViewRefreshType  getRefreshType() {
        return refreshType;
    }

    public MaterializedView.AsyncRefreshContext getAsyncRefreshContext() {
        return asyncRefreshContext;
    }

    public long getLastFreshnessConfirmedAt() {
        return lastFreshnessConfirmedAt;
    }

    public Long getLastRefreshTime() {
        return lastRefreshTime;
    }

    public MaterializedView.RefreshMode getLastExecutedRefreshMode() {
        return lastExecutedRefreshMode;
    }

    public MaterializedView.RefreshModeReason getLastRefreshModeReason() {
        return lastRefreshModeReason;
    }

    public String getLastRefreshModeReasonTable() {
        return lastRefreshModeReasonTable;
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
