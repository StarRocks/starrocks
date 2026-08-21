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

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;

import java.util.List;

/**
 * Metadata of a ClickHouse-compatible logical sink materialized view created via
 * {@code CREATE MATERIALIZED VIEW mv TO <target> AS SELECT ... FROM <base>}.
 *
 * <p>Unlike a synchronous rollup MV, a logical sink MV holds no storage of its own and creates no
 * tablets: it is a write-triggered transformation rule attached to the base table. On every load
 * into the base table, the base rows are filtered and projected by this rule and the resulting rows
 * are written into the existing, user-managed target OLAP table within the same load transaction.
 *
 * <p>Following the synchronous MV convention, only the originating {@code defineStmt} is persisted;
 * the per-column define expressions and the optional WHERE predicate are recovered by re-parsing
 * the statement (see {@code CreateMaterializedViewStmt#parseDefineExprWithoutAnalyze}). This keeps
 * the metadata small and avoids serializing analyzed Expr trees.
 */
public class LogicalSinkMVMeta {

    @SerializedName(value = "mvId")
    private long mvId;

    @SerializedName(value = "mvName")
    private String mvName;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "baseTableId")
    private long baseTableId;

    // The existing target OLAP table the transformed rows are written into.
    @SerializedName(value = "targetTableId")
    private long targetTableId;

    // Original `CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...`, re-parsed to recover the
    // per-column transform expressions and the optional WHERE predicate.
    @SerializedName(value = "defineStmt")
    private String defineStmt;

    // Phase-1 projection mapping: for each target column (in order), the base column name the MV's
    // SELECT item projects from, when that item is a plain column reference. Empty string means the
    // item is a scalar/JSON expression (not yet supported by the write-path fan-out).
    @SerializedName(value = "projectBaseColumns")
    private List<String> projectBaseColumns;

    public LogicalSinkMVMeta() {
    }

    public LogicalSinkMVMeta(long mvId, String mvName, long dbId, long baseTableId, long targetTableId,
                             String defineStmt) {
        this.mvId = mvId;
        this.mvName = mvName;
        this.dbId = dbId;
        this.baseTableId = baseTableId;
        this.targetTableId = targetTableId;
        this.defineStmt = defineStmt;
    }

    public long getMvId() {
        return mvId;
    }

    public String getMvName() {
        return mvName;
    }

    public long getDbId() {
        return dbId;
    }

    public long getBaseTableId() {
        return baseTableId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public String getDefineStmt() {
        return defineStmt;
    }

    public List<String> getProjectBaseColumns() {
        return projectBaseColumns;
    }

    public void setProjectBaseColumns(List<String> projectBaseColumns) {
        this.projectBaseColumns = projectBaseColumns;
    }

    // True only when every target column maps to a plain base column projection (Phase-1 supported).
    public boolean isPlainProjection() {
        if (projectBaseColumns == null || projectBaseColumns.isEmpty()) {
            return false;
        }
        for (String c : projectBaseColumns) {
            if (c == null || c.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
