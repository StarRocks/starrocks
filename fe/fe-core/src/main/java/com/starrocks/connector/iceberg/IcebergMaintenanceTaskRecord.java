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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;

import java.util.UUID;

/**
 * One finished iceberg metadata maintenance task (auto or manual), recorded for the
 * information_schema.iceberg_maintenance_tasks system table.
 */
public class IcebergMaintenanceTaskRecord {
    public static final String TRIGGER_REASON_SCHEDULE = "schedule";
    public static final String TRIGGER_REASON_MANUAL = "manual";

    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_FAILED = "failed";
    public static final String STATUS_PARTIAL = "partial";
    // the action ran successfully but had nothing to do (no snapshots to expire,
    // no manifests to compact, no orphan files to remove)
    public static final String STATUS_SKIPPED = "skipped";

    // failure reasons are truncated to this length; the iceberg_maintenance_tasks
    // FAILURE_REASON column is declared with the same width
    public static final int MAX_FAILURE_REASON_LENGTH = 4096;

    private final String taskId;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final String triggerReason;
    private final String stmt;
    private final long startTimeMs;

    private String action = "";
    private long endTimeMs;
    private String status = STATUS_FAILED;
    private String failureReason;
    private String detailsJson = "{}";

    private IcebergMaintenanceTaskRecord(String catalogName, String databaseName, String tableName,
                                         String triggerReason, String stmt, long startTimeMs) {
        this.taskId = UUID.randomUUID().toString();
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.triggerReason = triggerReason;
        this.stmt = stmt;
        this.startTimeMs = startTimeMs;
    }

    public static IcebergMaintenanceTaskRecord start(String catalogName, String databaseName, String tableName,
                                                     String triggerReason, String stmt) {
        return new IcebergMaintenanceTaskRecord(catalogName, databaseName, tableName, triggerReason, stmt,
                System.currentTimeMillis());
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setFailureReason(String failureReason) {
        if (failureReason != null && failureReason.length() > MAX_FAILURE_REASON_LENGTH) {
            failureReason = failureReason.substring(0, MAX_FAILURE_REASON_LENGTH);
        }
        this.failureReason = failureReason;
    }

    /**
     * Finalize the record with the procedure execution stats. Safe to call with
     * partially-filled stats (e.g. when the procedure threw early).
     */
    public void finish(IcebergMaintenanceTaskStats stats) {
        this.endTimeMs = System.currentTimeMillis();
        if (stats != null) {
            if (stats.getOperation() != null) {
                this.action = stats.getOperation().name().toLowerCase();
            }
            this.detailsJson = stats.toJson();
        }
    }

    public String getTaskId() {
        return taskId;
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

    public String getAction() {
        return action;
    }

    public String getTriggerReason() {
        return triggerReason;
    }

    public String getStmt() {
        return stmt;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public long getDurationMs() {
        return Math.max(0, endTimeMs - startTimeMs);
    }

    public String getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getDetailsJson() {
        return detailsJson;
    }
}
