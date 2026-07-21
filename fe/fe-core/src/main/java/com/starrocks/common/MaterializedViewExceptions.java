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

package com.starrocks.common;

import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * Encapsulate error message and exceptions for materialized view
 */
public class MaterializedViewExceptions {

    // reason for base table optimized, base table's partition is optimized which mv cannot be actived again.
    public static final String INACTIVE_REASON_FOR_BASE_TABLE_OPTIMIZED = "base-table optimized:";

    public static final String INACTIVE_REASON_FOR_BASE_TABLE_REORDER_COLUMNS = "base-table reordered columns:";

    public static final String INACTIVE_REASON_FOR_METADATA_TABLE_RESTORE_CORRUPTED = "metadata backup/restore mv corrupted:";

    public static final String INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES = "mv consecutive failures: ";

    public static final String INACTIVE_REASON_FOR_INCREMENTAL_BREAKING =
            "incremental refresh broken by non-append-only base change: ";

    /**
     * Create the inactive reason when base table not exists
     */
    public static String inactiveReasonForBaseTableNotExists(String tableName) {
        return "base-table dropped: " + tableName;
    }

    /**
     * Create the inactive reason when base table changed, eg: drop & recreated
     */
    public static String inactiveReasonForBaseTableChanged(String tableName) {
        return "base-table changed: " + tableName;
    }

    public static String inactiveReasonForBaseTableNotExists(long tableId) {
        return "base-table not exist: " + tableId;
    }

    public static String inactiveReasonForBaseTableRenamed(String tableName) {
        return "base-table renamed: " + tableName;
    }

    public static String inactiveReasonForBaseTableSwapped(String tableName) {
        return "base-table swapped: " + tableName;
    }

    public static String inactiveReasonForBaseTableOptimized(String tableName) {
        return INACTIVE_REASON_FOR_BASE_TABLE_OPTIMIZED + tableName;
    }

    public static String inactiveReasonForBaseTableReorderColumns(String tableName) {
        return INACTIVE_REASON_FOR_BASE_TABLE_REORDER_COLUMNS + tableName;
    }

    public static String inactiveReasonForIncrementalBreaking(String mvName) {
        return INACTIVE_REASON_FOR_INCREMENTAL_BREAKING + mvName;
    }

    // Canonical marker for a permanently-breaking (non-append-only) base change. MVIVMRefreshProcessor builds
    // its message from this constant and isIncrementalBreakingFailure matches it, so wording and detection can't drift.
    public static final String FE_NON_APPEND_ONLY_MARKER = "do not support non-append-only base changes";
    // BE ChangesDataSource rejects a delete it reads on a cloud-native CHANGES scan with this token
    // (be/src/connector/changes/changes_connector.cpp). OLAP deletes only surface here, not in the FE delta trait.
    private static final String BE_DELETE_PREDICATE_MARKER = "DELETE_PREDICATE_FOUND";

    /**
     * Whether an MV refresh failure is a non-append-only breakage that permanently disables incremental
     * refresh (as opposed to a transient error). Covers both the FE-detected non-append-only change and
     * the BE-detected delete on a cloud-native CHANGES scan, so a single caller handles OLAP and external
     * tables the same way. Walks the cause chain because the marker may be wrapped by the refresh pipeline.
     */
    public static boolean isIncrementalBreakingFailure(Throwable e) {
        for (Throwable t = e; t != null && t != t.getCause(); t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null
                    && (msg.contains(FE_NON_APPEND_ONLY_MARKER) || msg.contains(BE_DELETE_PREDICATE_MARKER))) {
                return true;
            }
        }
        return false;
    }

    public static String inactiveReasonForMetadataTableRestoreCorrupted(String tableName) {
        return INACTIVE_REASON_FOR_METADATA_TABLE_RESTORE_CORRUPTED + tableName;
    }

    public static String inactiveReasonForBaseTableInActive(String tableName) {
        return "base-mv inactive: " + tableName;
    }

    public static String inactiveReasonForBaseViewChanged(String tableName) {
        return "base-view changed: " + tableName;
    }

    public static String inactiveReasonForBaseInfoMissed() {
        return "base-info missed";
    }

    public static String inactiveReasonForDbNotExists(long dbId) {
        return "db not exists: " + dbId;
    }

    public static String inactiveReasonForColumnNotCompatible(String existingType, String newType) {
        return String.format("column schema not compatible: (%s) and (%s)", existingType, newType);
    }

    public static String inactiveReasonForColumnChanged(Set<String> columns) {
        return "base table schema changed for columns: " + StringUtils.join(columns, ",");
    }

    public static String inactiveReasonForSchemaCheckFailed(String mvName, String detail) {
        return "base table schema check failed for " + mvName + ": " + detail;
    }

    public static SemanticException reportBaseTableNotExists(String tableName) {
        return new SemanticException(inactiveReasonForBaseTableNotExists(tableName));
    }

    public static String inactiveReasonForConsecutiveFailures(String mvName) {
        return INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES + mvName;
    }

    public static String unsupportedReasonForLegacyIncrementalMaintenance() {
        return "Legacy incremental materialized view maintenance is no longer supported";
    }

    public static String unSupportedReasonForMVFSE(String reason) {
        return String.format("fast schema evolution failed: %s. Please use 1) 'CREATE a new MV " +
                "and use `SWAP MV` to replace the current', or 2) `ALTER MATERIALIZED VIEW <NAME> SET " +
                "('query_rewrite_consistency'='force_mv')` to force query rewrite. or 3) `ALTER MATERIALIZED VIEW " +
                "<NAME> set ('enable_query_rewrite'='false')` to disable query rewrite.", reason);
    }
}
