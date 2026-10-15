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

<<<<<<< HEAD
=======
import com.starrocks.common.util.ParseUtil;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;

<<<<<<< HEAD
=======
import java.util.Arrays;
import java.util.Optional;
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
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

    public static final String INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH =
            "incremental refresh broken: materialized view schema no longer matches its definition: ";

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

<<<<<<< HEAD
    /**
     * Which reason to inactivate with, for a failure {@link #isIncrementalBreakingFailure} accepted.
     * A base-table column change and a column ALTER on the MV itself raise the same two schema
     * markers, so that branch states the condition without attributing a cause it cannot tell apart.
     */
    public static String inactiveReasonForBreakingFailure(Throwable e, String mvName) {
        for (Throwable t = e; t != null && t != t.getCause(); t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains(MV_SCHEMA_COLUMN_CHANGED_MARKER)
                    || msg.contains(MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER))) {
                return INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH + mvName;
            }
        }
        return inactiveReasonForIncrementalBreaking(mvName);
    }

    // Canonical marker for a permanently-breaking (non-append-only) base change. MVIVMRefreshProcessor builds
    // its message from this constant and isIncrementalBreakingFailure matches it, so wording and detection can't drift.
    public static final String FE_NON_APPEND_ONLY_MARKER = "do not support non-append-only base changes";

    // Both classifiers of a stored schema that no longer matches the maintenance query --
    // isIncrementalBreakingFailure and MVRefreshSchemaChecker#isLikelyDriftException -- match these,
    // so wording and detection can't drift apart.
    public static final String MV_SCHEMA_COLUMN_CHANGED_MARKER = "base table schema changed for columns: ";
    public static final String MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER = "column schema not compatible: ";

    /**
     * Whether an MV refresh failure is a non-append-only breakage that permanently disables incremental
     * refresh (vs. a transient error). Covers the FE-detected non-append-only change and a stored schema
     * that no longer matches the maintenance query, so a single caller can inactivate the MV. Walks the
     * cause chain since the marker may be wrapped by the refresh pipeline.
     */
    public static boolean isIncrementalBreakingFailure(Throwable e) {
        for (Throwable t = e; t != null && t != t.getCause(); t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains(FE_NON_APPEND_ONLY_MARKER)
                    || msg.contains(MV_SCHEMA_COLUMN_CHANGED_MARKER)
                    || msg.contains(MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER))) {
                return true;
            }
        }
        return false;
=======
    public static boolean isRecoveredByForceRefresh(String inactiveReason) {
        return inactiveReason != null && Arrays.stream(RefreshFailureKind.values())
                .anyMatch(kind -> kind.isRecoveredByForce() && inactiveReason.startsWith(kind.inactiveReasonPrefix()));
    }

    public static String recoverByRebuildHint(String dbName, String mvName) {
        return String.format("Drop and recreate the materialized view, or run REFRESH MATERIALIZED VIEW %s.%s FORCE, "
                + "to recover.", ParseUtil.backquote(dbName), ParseUtil.backquote(mvName));
    }

    // Wording of the breakages the frontend detects; the failure's kind, not this text, decides what happens next.
    public static final String FE_NON_APPEND_ONLY_MARKER = "do not support non-append-only base changes";

    // The bookmark an incremental MV recorded as its baseline no longer exists. Only the baseline endpoint
    // is typed as missing: the other endpoint is the bookmark the current round just took, and losing that
    // one is transient -- the next round takes a new one.
    public static final String BASELINE_BOOKMARK_MISSING_MARKER = "from-snapshot bookmark not found";

    // How a view reports that the baseline it refreshes from cannot be reached any more -- an expired
    // Iceberg snapshot or a reclaimed Lake bookmark.
    public static final String SNAPSHOT_ANCESTRY_BROKEN_MARKER = "snapshot ancestry broken";

    public static final String MV_SCHEMA_COLUMN_CHANGED_MARKER = "base table schema changed for columns: ";
    public static final String MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER = "column schema not compatible: ";

    /** The kind of the first classified failure along the cause chain; suppressed failures were handled already. */
    public static Optional<RefreshFailureKind> kindOf(Throwable e) {
        return ExceptionUtils.getThrowableList(e).stream()
                .filter(ClassifiedRefreshFailure.class::isInstance)
                .map(t -> ((ClassifiedRefreshFailure) t).kind())
                .findFirst();
>>>>>>> 103084cf4fc... [Refactor][RB-390] Classify MV refresh failures by type instead of message text (#65255)
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
        return String.format("%s(%s) and (%s)", MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER, existingType, newType);
    }

    public static String inactiveReasonForColumnChanged(Set<String> columns) {
        return MV_SCHEMA_COLUMN_CHANGED_MARKER + StringUtils.join(columns, ",");
    }

    public static String unsupportedReasonForIvmColumnChange(String operation, String columnName, String mvName) {
        return String.format("Cannot %s column '%s' on incrementally maintained materialized view '%s': its "
                        + "stored schema carries hidden columns whose layout is fixed at creation, so a column "
                        + "change breaks every later incremental refresh. Rebuild instead: CREATE a new "
                        + "materialized view with the columns you want, REFRESH it, 'ALTER MATERIALIZED VIEW %s "
                        + "SWAP WITH <new_mv>', then DROP the swapped-out one.",
                operation, columnName, mvName, mvName);
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
