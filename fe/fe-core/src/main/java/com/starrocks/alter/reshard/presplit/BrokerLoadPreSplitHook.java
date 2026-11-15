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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * BrokerLoadJob → coordinator bridge for Sample-Based Tablet Pre-Split on the
 * Broker Load path.
 *
 * <p>The hook is invoked from {@code BrokerLoadJob.createLoadingTask} after
 * the per-table {@link OlapTable} reference and the broker-resolved file
 * statuses are snapshotted — but <b>before</b> {@code beginTxn()} opens
 * {@code T_load} and before any {@code LoadLoadingTask.prepare()} pins the
 * sink plan. Pre-created partitions and the post-reshard tablet layout
 * therefore accelerate the triggering Broker Load itself, symmetric with
 * the INSERT hook ({@link InsertPreSplitHook}).
 *
 * <p>The hook resolves the Broker Load source into a {@link PreSplitFlow.Prepared}
 * bundle and hands control to {@link PreSplitFlow#dispatch}, which sync-awaits the
 * reshard daemon's {@code FINISHED} transition on both the single-partition and
 * multi-partition paths. Both are fail-safe: on timeout / abort / wait failure the
 * flow logs and proceeds against whatever tablet layout is currently visible — never
 * aborts the triggering load. Sync-await is deadlock-safe here specifically
 * because {@code BrokerLoadJob.unprotectedExecute()} defers {@code beginTxn}
 * until <b>after</b> the hook returns, so the reshard daemon's cleanup-phase
 * {@code isPreviousTransactionsFinished(endTransactionId, ...)} wait cannot
 * include the not-yet-allocated {@code T_load}.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}: meta tier uses
 * {@link BrokerLoadRowGroupStatisticsProvider}, data tier uses
 * {@link BrokerLoadSampleSubqueryExecutor}. The per-path Config flag
 * {@code enable_tablet_pre_split_for_broker_load} defaults to
 * {@code true} as of v4.1.0 (GA flip); set it to {@code false} to disable
 * cluster-wide. The session variable {@code enable_tablet_pre_split}
 * (also default {@code true}) provides a per-session opt-out checked
 * early in this hook so a session-opt-out load does not pay the
 * eligibility-target walk and scan-context build. The Broker Load caller
 * resolves that opt-out from the value {@code BulkLoadJob} persisted when the
 * statement was accepted and hands it to {@code maybeRunPreSplit}, so a load
 * that has sat pending is not re-decided by whatever its submitter's session
 * holds now.
 */
public final class BrokerLoadPreSplitHook {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPreSplitHook.class);

    private BrokerLoadPreSplitHook() {
    }

    /**
     * Entry point invoked from {@code BrokerLoadJob.createLoadingTask}.
     *
     * <p>The method is fully self-contained: any throw is swallowed and the
     * load proceeds without pre-split. Failing here must not abort an
     * otherwise-valid load.
     *
     * <p>{@code shouldAbort} is polled between reshard-daemon polls so that
     * {@code processTimeout} / user-cancel releases the
     * {@code pending_load_task_scheduler} slot promptly rather than waiting
     * out the {@code tablet_pre_split_post_submit_wait_seconds} ceiling. A
     * {@code true} return short-circuits the wait without aborting the load.
     * Pass {@code () -> false} to disable.
     *
     * @param context      the load's {@link ConnectContext}. Mirrors the
     *                     {@link InsertPreSplitHook} parameter
     *                     threading: passing the context explicitly avoids
     *                     a thread-local fallback that would create an
     *                     uninitialized {@link ConnectContext} (no auth /
     *                     no session vars / no current DB) and surface as
     *                     a confusing analyze-time NPE inside
     *                     {@link PartitionSampleGrouper}.
     * @param fileStatuses nested per-file-group file statuses from
     *                     {@code BrokerPendingTaskAttachment.getFileStatusByTable}.
     */
    public static void maybeRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource, BooleanSupplier shouldAbort) {
        maybeRunPreSplit(context, database, targetTable, brokerDesc, fileGroups, fileStatuses,
                computeResource, shouldAbort, null, null);
    }

    /**
     * @param sessionPreSplitEnabled the {@code enable_tablet_pre_split} opt-out as the calling load
     *                               resolved it, or {@code null} to read it off {@code context}.
     *                               {@code BrokerLoadJob} passes the submit-time snapshot: outside an
     *                               FE failover {@code context} is the submitter's own still-live
     *                               session, so reading it here would let a {@code SET} issued while
     *                               the job sat pending re-decide a load already accepted — and after
     *                               a failover the recreated context carries only the default.
     */
    public static void maybeRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource, BooleanSupplier shouldAbort, PreSplitProfile profile,
            Boolean sessionPreSplitEnabled) {
        try {
            tryRunPreSplit(context, database, targetTable, brokerDesc, fileGroups, fileStatuses,
                    computeResource, shouldAbort, profile, sessionPreSplitEnabled);
        } catch (Throwable unexpected) {
            PreSplitProfile.recordOutcome(profile, "FAILED_FALLBACK");
            LOG.warn("Sample-Based Tablet Pre-Split hook failed for Broker Load; proceeding without pre-split",
                    unexpected);
        }
    }

    private static void tryRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource, BooleanSupplier shouldAbort, PreSplitProfile profile,
            Boolean sessionPreSplitEnabled) {
        Objects.requireNonNull(context, "context");
        if (!Config.enable_tablet_pre_split_for_broker_load) {
            // Record here: the coordinator's checkConfigAndSession is never
            // reached on this early return, so it can't bump the bucket itself.
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DISABLED_BY_CONFIG);
            return;
        }
        // Honor the per-session opt-out before target resolution + scan-context
        // build. sessionPreSplitEnabled is the value the submitting session held when
        // LOAD LABEL was accepted; null means no caller resolved it, so fall back to the
        // session this hook runs under. The helper bumps the disabled_by_session bvar —
        // the coordinator never sees this skip, but operators still need the bvar.
        boolean preSplitEnabled = sessionPreSplitEnabled != null
                ? sessionPreSplitEnabled
                : context.getSessionVariable().isEnableTabletPreSplit();
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(preSplitEnabled)) {
            return;
        }
        // BrokerLoadJob.createLoadingTask guarantees database / targetTable / computeResource are
        // non-null by the time the hook fires; fileGroups / fileStatuses come from the attachment
        // and the pending-task contract makes them non-null too, but we tolerate null defensively.
        if (fileGroups == null || fileStatuses == null) {
            return;
        }
        try (PreSplitProfile.Scope ignored = profile == null
                ? PreSplitProfile.startAttempt(context, LoadKind.BROKER_LOAD)
                : PreSplitProfile.startAttempt(profile, LoadKind.BROKER_LOAD)) {
            PreSplitProfile.recordTable(targetTable.getName());
            // Table-level eligibility: structural checks shared with the multi-partition
            // coordinator's defensive re-check. Per-partition checks (single physical
            // partition, single base tablet, empty partition) remain with the legacy
            // single-partition path; the multi-partition path runs them per-bucket
            // after pre-create under its own short READ lock.
            SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(database, targetTable);
            if (tableLevelSkip != null) {
                PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
                PreSplitProfile.recordOutcome("SKIPPED: " + tableLevelSkip);
                return;
            }
            List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(targetTable);
            List<Column> partitionColumns =
                    targetTable.getPartitionInfo().getPartitionColumns(targetTable.getIdToColumn());
            List<SecondaryIndexSpec> secondaryIndexSpecs = SecondaryIndexSpec.forVisibleRollups(targetTable);
            List<Column> sampledColumns =
                    InsertSelectSourceColumns.sampledColumns(sortKeyColumns, partitionColumns, secondaryIndexSpecs);
            InsertSelectSourceColumns.Resolved generated =
                    resolveGeneratedSampledColumns(targetTable, fileGroups, sampledColumns, context);
            if (generated == null) {
                return;
            }
            PreSplitPartitionScope partitionScope = partitionScopeOf(targetTable, fileGroups);
            if (partitionScope == null) {
                LOG.info("Sample-Based Tablet Pre-Split: Broker Load into table {} names partitions that no single "
                        + "partition scope describes (temporary and normal partitions mixed, or one was dropped); "
                        + "skipping pre-split", targetTable.getName());
                PreSplitProfile.recordOutcome("SKIPPED: UNSUPPORTED_PARTITION_SCOPE");
                return;
            }
            // The load session timezone. This same context feeds JobSpec.fromBrokerLoadJobSpec ->
            // loadPlanner.getContext() for the BE query globals, so it matches the offset the BE applies to
            // a UTC-adjusted / TIMESTAMP_INSTANT value. A non-fixed zone -> the readers defer to data tier.
            BrokerLoadScanContext scanContext = new BrokerLoadScanContext(
                    brokerDesc, fileGroups, fileStatuses, computeResource, context.getSessionVariable().getTimeZone(),
                    // Copied, not aliased: this is the positional field layout a CSV file group with no
                    // COLUMNS list inherits, and it must not shift under a later alter.
                    List.copyOf(targetTable.getBaseSchema()),
                    generated.targetToConstantSql(), generated.targetToExpressionSql(),
                    generatedColumnInputs(targetTable, sampledColumns),
                    // Copied by value now: outside an FE failover this context holds the submitter's own live
                    // session, the one LoadPlanner reads as well.
                    SampleSessionSemantics.capture(context.getSessionVariable()));
            PreSplitFlow.Prepared prepared = new PreSplitFlow.Prepared(
                    scanContext,
                    sortKeyColumns,
                    partitionColumns,
                    sumFileBytes(fileStatuses),
                    computeResource,
                    secondaryIndexSpecs,
                    preSplitEnabled);
            PreSplitFlow.dispatch(database, targetTable, prepared, LoadKind.BROKER_LOAD, shouldAbort, context,
                    partitionScope);
        }
    }

    /**
     * Resolves the generated columns among {@code sampledColumns} over the columns every file group reads
     * straight from its files, by name -- the inputs {@code Load} computes a generated column from. Returns
     * a projection with empty maps when no sampled column is generated, or {@code null} after recording
     * the skip when one cannot be computed.
     */
    static InsertSelectSourceColumns.Resolved resolveGeneratedSampledColumns(
            OlapTable target, List<BrokerFileGroup> fileGroups, List<Column> sampledColumns, ConnectContext context) {
        List<Column> generatedColumns = sampledColumns.stream().filter(Column::isGeneratedColumn).toList();
        if (generatedColumns.isEmpty()) {
            return new InsertSelectSourceColumns.Resolved(Map.of(), Map.of(), Map.of(), Set.of());
        }
        // Load converts each input to its column's type before computing a generated column, which is what the
        // sampler does. The sampler's FILES() relation has no qualifier and every substituted reference is
        // unqualified, so the safety rule never compares a table name here.
        InsertSelectSourceColumns.Resolved resolved = InsertSelectSourceColumns.withGeneratedColumns(
                columnsReadFromFiles(target, fileGroups), target, generatedColumns,
                InsertSelectSourceColumns.InputReading.AS_COLUMN_TYPE, context, /*sourceName*/ null, /*sourceAlias*/ null);
        InsertSelectSourceColumns.Unsampleable unsampleable = InsertSelectSourceColumns.firstUnsampleable(
                generatedColumns, resolved, InsertSelectSourceColumns.InputReading.AS_COLUMN_TYPE);
        if (unsampleable != null) {
            unsampleable.record(target.getName());
            return null;
        }
        return resolved;
    }

    /**
     * The non-generated target columns every file group reads straight from its files, mapped to themselves
     * by name: the bare entries of a COLUMNS list, or -- for a group with no COLUMNS list -- every
     * non-generated column, which {@code Load.initColumns} reads by name. A column some group maps with SET
     * is the mapped value even when its bare name is listed too ({@code Load} applies the SET entries
     * last), so it is reported as an unsupported projection instead.
     */
    private static InsertSelectSourceColumns.Resolved columnsReadFromFiles(
            OlapTable target, List<BrokerFileGroup> fileGroups) {
        Map<String, String> namesByLowerCase = new HashMap<>();
        for (Column column : target.getBaseSchemaWithoutGeneratedColumn()) {
            namesByLowerCase.put(column.getName().toLowerCase(), column.getName());
        }
        Set<String> readByEveryGroup = new HashSet<>(namesByLowerCase.keySet());
        Set<String> setMapped = new HashSet<>();
        for (BrokerFileGroup fileGroup : fileGroups) {
            List<ImportColumnDesc> columnExpressions = fileGroup.getColumnExprList();
            if (columnExpressions == null || columnExpressions.isEmpty()) {
                continue;
            }
            Set<String> named = new HashSet<>();
            for (ImportColumnDesc columnExpression : columnExpressions) {
                if (columnExpression.isColumn()) {
                    named.add(columnExpression.getColumnName().toLowerCase());
                } else {
                    setMapped.add(columnExpression.getColumnName().toLowerCase());
                }
            }
            // A list made only of SET mappings names no file field, so Load reads every column by its own name.
            if (!named.isEmpty()) {
                readByEveryGroup.retainAll(named);
            }
        }
        readByEveryGroup.removeAll(setMapped);
        setMapped.retainAll(namesByLowerCase.keySet());
        Map<String, String> readFromFiles = new HashMap<>();
        for (String name : readByEveryGroup) {
            readFromFiles.put(name, namesByLowerCase.get(name));
        }
        return new InsertSelectSourceColumns.Resolved(readFromFiles, Map.of(), Map.of(), setMapped);
    }

    /**
     * The partitions the load may write. A file group that names partitions ({@code PARTITION(...)}) turns
     * automatic partition creation off for the whole load ({@code LoadPlanner}), so pre-split must neither
     * create nor split a partition outside them; a group that names none may still write any existing
     * partition. {@code unrestricted()} when no group names partitions, or the target is unpartitioned
     * (its single partition carries the table's name); {@code null} when the named partitions mix real and
     * temporary ones, or one has been dropped, which no single scope describes.
     */
    static PreSplitPartitionScope partitionScopeOf(OlapTable target, List<BrokerFileGroup> fileGroups) {
        if (!target.getPartitionInfo().isPartitioned()
                || fileGroups.stream().noneMatch(BrokerFileGroup::isSpecifyPartition)) {
            return PreSplitPartitionScope.unrestricted();
        }
        Set<String> partitionNames = new LinkedHashSet<>();
        Set<Boolean> temporary = new HashSet<>();
        for (BrokerFileGroup fileGroup : fileGroups) {
            if (!fileGroup.isSpecifyPartition()) {
                for (Partition partition : target.getPartitions()) {
                    partitionNames.add(partition.getName());
                }
                temporary.add(false);
                continue;
            }
            for (long partitionId : fileGroup.getPartitionIds()) {
                Partition partition = target.getPartition(partitionId);
                if (partition == null) {
                    return null;
                }
                partitionNames.add(partition.getName());
                temporary.add(target.getPartition(partition.getName(), false) != partition);
            }
        }
        return temporary.size() == 1
                ? PreSplitPartitionScope.explicit(partitionNames, temporary.iterator().next()) : null;
    }

    /** The target columns the sampled generated columns read, which a CSV sample declares in its schema. */
    private static List<Column> generatedColumnInputs(OlapTable target, List<Column> sampledColumns) {
        Map<String, Column> inputs = new LinkedHashMap<>();
        for (Column column : sampledColumns) {
            if (!column.isGeneratedColumn()) {
                continue;
            }
            for (SlotRef reference : column.getGeneratedColumnRef(target.getIdToColumn())) {
                Column input = target.getIdToColumn().get(reference.getColumnId());
                inputs.putIfAbsent(input.getName().toLowerCase(), input);
            }
        }
        return List.copyOf(inputs.values());
    }

    private static long sumFileBytes(List<List<TBrokerFileStatus>> fileStatuses) {
        long total = 0L;
        for (List<TBrokerFileStatus> fileGroupStatuses : fileStatuses) {
            if (fileGroupStatuses == null) {
                continue;
            }
            for (TBrokerFileStatus fileStatus : fileGroupStatuses) {
                if (fileStatus != null) {
                    total += fileStatus.size;
                }
            }
        }
        return total;
    }
}
