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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Groups a {@link SampleSet}'s rows into per-target-partition buckets ready
 * for the multi-partition pre-split coordinator.
 *
 * <p>For each distinct partition-value tuple in the sample, the grouper
 * formats the {@link Variant} cells through {@link PartitionValueFormatter}
 * (so DATE / DATETIME round-trip into SQL-parseable strings the analyzer
 * accepts), then derives + analyzes an {@link AddPartitionClause} once per
 * distinct value (the analyzer is the expensive step). Raw groups that resolve
 * to the same partition name (e.g. two timestamps in the same day under
 * {@code PARTITION BY date_trunc('day', ts)}) are merged into one logical group
 * so the downstream coordinator sees exactly one entry per partition. The
 * analyzer runs OUTSIDE the table lock, mirroring
 * {@code FrontendServiceImpl#parseAddPartitionClause} (which analyzes without
 * holding the table READ lock; {@code addPartitions} locks later). Only the
 * catalog reads (partition existence, tablet count, row count) are performed
 * under an intensive table READ lock; the lock is released before this method
 * returns so the coordinator can acquire its own WRITE lock for
 * {@code addPartitions}.
 *
 * <p>The result is capped at {@link Config#tablet_pre_split_max_partitions_per_load}.
 * Excess partitions (those with the lowest sample count) are dropped and the
 * {@code tablet_pre_split_partitions_capped} bvar is incremented per drop.
 */
public final class PartitionSampleGrouper {

    private static final Logger LOG = LogManager.getLogger(PartitionSampleGrouper.class);

    private PartitionSampleGrouper() {
    }

    /**
     * @param samples         output of the data-tier sampler; must carry
     *                        {@code partitionSourceTuples} parallel to
     *                        {@code tuples}
     * @param table           load target
     * @param ctx             ConnectContext for the analyzer pass
     * @param dbId            db id for the intensive lock scope
     * @param totalFileBytes  total bytes of the load's input (per {@code Estimates});
     *                        used to apportion {@code estimatedBytes} across groups
     * @param sampledSecondaryIndexMetaIds  authoritative secondary index-id set the
     *                        sampler projected ({@link SampleSet#getSecondaryIndexMetaIds()});
     *                        an existing partition whose currently-resolved secondary
     *                        index-id set differs is dropped as ineligible (a rollup
     *                        appeared or vanished since the sample)
     * @return list of {@link PartitionSamples} sorted by sample-count descending
     *         (heaviest first), capped at
     *         {@link Config#tablet_pre_split_max_partitions_per_load}; possibly
     *         empty
     */
    public static List<PartitionSamples> group(SampleSet samples, OlapTable table, ConnectContext ctx,
                                               long dbId, long totalFileBytes,
                                               Set<Long> sampledSecondaryIndexMetaIds) {
        return group(samples, table, ctx, dbId, totalFileBytes, sampledSecondaryIndexMetaIds,
                false, null, PreSplitPartitionScope.unrestricted());
    }

    /** Explicit real/temporary partition variant. Never creates a partition outside the SQL scope. */
    public static List<PartitionSamples> groupSpecified(
            SampleSet samples, OlapTable table, ConnectContext ctx, long dbId, long totalFileBytes,
            Set<Long> sampledSecondaryIndexMetaIds, PreSplitPartitionScope partitionScope) {
        return group(samples, table, ctx, dbId, totalFileBytes, sampledSecondaryIndexMetaIds,
                partitionScope.isTemporary(), null, partitionScope);
    }

    /**
     * Dynamic-overwrite variant. Resolves sampled values to transaction-scoped temporary
     * partitions using the same naming convention as the runtime auto-partition RPC.
     */
    public static List<PartitionSamples> groupTemporary(
            SampleSet samples, OlapTable table, ConnectContext ctx, long dbId, long totalFileBytes,
            Set<Long> sampledSecondaryIndexMetaIds, long transactionId) {
        return group(samples, table, ctx, dbId, totalFileBytes, sampledSecondaryIndexMetaIds,
                true, "txn" + transactionId, PreSplitPartitionScope.unrestricted());
    }

    private static List<PartitionSamples> group(
            SampleSet samples, OlapTable table, ConnectContext ctx, long dbId, long totalFileBytes,
            Set<Long> sampledSecondaryIndexMetaIds, boolean temporaryPartition, String partitionNamePrefix,
            PreSplitPartitionScope partitionScope) {
        List<Tuple> partitionSourceTuples = samples.getPartitionSourceTuples();
        if (partitionSourceTuples.isEmpty()) {
            // Sampler did not project partition source columns (target was unpartitioned).
            return Collections.emptyList();
        }

        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Column> partitionColumns = partitionInfo.getPartitionColumns(table.getIdToColumn());
        if (partitionColumns.isEmpty()) {
            return Collections.emptyList();
        }
        PartitionValueFormatter[] formatters = new PartitionValueFormatter[partitionColumns.size()];
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (!PartitionValueFormatter.isSupportedColumnType(partitionColumns.get(i))) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.UNSUPPORTED_PARTITION_COLUMN_TYPE);
                LOG.info("Pre-split: unsupported partition column type {} on table {}",
                        partitionColumns.get(i).getType(), table.getName());
                return Collections.emptyList();
            }
            formatters[i] = new PartitionValueFormatter(partitionColumns.get(i));
        }

        // Pass 1 (no lock): format every row's partition tuple and bucket rows by
        // formatted raw-value key. This is a cheap dedup of identical raw tuples;
        // the analyzer is NOT invoked here.
        Map<List<String>, RawGroup> rawGroups = new LinkedHashMap<>();
        List<Tuple> sortKeyTuples = samples.getTuples();
        for (int rowIndex = 0; rowIndex < sortKeyTuples.size(); rowIndex++) {
            List<Variant> partitionSourceTuple = partitionSourceTuples.get(rowIndex).getValues();
            List<String> formatted = formatPartitionTuple(partitionSourceTuple, formatters);
            if (formatted == null) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.INVALID_PARTITION_VALUE);
                continue;
            }
            RawGroup group = rawGroups.get(formatted);
            if (group == null) {
                group = new RawGroup(formatted);
                rawGroups.put(formatted, group);
            }
            // Carry the id-tagged secondary-index tuples through only when the sampler
            // projected them; getSecondaryIndexTuples() is empty for single-index targets,
            // so never index into it in that case.
            List<IndexTuple> secondaryIndexTuples = samples.getSecondaryIndexTuples().isEmpty()
                    ? List.of() : samples.getSecondaryIndexTuples().get(rowIndex);
            group.rows.add(new SampleRow(
                    sortKeyTuples.get(rowIndex).getValues(), partitionSourceTuple, secondaryIndexTuples));
        }

        if (rawGroups.isEmpty()) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.GROUPER_EMPTY);
            return Collections.emptyList();
        }

        // Analyze + merge phase (no lock): resolve each raw group to its target
        // partition name and merge raw groups that resolve to the SAME partition
        // into one logical group. For expression partitioning (e.g.
        // PARTITION BY date_trunc('day', ts)), two raw values within the same day
        // resolve to one partition; without this merge they would emit two
        // PartitionSamples with the same partitionName + oldTabletId, and the
        // coordinator's oldTabletIdToRanges.put would silently drop one.
        //
        // The analyzer runs OUTSIDE the intensive table READ lock here, mirroring
        // the runtime auto-create path: FrontendServiceImpl.parseAddPartitionClause
        // analyzes without holding the table READ lock and addPartitions does its
        // own locking later.
        Map<String, MergedGroup> byPartitionName = new LinkedHashMap<>();
        for (RawGroup rawGroup : rawGroups.values()) {
            AnalyzedClauseEntry entry = analyzeOnce(
                    table, rawGroup.formattedValues, ctx, temporaryPartition, partitionNamePrefix);
            if (entry == null) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.INVALID_PARTITION_VALUE);
                continue;
            }
            MergedGroup merged = byPartitionName.get(entry.partitionName);
            if (merged == null) {
                // The first raw group that resolves to this partition supplies the
                // representative formatted values + analyzed clause; later raw
                // groups resolve to the same partition so their clause is equivalent.
                merged = new MergedGroup(entry.partitionName, rawGroup.formattedValues, entry.clause);
                byPartitionName.put(entry.partitionName, merged);
            }
            merged.rows.addAll(rawGroup.rows);
        }

        if (byPartitionName.isEmpty()) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.GROUPER_EMPTY);
            return Collections.emptyList();
        }

        // Scope phase (no lock): when the INSERT named its target partitions, drop every group that
        // resolves outside that set BEFORE the cap ranks anything. Capping first lets out-of-scope
        // partitions with more sampled rows evict the named target, and the scope check further down
        // then discards whatever the cap kept -- so an explicitly targeted INSERT or overwrite would
        // silently receive no pre-split at all. Resolving here also memoizes the mapping, so the
        // locked pass below does not repeat the range-fallback scan. Catalog name lookups are safe
        // outside the lock for the same reason the analyzer above is: the locked pass re-resolves
        // every partition it actually uses.
        Map<String, String> scopedCatalogNames = new LinkedHashMap<>();
        List<MergedGroup> mergedGroups;
        if (partitionScope.isSpecified()) {
            mergedGroups = new ArrayList<>();
            for (MergedGroup group : byPartitionName.values()) {
                String catalogPartitionName =
                        resolveCatalogPartitionName(table, group, partitionColumns, partitionScope);
                if (catalogPartitionName == null) {
                    // The sample row belongs to a partition outside the explicit INSERT target.
                    continue;
                }
                scopedCatalogNames.put(group.partitionName, catalogPartitionName);
                mergedGroups.add(group);
            }
            if (mergedGroups.isEmpty()) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.GROUPER_EMPTY);
                return Collections.emptyList();
            }
        } else {
            mergedGroups = new ArrayList<>(byPartitionName.values());
        }

        // Cap phase (no lock): rank merged groups heaviest-first and trim to the
        // per-load cap BEFORE the catalog lock so both analyzer (already done) and
        // catalog work are bounded for high-cardinality loads.
        mergedGroups.sort((left, right) -> Integer.compare(right.rows.size(), left.rows.size()));
        int cap = Config.tablet_pre_split_max_partitions_per_load;
        if (cap > 0 && mergedGroups.size() > cap) {
            for (int i = cap; i < mergedGroups.size(); i++) {
                PreSplitMetrics.recordPartitionCapped();
            }
            mergedGroups = new ArrayList<>(mergedGroups.subList(0, cap));
        }

        long sampleRowCount = sortKeyTuples.size();
        List<PartitionSamples> resolved = new ArrayList<>(mergedGroups.size());

        // Pass 2 (intensive table READ lock): catalog lookups only. The merged
        // groups arrive heaviest-first from the cap phase; that order is preserved
        // (no re-sort) so downstream callers still get a deterministic ordering.
        try (AutoCloseableLock ignored = new AutoCloseableLock(
                new Locker(), dbId, Lists.newArrayList(table.getId()), LockType.READ)) {
            for (MergedGroup group : mergedGroups) {
                long estimatedBytes = sampleRowCount == 0 ? 0L :
                        Math.round((double) group.rows.size() / sampleRowCount * totalFileBytes);

                // Resolved by the scope phase above for an explicit target; an unrestricted load
                // writes the deterministic auto-partition name itself.
                String catalogPartitionName =
                        scopedCatalogNames.getOrDefault(group.partitionName, group.partitionName);
                Partition partition = temporaryPartition
                        ? table.getPartition(catalogPartitionName, true)
                        : table.getPartition(catalogPartitionName);
                if (partition == null) {
                    if (partitionScope.isSpecified()) {
                        PreSplitMetrics.recordEligibilitySkip(SkipReason.STALE_CATALOG_STATE);
                        continue;
                    }
                    resolved.add(new PartitionSamples(
                            group.formattedValues, catalogPartitionName, false,
                            -1L, -1L, group.clause,
                            ImmutableList.copyOf(group.rows), estimatedBytes));
                    continue;
                }
                PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
                if (physicalPartition == null) {
                    // Catalog raced: partition existed by name but physical partition vanished.
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.STALE_CATALOG_STATE);
                    LOG.info("Pre-split skipped: catalog state changed for partition {} "
                            + "(physical partition missing on table {})",
                            group.partitionName, table.getName());
                    continue;
                }
                MaterializedIndex baseIndex = physicalPartition.getIndex(table.getBaseIndexMetaId());
                if (baseIndex == null) {
                    // Catalog raced: base index dropped between getPartition and getIndex.
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.STALE_CATALOG_STATE);
                    LOG.info("Pre-split skipped: catalog state changed for partition {} "
                            + "(base index missing on table {})",
                            group.partitionName, table.getName());
                    continue;
                }
                if (baseIndex.getTablets().size() != 1 || baseIndex.getRowCount() > 0) {
                    // Existing partition is non-empty or multi-tablet, so it is not
                    // eligible for pre-split. Record the specific skip reason at the
                    // drop site so operators can tell these apart from a truly empty
                    // grouper result.
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
                    continue;
                }
                // Resolve every visible index (base + rollups) and confirm the resolved
                // secondary index-id set still matches what the sampler projected. A
                // rollup that appeared or vanished since the sample, or a rollup that no
                // longer resolves (multi-tablet / non-scalar sort key), drops the whole
                // partition -- the authoritative re-check runs again at plan time. The base
                // oldTabletId stays on PartitionSamples; per-index targets are re-resolved
                // at plan time, not carried from here.
                List<IndexPreSplitTarget> indexTargets =
                        PreSplitTargets.resolveVisibleIndexTargets(table, physicalPartition);
                if (indexTargets == null
                        || !resolvedSecondaryIndexMetaIds(indexTargets).equals(sampledSecondaryIndexMetaIds)) {
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
                    continue;
                }
                long oldTabletId = baseIndex.getTablets().get(0).getId();
                resolved.add(new PartitionSamples(
                        group.formattedValues, catalogPartitionName, true,
                        physicalPartition.getId(), oldTabletId, null,
                        ImmutableList.copyOf(group.rows), estimatedBytes));
            }
        }

        // Do NOT record a second GROUPER_EMPTY when resolved is empty here: the
        // per-drop reasons (PARTITION_NOT_ELIGIBLE_POST_CREATE / STALE_CATALOG_STATE)
        // already explain why each group was dropped. There WERE groups; they were
        // ineligible, not empty. Return the (possibly empty) heaviest-first list.
        return resolved;
    }

    /**
     * Resolves the deterministic auto-partition name to a partition inside the explicit scope.
     * Static overwrite normally hits the direct source-to-temp mapping. For an explicitly named
     * custom real/temp range partition, compare the analyzed fixed range with the catalog range so
     * correctness does not depend on a naming convention.
     */
    private static String resolveCatalogPartitionName(
            OlapTable table, MergedGroup group, List<Column> partitionColumns,
            PreSplitPartitionScope partitionScope) {
        if (!partitionScope.isSpecified()) {
            return null;
        }
        String mapped = partitionScope.mappedCatalogName(group.partitionName);
        if (mapped != null) {
            return mapped;
        }
        if (!(table.getPartitionInfo() instanceof RangePartitionInfo rangePartitionInfo)) {
            return null;
        }
        Range<PartitionKey> sampledRange = rangeFromAnalyzedClause(group.clause, partitionColumns);
        if (sampledRange == null) {
            return null;
        }
        for (String partitionName : partitionScope.catalogPartitionNames()) {
            Partition candidate = table.getPartition(partitionName, partitionScope.isTemporary());
            if (candidate != null && sampledRange.equals(rangePartitionInfo.getRange(candidate.getId()))) {
                return partitionName;
            }
        }
        return null;
    }

    private static Range<PartitionKey> rangeFromAnalyzedClause(
            AddPartitionClause clause, List<Column> partitionColumns) {
        if (clause == null || clause.getResolvedPartitionDescList() == null
                || clause.getResolvedPartitionDescList().size() != 1
                || !(clause.getResolvedPartitionDescList().get(0) instanceof SingleRangePartitionDesc desc)) {
            return null;
        }
        PartitionKeyDesc keyDesc = desc.getPartitionKeyDesc();
        if (keyDesc.getPartitionType() != PartitionKeyDesc.PartitionRangeType.FIXED
                || !keyDesc.hasLowerValues() || !keyDesc.hasUpperValues()) {
            return null;
        }
        try {
            PartitionKey lower = PartitionKey.createPartitionKey(keyDesc.getLowerValues(), partitionColumns);
            PartitionKey upper = PartitionKey.createPartitionKey(keyDesc.getUpperValues(), partitionColumns);
            return Range.closedOpen(lower, upper);
        } catch (Throwable invalidRange) {
            LOG.debug("Pre-split: cannot resolve analyzed range for explicit partition {}: {}",
                    desc.getPartitionName(), invalidRange.getMessage());
            return null;
        }
    }

    /**
     * The secondary (non-base) index-meta ids of a base-first
     * {@link IndexPreSplitTarget} list -- the base is always {@code targets.get(0)}.
     */
    private static Set<Long> resolvedSecondaryIndexMetaIds(List<IndexPreSplitTarget> indexTargets) {
        Set<Long> secondaryIds = new HashSet<>();
        for (int i = 1; i < indexTargets.size(); i++) {
            secondaryIds.add(indexTargets.get(i).indexMetaId());
        }
        return secondaryIds;
    }

    private static List<String> formatPartitionTuple(List<Variant> partitionSourceTuple,
                                                     PartitionValueFormatter[] formatters) {
        if (partitionSourceTuple.size() != formatters.length) {
            return null;
        }
        List<String> formattedValues = new ArrayList<>(partitionSourceTuple.size());
        for (int i = 0; i < partitionSourceTuple.size(); i++) {
            String formatted;
            try {
                formatted = formatters[i].format(partitionSourceTuple.get(i));
            } catch (RuntimeException e) {
                LOG.debug("Pre-split: failed to format partition cell {}: {}",
                        partitionSourceTuple.get(i), e.getMessage());
                return null;
            }
            if (formatted == null) {
                return null;
            }
            formattedValues.add(formatted);
        }
        return formattedValues;
    }

    /**
     * Build + analyze an {@link AddPartitionClause} for one formatted partition
     * value. Returns {@code null} if the analyzer rejects the value (caller
     * counts under {@link SkipReason#INVALID_PARTITION_VALUE}).
     */
    private static AnalyzedClauseEntry analyzeOnce(
            OlapTable table, List<String> formattedValues, ConnectContext ctx,
            boolean temporaryPartition, String partitionNamePrefix) {
        try {
            AddPartitionClause clause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                    table, Collections.singletonList(formattedValues), temporaryPartition, partitionNamePrefix);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
            analyzer.analyze(ctx, clause);
            List<PartitionDesc> resolved = clause.getResolvedPartitionDescList();
            if (resolved == null || resolved.isEmpty()) {
                return null;
            }
            return new AnalyzedClauseEntry(resolved.get(0).getPartitionName(), clause);
        } catch (Throwable t) {
            // Catching Throwable: AnalyzerUtils / AlterTableClauseAnalyzer can throw
            // OutOfRangeException, SemanticException, AnalysisException,
            // IllegalArgumentException, and various other runtime exceptions
            // depending on the partition expression and the input value.
            // Dropping one bad row must not abort the load.
            LOG.debug("Pre-split: analyzer rejected partition values {}: {}", formattedValues, t.getMessage());
            return null;
        }
    }

    /** First-pass per-key bucket of sample rows; analyzer + catalog lookup happen later. */
    private static final class RawGroup {
        private final List<String> formattedValues;
        private final List<SampleRow> rows = new ArrayList<>();

        RawGroup(List<String> formattedValues) {
            this.formattedValues = ImmutableList.copyOf(formattedValues);
        }
    }

    /** Output of {@link #analyzeOnce}: resolved partition name + the analyzed clause. */
    private static final class AnalyzedClauseEntry {
        private final String partitionName;
        private final AddPartitionClause clause;

        AnalyzedClauseEntry(String partitionName, AddPartitionClause clause) {
            this.partitionName = partitionName;
            this.clause = clause;
        }
    }

    /**
     * One logical target partition after raw groups have been merged by resolved
     * partition name. {@code formattedValues} and {@code clause} come from the
     * first raw group that resolved to {@code partitionName}; later raw groups
     * resolve to the same partition so their clause is equivalent. {@code rows}
     * accumulates every merged raw group's rows.
     */
    private static final class MergedGroup {
        private final String partitionName;
        private final List<String> formattedValues;
        private final AddPartitionClause clause;
        private final List<SampleRow> rows = new ArrayList<>();

        MergedGroup(String partitionName, List<String> formattedValues, AddPartitionClause clause) {
            this.partitionName = partitionName;
            this.formattedValues = ImmutableList.copyOf(formattedValues);
            this.clause = clause;
        }
    }
}
