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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkScopedTableResolver;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TChangesMetaKind;
import com.starrocks.thrift.TOpType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds the logical operators for a {@code [_CHANGES_]} table reference over a bookmark range —
 * the single entry point callers share so they all plan it the same way.
 */
public final class ChangesScanBuilder {

    /**
     * Resolves {@code range}'s base and head ids against the BookmarkManager,
     * computes the delta, rejects non-trackable changes, and returns the scan
     * operator over a bookmark-scoped view of {@code table}. PK rejection and
     * the base &lt;= head invariant are the caller's responsibility.
     *
     * @throws SemanticException if either bookmark id is not registered for
     *     {@code table}, or the delta contains non-trackable changes
     */
    public static LogicalChangesScanOperator buildScanOperator(
            OlapTable table, BookmarkRange range,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            List<ChangesMetaDescriptor> metaDescriptors,
            PartitionRef partitionNameHint,
            List<Long> tabletIdHint,
            DistributionSpec distributionSpec) {
        long dbId = table.mayGetDatabaseId().orElseThrow(() ->
                new IllegalStateException(
                        String.format("dbId missing on %s", table.getName())));
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = bookmarkManager.findBookmarkById(dbId, table.getId(), range.base())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.base(), table.getName())));
        Bookmark head = bookmarkManager.findBookmarkById(dbId, table.getId(), range.head())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.head(), table.getName())));
        BookmarkChange delta = BookmarkChange.computeChanges(Optional.of(base), head);
        OlapTable scopedTable = BookmarkScopedTableResolver.resolveByChange(table, delta);

        validatePartitionHint(scopedTable, partitionNameHint,
                base.getBookmarkId(), head.getBookmarkId(), table.getName());
        validateTabletHint(scopedTable, partitionNameHint, tabletIdHint,
                base.getBookmarkId(), head.getBookmarkId(), table.getName());

        LogicalChangesScanOperator op = new LogicalChangesScanOperator(
                scopedTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT, metaDescriptors);
        PartitionNames partitionNames = partitionNameHint == null ? null
                : new PartitionNames(partitionNameHint.isTemp(),
                        partitionNameHint.getPartitionNames(), partitionNameHint.getPos());
        return new LogicalChangesScanOperator.Builder()
                .withOperator(op)
                .setPartitionNameHints(partitionNames)
                .setTabletIdHints(tabletIdHint)
                .setDistributionSpec(distributionSpec)
                .build();
    }

    private static void validatePartitionHint(OlapTable scopedTable, PartitionRef partitionNameHint,
                                              long baseId, long headId, String tableName) {
        if (partitionNameHint == null) {
            return;
        }
        // PARTITION(col='value') resolves to no partition name (the value lives in
        // partitionColValues) and no SELECT path resolves it, so it would silently
        // prune to an empty scan. Reject it; only the partition-name form is honored.
        if (partitionNameHint.isKeyPartitionNames()) {
            throw new SemanticException(String.format(
                    "CHANGES on table '%s' does not support a PARTITION hint by column value; "
                            + "specify partition names", tableName));
        }
        for (String name : partitionNameHint.getPartitionNames()) {
            if (scopedTable.getPartition(name, partitionNameHint.isTemp()) == null) {
                throw new SemanticException(String.format(
                        "CHANGES from bookmark %d to %d on table '%s' not trackable: partition '%s' not present",
                        baseId, headId, tableName, name));
            }
        }
    }

    private static void validateTabletHint(OlapTable scopedTable, PartitionRef partitionNameHint,
                                           List<Long> tabletIdHint, long baseId, long headId, String tableName) {
        if (tabletIdHint == null || tabletIdHint.isEmpty()) {
            return;
        }
        Collection<Partition> scope;
        if (partitionNameHint == null) {
            scope = scopedTable.getPartitions();
        } else {
            // Names already validated present by validatePartitionHint.
            scope = partitionNameHint.getPartitionNames().stream()
                    .map(name -> scopedTable.getPartition(name, partitionNameHint.isTemp()))
                    .collect(Collectors.toList());
        }
        Set<Long> unfound = new HashSet<>(tabletIdHint);
        for (Partition partition : scope) {
            for (PhysicalPartition physical : partition.getSubPartitions()) {
                for (Tablet tablet : physical.getLatestBaseIndex().getTablets()) {
                    unfound.remove(tablet.getId());
                    if (unfound.isEmpty()) {
                        return;
                    }
                }
            }
        }
        for (Long tabletId : tabletIdHint) {
            if (unfound.contains(tabletId)) {
                throw new SemanticException(String.format(
                        "CHANGES from bookmark %d to %d on table '%s' not trackable: tablet %d not present",
                        baseId, headId, tableName, tabletId));
            }
        }
    }

    /**
     * Folds the CHANGES read by {@code scanBuilder}'s root {@link LogicalChangesScanOperator} into
     * the whole-range net change, stacking the fold above that root. A primary-key table folds each
     * key's per-transaction changes to one net DELETE/INSERT pair; a Duplicate Key or Aggregate Key
     * table is append-only (its CHANGES are already net changes), so it comes back unchanged.
     *
     * @param scanBuilder a builder whose root is the LogicalChangesScanOperator to fold above
     * @param factory column-ref factory for the two window output columns
     * @param windowPartitionMode the {@code window_partition_mode} value; 2 selects a hash-based analytic
     */
    public static OptExprBuilder applyNetChange(
            OptExprBuilder scanBuilder, ColumnRefFactory factory, int windowPartitionMode) {
        LogicalChangesScanOperator scan = (LogicalChangesScanOperator) scanBuilder.getRoot().getOp();
        List<Operator> layers = buildNetChangeOperators(scan, factory, windowPartitionMode);
        OptExprBuilder result = scanBuilder;
        for (Operator layer : layers) {
            result = result.withNewRoot(layer);
        }
        return result;
    }

    /**
     * Folds the CHANGES read by {@code scan} into the whole-range net change and returns the
     * folded expression. A primary-key table folds each key's per-transaction changes to one net
     * DELETE/INSERT pair; a Duplicate Key or Aggregate Key table is append-only (its CHANGES are
     * already net changes), so the scan comes back unchanged.
     *
     * @param scan the LogicalChangesScanOperator to fold above
     * @param factory column-ref factory for the two window output columns
     * @param windowPartitionMode the {@code window_partition_mode} value; 2 selects a hash-based analytic
     */
    public static OptExpression applyNetChange(
            LogicalChangesScanOperator scan, ColumnRefFactory factory, int windowPartitionMode) {
        List<Operator> layers = buildNetChangeOperators(scan, factory, windowPartitionMode);
        OptExpression result = OptExpression.create(scan);
        for (Operator layer : layers) {
            result = OptExpression.create(layer, result);
        }
        return result;
    }

    /**
     * Builds the operators that fold {@code scan}'s CHANGES into the whole-range net change, for the
     * caller to stack above the scan.
     *
     * <p>The net change is for consumers that only need each key's net effect over the range rather
     * than its full history: intermediate values enter and leave and cancel out, so the changes in
     * between are wasted work for them. (IVM incremental refresh is one such consumer.)
     *
     * <p>A primary-key table is folded to one net DELETE/INSERT pair per key — the key's value
     * before the range and its value at the end. A key that already held 10 at the range's start
     * and was then updated to 15 and 20 folds to DELETE 10 + INSERT 20, dropping the intermediate 15.
     *
     * <p>A Duplicate Key or Aggregate Key table is append-only: its CHANGES are all INSERTs (never
     * a DELETE) and already net changes, so it is not folded (an empty list is returned).
     *
     * <p>For each key, keep the DELETE at its minimum {@code __ROW_VERSION__} and the INSERT at its
     * maximum: the DELETE from the earliest transaction that changed the key carries its value as
     * of the range start, the INSERT from the latest transaction its value at the end. A key
     * created inside the range has no DELETE at that minimum (it did not exist at the start); one
     * deleted inside has no INSERT at that maximum. The choice is purely by {@code __ROW_VERSION__}
     * boundary — no snapshot read, no value comparison. A Window tags each row with its key's
     * __MIN_ROW_VERSION__/__MAX_ROW_VERSION__ and a Filter keeps the boundary rows, equivalent to:
     * <pre>{@code
     * SELECT <data cols>, __CHANGE_TYPE__, __ROW_VERSION__ FROM (
     *   SELECT *, MIN(__ROW_VERSION__) OVER (PARTITION BY <pk>) AS __MIN_ROW_VERSION__,
     *             MAX(__ROW_VERSION__) OVER (PARTITION BY <pk>) AS __MAX_ROW_VERSION__
     *   FROM <changes>)
     * WHERE (__CHANGE_TYPE__ = 1 AND __ROW_VERSION__ = __MIN_ROW_VERSION__)   -- keep the DELETE
     *    OR (__CHANGE_TYPE__ = 0 AND __ROW_VERSION__ = __MAX_ROW_VERSION__)   -- keep the INSERT
     * }</pre>
     *
     * @param scan the LogicalChangesScanOperator whose CHANGES to fold
     * @param factory column-ref factory for the two window output columns
     * @param windowPartitionMode the {@code window_partition_mode} value; 2 selects a hash-based analytic
     * @return the Window and Filter to stack above the scan (bottom-up), or an empty list for a
     *     non-primary-key table
     */
    private static List<Operator> buildNetChangeOperators(
            LogicalChangesScanOperator scan, ColumnRefFactory factory, int windowPartitionMode) {
        OlapTable table = (OlapTable) scan.getTable();
        if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
            return List.of();
        }

        // Skip the fold when every changed partition spans a single version
        // (headVersion - baseVersion <= 1): each key then appears at exactly one
        // __ROW_VERSION__, so MIN == MAX and the Filter keeps every row -- the
        // Window+Filter would be an identity transform at full analytic cost. A
        // non-trackable change (empty range) is not provably single-version, so it
        // keeps the fold; buildScanOperator already rejects such changes upstream.
        boolean allSingleVersion = scan.getDelta().getChanges().values().stream()
                .flatMap(List::stream)
                .allMatch(c -> c.versionRange().map(p -> p.second - p.first <= 1).orElse(false));
        if (allSingleVersion) {
            return List.of();
        }

        // Resolve the metadata column refs by descriptor kind (names may have
        // been disambiguated against the base schema, so match by name from descriptor).
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = scan.getColRefToColumnMetaMap();
        List<ChangesMetaDescriptor> metaDescriptors = scan.getChangesMetaDescriptors();
        String changeTypeName = nameOf(metaDescriptors, TChangesMetaKind.CHANGE_TYPE);
        String rowVersionName = nameOf(metaDescriptors, TChangesMetaKind.ROW_VERSION);
        ColumnRefOperator changeTypeRef = null;
        ColumnRefOperator rowVersionRef = null;
        List<ColumnRefOperator> pkRefs = new ArrayList<>();
        Set<String> pkRefNames = new HashSet<>();
        for (Map.Entry<ColumnRefOperator, Column> e : colRefToColumnMetaMap.entrySet()) {
            Column col = e.getValue();
            if (col.getName().equalsIgnoreCase(changeTypeName)) {
                changeTypeRef = e.getKey();
            } else if (col.getName().equalsIgnoreCase(rowVersionName)) {
                rowVersionRef = e.getKey();
            } else if (col.isKey()) {
                pkRefs.add(e.getKey());
                pkRefNames.add(col.getName().toLowerCase(Locale.ROOT));
            }
        }
        if (changeTypeRef == null || rowVersionRef == null || pkRefs.isEmpty()) {
            throw new IllegalStateException(
                    "net change needs change-type, row-version and key columns on " + table.getName());
        }
        // PARTITION BY must carry the table's full primary key: folding by a subset of the key
        // columns would merge distinct keys into one partition and net their changes together.
        // Verify every key column reached the scan (column pruning, or a metadata column whose
        // name collides with a key column, could otherwise drop one).
        List<String> missingKeys = new ArrayList<>();
        for (Column keyCol : table.getKeyColumns()) {
            if (!pkRefNames.contains(keyCol.getName().toLowerCase(Locale.ROOT))) {
                missingKeys.add(keyCol.getName());
            }
        }
        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException(
                    "net change needs every primary-key column of " + table.getName()
                            + " in the scan; missing " + missingKeys);
        }

        // Window: __MIN_ROW_VERSION__ = MIN(rv) OVER (PARTITION BY pk), __MAX_ROW_VERSION__ = MAX(rv) OVER (...).
        // No ORDER BY and no explicit frame: whole-partition semantics, which is what we want.
        Type rvType = rowVersionRef.getType();
        ColumnRefOperator vMin = factory.create("__MIN_ROW_VERSION__", rvType, true);
        ColumnRefOperator vMax = factory.create("__MAX_ROW_VERSION__", rvType, true);
        Map<ColumnRefOperator, CallOperator> windowCall = new LinkedHashMap<>();
        windowCall.put(vMin, analyticAgg(FunctionSet.MIN, rowVersionRef, rvType));
        windowCall.put(vMax, analyticAgg(FunctionSet.MAX, rowVersionRef, rvType));
        List<ScalarOperator> partitionBy = new ArrayList<>(pkRefs);
        // Honor window_partition_mode the same no-hint way WindowTransformer does, so net change plans
        // like an equivalent MIN/MAX(...) OVER (PARTITION BY pk) query. A sort-based analytic (default)
        // finds each partition by comparing adjacent rows, so it needs a sort by primary key enforced
        // below it -- without it keys arrive interleaved, split across partitions, and MIN/MAX comes out
        // wrong; a hash-based analytic groups by a hash table and needs no sort. WindowTransformer also
        // guards this on isEnablePipelineEngine(); omitted here because CHANGES planning is always
        // pipeline -- a non-pipeline caller would otherwise get a hash window with no sort below it.
        boolean useHashBasedPartition = windowPartitionMode == 2;
        List<Ordering> enforceSort = new ArrayList<>();
        if (!useHashBasedPartition) {
            for (ColumnRefOperator pk : pkRefs) {
                enforceSort.add(new Ordering(pk, true, true));
            }
        }
        LogicalWindowOperator window = new LogicalWindowOperator.Builder()
                .setWindowCall(windowCall)
                .setPartitionExpressions(partitionBy)
                .setEnforceSortColumns(enforceSort)
                .setUseHashBasedPartition(useHashBasedPartition)
                .build();

        // Filter: (ct = 1 DELETE AND rv = __MIN_ROW_VERSION__) OR (ct = 0 INSERT AND rv = __MAX_ROW_VERSION__).
        ConstantOperator delete = ConstantOperator.createTinyInt((byte) TOpType.DELETE.getValue());
        ConstantOperator insert = ConstantOperator.createTinyInt((byte) TOpType.UPSERT.getValue());
        ScalarOperator keepDelete = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, changeTypeRef, delete),
                new BinaryPredicateOperator(BinaryType.EQ, rowVersionRef, vMin));
        ScalarOperator keepInsert = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, changeTypeRef, insert),
                new BinaryPredicateOperator(BinaryType.EQ, rowVersionRef, vMax));
        LogicalFilterOperator filter =
                new LogicalFilterOperator(Utils.compoundOr(keepDelete, keepInsert));

        // Returned bottom-up: window first, then filter. The filter tests __ROW_VERSION__ against the
        // per-key __MIN_ROW_VERSION__/__MAX_ROW_VERSION__ the window produces, so it references window
        // output columns and must be stacked above the window (it cannot sink below into the scan).
        return List.of(window, filter);
    }

    /**
     * Resolves the builtin aggregate function for use in an analytic (window) context,
     * using the same {@code ExprUtils.getBuiltinFunction} path that WindowTransformer uses.
     */
    private static CallOperator analyticAgg(String fnName, ColumnRefOperator arg, Type returnType) {
        Function fn = ExprUtils.getBuiltinFunction(
                fnName, new Type[] {arg.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(fnName, returnType, ImmutableList.of(arg), fn);
    }

    private static String nameOf(List<ChangesMetaDescriptor> descriptors, TChangesMetaKind kind) {
        for (ChangesMetaDescriptor d : descriptors) {
            if (d.kind() == kind) {
                return d.name();
            }
        }
        throw new IllegalStateException("missing CHANGES descriptor: " + kind);
    }
}
