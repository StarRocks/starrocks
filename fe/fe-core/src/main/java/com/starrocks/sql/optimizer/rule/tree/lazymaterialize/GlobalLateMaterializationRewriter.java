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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFetchOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLookUpOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.PrimitiveType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// Rewrites a PhysicalPlan so columns are materialized lazily. The rewriter
// discovers which scan outputs can stay deferred, tracks where they must be
// fetched, and injects the necessary fetch / lookup operators while keeping
// projections and logical properties consistent.
public class GlobalLateMaterializationRewriter {
    private static final Logger LOG = LogManager.getLogger(GlobalLateMaterializationRewriter.class);

    public OptExpression rewrite(OptExpression root, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableGlobalLateMaterialization()) {
            return root;
        }
        // Fast check: GLM only makes sense when a row-limit is present somewhere in the
        // tree to bound the number of rows that need lazy fetching.  Skip the entire
        // pipeline (all four passes) if no operator carries a limit.
        if (!hasLimit(root)) {
            return root;
        }
        // stage A split projection
        root = root.getOp().accept(new SplitProjectionRewriter(), root, null);

        CollectorContext collectorContext = new CollectorContext(context.getColumnRefFactory());

        ColumnCollector columnCollector = new ColumnCollector(context);
        root.getOp().accept(columnCollector, root, collectorContext);

        mergeFetchPosition(root, collectorContext, context);
        boolean enableCostBased = context.getSessionVariable().isEnableGlobalLateMaterializationCostBased();
        if (enableCostBased && !hasTopNWithLimit(root) && !hasLimitAfterJoin(root)) {
            return root;
        }
        collectorContext.costBasedGlm = enableCostBased;
        root = rewrite(root, collectorContext);

        root = root.getOp().accept(new MergeProjectIntoPhysicalOperator(), root, null);

        return root;
    }

    // Walk the tree looking for any operator that carries a row limit.
    // Stops as soon as one is found (does not visit the full tree unnecessarily).
    private static boolean hasLimit(OptExpression root) {
        if (root.getOp().hasLimit()) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (hasLimit(child)) {
                return true;
            }
        }
        return false;
    }

    // Returns true when the plan contains a PhysicalTopN operator with a row limit (ORDER BY ... LIMIT).
    private static boolean hasTopNWithLimit(OptExpression root) {
        if (root.getOp() instanceof PhysicalTopNOperator && root.getOp().hasLimit()) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (hasTopNWithLimit(child)) {
                return true;
            }
        }
        return false;
    }

    // Returns true when any limit-carrying node has a join operator in its subtree
    // (i.e., a LIMIT appears after / above a JOIN).
    private static boolean hasLimitAfterJoin(OptExpression root) {
        if (root.getOp().hasLimit() && subtreeHasJoin(root)) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (hasLimitAfterJoin(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean subtreeHasJoin(OptExpression root) {
        if (root.getOp() instanceof PhysicalJoinOperator) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (subtreeHasJoin(child)) {
                return true;
            }
        }
        return false;
    }

    private static class SplitProjectionRewriter extends OptExpressionVisitor<OptExpression, Void> {
        // for operator with projection, we split it into operator -> project
        private OptExpression splitProjection(OptExpression optExpression) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            Projection projection = op.getProjection();
            if (projection != null) {
                // remove projection from the original operator and create a new one
                PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(
                        projection.getColumnRefMap(), projection.getCommonSubOperatorMap());
                RowOutputInfo projectedRowOutputInfo = optExpression.getRowOutputInfo();

                op.setProjection(null);
                op.clearRowOutputInfo();

                RowOutputInfo childRowOutputInfo = optExpression.getRowOutputInfo();
                LogicalProperty childLogicalProperty = new LogicalProperty(optExpression.getLogicalProperty());

                childLogicalProperty.setOutputColumns(childRowOutputInfo.getOutputColumnRefSet());

                optExpression.setLogicalProperty(childLogicalProperty);

                OptExpression result = OptExpression.create(projectOperator, optExpression);
                LogicalProperty projectLogicalProperty = new LogicalProperty(childLogicalProperty);
                projectLogicalProperty.setOutputColumns(projectedRowOutputInfo.getOutputColumnRefSet());
                result.setLogicalProperty(projectLogicalProperty);
                result.setStatistics(optExpression.getStatistics());
                return result;
            }

            return optExpression;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();

            if (op.getProjection() != null) {
                // process projection
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            }

            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                inputs.add(input.getOp().accept(this, input, context));
            }

            return OptExpression.builder().with(optExpression).setInputs(inputs).build();
        }
    }

    // Inverse of SplitProjectionRewriter: merges a PhysicalProjectOperator back into its
    // child operator's projection field, eliminating the intermediate PROJECT node.
    //
    // Conditions for merging PROJECT(child) → child[projection]:
    //   1. The child does not already have an embedded projection (should always hold after
    //      SplitProjectionRewriter, except for PhysicalFetchOperator which sets its own).
    //   2. The child is not a PhysicalFetchOperator — FETCH's projection encodes the
    //      row-locator → column mapping that the FETCH executor depends on; pushing an
    //      additional PROJECT into FETCH would silently corrupt that mapping.
    //   3. The child is not another PhysicalProjectOperator (would require projection
    //      composition instead of simple assignment).
    private static class MergeProjectIntoPhysicalOperator extends OptExpressionVisitor<OptExpression, Void> {

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                inputs.add(input.getOp().accept(this, input, context));
            }
            return OptExpression.builder().with(optExpression).setInputs(inputs).build();
        }

        @Override
        public OptExpression visitPhysicalProject(OptExpression optExpression, Void context) {
            // Process children first so they are already merged before we try to absorb them.
            Preconditions.checkState(optExpression.getInputs().size() == 1);
            OptExpression childExpr = optExpression.inputAt(0).getOp()
                    .accept(this, optExpression.inputAt(0), context);

            PhysicalOperator childOp = (PhysicalOperator) childExpr.getOp();
            PhysicalProjectOperator projectOp = (PhysicalProjectOperator) optExpression.getOp();

            if (childOp.getProjection() == null
                    && !(childOp instanceof PhysicalFetchOperator)
                    && !(childOp instanceof PhysicalProjectOperator)) {
                // Push projection into child and replace this PROJECT node with the child.
                childOp.setProjection(new Projection(
                        projectOp.getColumnRefMap(), projectOp.getCommonSubOperatorMap()));
                // Carry the project's logical property forward so callers see the correct
                // output column set (the projected columns, not the raw child columns).
                childExpr.setLogicalProperty(optExpression.getLogicalProperty());
                return childExpr;
            }

            // Cannot merge — keep the PROJECT node with the (already processed) child.
            return OptExpression.builder().with(optExpression)
                    .setInputs(List.of(childExpr)).build();
        }
    }

    // IdentifyOperator is used to uniquely identify an Operator.
    // It is introduced because during the rewriting process, we may change the member variables of the Operator,
    // which will cause the original hash method result to change.
    private static class IdentifyOperator {
        PhysicalOperator physicalOperator;
        int hashCode;

        public IdentifyOperator(PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
            this.hashCode = System.identityHashCode(physicalOperator);
        }

        public PhysicalOperator get() {
            return physicalOperator;
        }

        @Override
        public String toString() {
            return hashCode + ":" + physicalOperator;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return physicalOperator == ((IdentifyOperator) o).physicalOperator;
        }
    }

    public static class CollectorContext {
        CollectorContext(ColumnRefFactory refFactory) {
            this.columnSources = Maps.newHashMap();
            this.alias = Maps.newHashMap();
            this.dependency = Maps.newHashMap();
            this.needLookupSources = Sets.newHashSet();
            this.cteCtxMap = Maps.newHashMap();
            this.cteProduceMap = Maps.newHashMap();
            this.columnRefFactory = refFactory;
            this.fetchPositions = HashBasedTable.create();
        }

        // un-materialized columns
        Map<IdentifyOperator, ColumnRefSet> unMaterializedColumns = new HashMap<>();

        // which ScanOperator the column comes from
        Map<ColumnRefOperator, IdentifyOperator> columnSources;
        // column alias
        // eg: project 1 <- 2 then 1 is alias for 2
        Map<ColumnRefOperator, ColumnRefOperator> alias;

        // operator scan dependency
        Map<IdentifyOperator, Set<IdentifyOperator>> dependency;

        // used to record the fetch position. for example,
        // a row in the table (A, B, [C,D]) indicates that
        // the Column C and D from PhysicalOlapScan B should be fetched before PhysicalOperator A
        Table<IdentifyOperator, IdentifyOperator, ColumnRefSet> fetchPositions;

        // used to record all scan operators that have late-materialized columns
        Set<IdentifyOperator> needLookupSources;

        // cte id -> cte producer
        // shared
        Map<Integer, CollectorContext> cteCtxMap;
        Map<Integer, OptExpression> cteProduceMap;

        ColumnRefFactory columnRefFactory;

        boolean costBasedGlm = false;

        ColumnRefOperator getOriginColumnRef(ColumnRefOperator col) {
            while (alias.containsKey(col)) {
                ColumnRefOperator target = alias.getOrDefault(col, col);
                if (target.equals(col)) {
                    return col;
                }
                col = target;
            }
            return col;
        }

        CollectorContext createChild() {
            final CollectorContext collectorContext = new CollectorContext(columnRefFactory);
            collectorContext.columnSources = this.columnSources;
            collectorContext.alias = this.alias;
            collectorContext.dependency = this.dependency;
            collectorContext.cteCtxMap = this.cteCtxMap;
            collectorContext.cteProduceMap = this.cteProduceMap;
            collectorContext.needLookupSources = this.needLookupSources;
            collectorContext.fetchPositions = this.fetchPositions;
            collectorContext.costBasedGlm = this.costBasedGlm;
            return collectorContext;
        }

        void mergeChildContext(CollectorContext other) {
            other.unMaterializedColumns.forEach((op, colSet) ->
                    unMaterializedColumns.merge(op, colSet, (existing, incoming) -> {
                        existing.union(incoming);
                        return existing;
                    })
            );
        }
    }

    private static class AliasResolver {
        AliasResolver() {
        }

        AliasResolver(Map<ColumnRefOperator, ColumnRefOperator> alias) {
            alias.forEach(this::addProjection);
        }

        // col -> base col
        private final Map<ColumnRefOperator, ColumnRefOperator> resolved = Maps.newHashMap();

        void addProjection(Map<ColumnRefOperator, ScalarOperator> projection) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.entrySet()) {
                ColumnRefOperator key = entry.getKey();
                ScalarOperator value = entry.getValue();
                addProjection(key, value);
            }
        }

        void addProjection(ColumnRefOperator to, ScalarOperator from) {
            if (from.equals(to)) {
                return;
            }
            if (from instanceof ColumnRefOperator fromColumnRef) {
                ColumnRefOperator baseCol = resolved.getOrDefault(fromColumnRef, fromColumnRef);
                resolved.put(to, baseCol);
            } else {
                resolved.put(to, to);
            }
        }

        ColumnRefOperator resolve(ColumnRefOperator col) {
            return resolved.getOrDefault(col, col);
        }
    }

    // Traverse the PlanTree from bottom to top,
    // find all the columns that can be lazy read and where they need to be materialized
    // Bottom-up traversal that identifies lazy columns and records where they
    // first need to be materialized for correctness.
    public static class ColumnCollector extends OptExpressionVisitor<Void, CollectorContext> {
        private final OptimizerContext optimizerContext;

        public ColumnCollector(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        private void recordMaterializedBefore(ColumnRefSet columns, PhysicalOperator physicalOperator,
                                              CollectorContext context) {
            ColumnRefSet unMaterializedColRefSet = new ColumnRefSet();
            for (ColumnRefSet value : context.unMaterializedColumns.values()) {
                unMaterializedColRefSet.union(value);
            }
            columns.intersect(unMaterializedColRefSet);

            if (columns.isEmpty()) {
                return;
            }

            final List<ColumnRefOperator> columnRefOperators =
                    columns.getColumnRefOperators(optimizerContext.getColumnRefFactory());

            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                final ColumnRefOperator origin = context.getOriginColumnRef(columnRefOperator);
                if (!context.columnSources.containsKey(origin)) {
                    continue;
                }
                IdentifyOperator sourceOperator = context.columnSources.get(origin);
                IdentifyOperator operator = new IdentifyOperator(physicalOperator);

                if (!context.fetchPositions.contains(operator, sourceOperator)) {
                    context.fetchPositions.put(operator, sourceOperator, new ColumnRefSet());
                }

                final ColumnRefSet columnRefSet = context.fetchPositions.get(operator, sourceOperator);
                Preconditions.checkState(columnRefSet != null);
                columnRefSet.union(columnRefOperator.getId());

                if (context.unMaterializedColumns.containsKey(sourceOperator)) {
                    context.unMaterializedColumns.get(sourceOperator).except(ColumnRefSet.of(columnRefOperator));
                    if (context.unMaterializedColumns.get(sourceOperator).isEmpty()) {
                        context.unMaterializedColumns.remove(sourceOperator);
                    }
                }

            }
        }

        private void visitChildren(OptExpression optExpression, CollectorContext context) {
            final PhysicalOperator self = (PhysicalOperator) optExpression.getOp();
            final IdentifyOperator id = new IdentifyOperator(self);
            for (OptExpression input : optExpression.getInputs()) {
                final CollectorContext childCtx = context.createChild();

                final PhysicalOperator op = (PhysicalOperator) input.getOp();
                op.accept(this, input, childCtx);

                context.mergeChildContext(childCtx);
                context.dependency.computeIfAbsent(id, k -> Sets.newHashSet());
                final Set<IdentifyOperator> child = context.dependency.getOrDefault(new IdentifyOperator(op), Set.of());
                context.dependency.get(id).addAll(child);
            }
        }

        private void collectEarlyMaterialized(CollectorContext context, AliasResolver resolver,
                                              Map<ColumnRefOperator, ScalarOperator> projection, ColumnRefSet set) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.entrySet()) {
                if (!entry.getValue().isColumnRef()) {
                    final ColumnRefSet usedColumns = entry.getValue().getUsedColumns();
                    for (ColumnRefOperator col : usedColumns.getColumnRefOperators(context.columnRefFactory)) {
                        resolver.resolve(col);
                        set.union(resolver.resolve(col));
                    }
                    set.union(usedColumns);
                }
            }
        }

        Map<ColumnRefOperator, ColumnRefOperator> buildUnMaterializedAlias(CollectorContext context,
                                                                           AliasResolver resolver) {
            Map<ColumnRefOperator, ColumnRefOperator> alias = Maps.newHashMap();
            resolver.resolved.forEach((k, v) -> {
                if (!k.equals(v) && context.columnSources.containsKey(v)) {
                    alias.put(k, v);
                }
            });
            return alias;
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);
            PhysicalProjectOperator op = (PhysicalProjectOperator) optExpression.getOp();

            AliasResolver resolver = new AliasResolver();
            resolver.addProjection(op.getCommonSubOperatorMap());
            resolver.addProjection(op.getColumnRefMap());

            // collectEarlyMaterialized
            final ColumnRefSet earlyMaterializedColumns = new ColumnRefSet();
            collectEarlyMaterialized(context, resolver, op.getCommonSubOperatorMap(), earlyMaterializedColumns);
            collectEarlyMaterialized(context, resolver, op.getColumnRefMap(), earlyMaterializedColumns);

            // collect un-materialized columns
            final Map<ColumnRefOperator, ColumnRefOperator> alias = buildUnMaterializedAlias(context, resolver);
            context.alias.putAll(alias);

            recordMaterializedBefore(earlyMaterializedColumns, op, context);

            // remove un-projected un-materialized columns
            context.unMaterializedColumns.clear();

            for (ColumnRefOperator columnRefOperator : op.getColumnRefMap().keySet()) {
                final ColumnRefOperator origin = context.getOriginColumnRef(columnRefOperator);
                if (context.columnSources.containsKey(origin)) {
                    final IdentifyOperator identifyOperator = context.columnSources.get(origin);
                    context.unMaterializedColumns.computeIfAbsent(identifyOperator, k -> new ColumnRefSet());
                    context.unMaterializedColumns.get(identifyOperator).union(columnRefOperator);
                }
            }

            return null;
        }

        @Override
        public Void visit(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            final ColumnRefSet usedColumns = op.getUsedColumns();
            recordMaterializedBefore(usedColumns, op, context);
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, CollectorContext context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(scanOperator);
            context.dependency.put(identifyOperator, Sets.newHashSet());
            context.dependency.get(identifyOperator).add(identifyOperator);

            if (scanOperator.getOutputColumns().isEmpty()) {
                return null;
            }
            final LazyMaterializationSupport handler = LazyMaterializationRegistry.getHandler(scanOperator);
            if (!handler.supports(scanOperator)) {
                return null;
            }

            // collect possible un-materialized columns
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
            for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
                context.columnSources.put(columnRefOperator, identifyOperator);
                context.unMaterializedColumns.computeIfAbsent(identifyOperator, (k) -> new ColumnRefSet());
                context.unMaterializedColumns.get(identifyOperator).union(columnRefOperator);
            }
            context.needLookupSources.add(identifyOperator);

            final ColumnRefSet predicateUsedColumns = handler.predicateUsedColumns(scanOperator);
            recordMaterializedBefore(predicateUsedColumns, scanOperator, context);

            return null;
        }

        @Override
        public Void visitPhysicalTopN(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);
            PhysicalTopNOperator topNOperator = (PhysicalTopNOperator) optExpression.getOp();
            // we have split projection
            Preconditions.checkState(topNOperator.getProjection() == null);
            List<Ordering> orderings = topNOperator.getOrderSpec().getOrderDescs();

            final ColumnRefSet earlyMaterializedColumns = new ColumnRefSet();

            for (Ordering ordering : orderings) {
                ColumnRefOperator columnRefOperator = ordering.getColumnRef();
                earlyMaterializedColumns.union(columnRefOperator);
            }

            if (topNOperator.getPreAggCall() != null) {
                for (Map.Entry<ColumnRefOperator, CallOperator> entry : topNOperator.getPreAggCall().entrySet()) {
                    earlyMaterializedColumns.union(entry.getValue().getUsedColumns());
                }
            }

            if (topNOperator.getPartitionByColumns() != null) {
                for (ColumnRefOperator partitionByColumn : topNOperator.getPartitionByColumns()) {
                    earlyMaterializedColumns.union(partitionByColumn);
                }
            }

            recordMaterializedBefore(earlyMaterializedColumns, topNOperator, context);

            return null;
        }

        @Override
        public Void visitPhysicalDistribution(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);

            PhysicalDistributionOperator distributionOperator = (PhysicalDistributionOperator) optExpression.getOp();
            DistributionSpec distributionSpec = distributionOperator.getDistributionSpec();
            // handle different spec
            if (Objects.requireNonNull(distributionSpec.getType()) == DistributionSpec.DistributionType.SHUFFLE) {
                HashDistributionSpec hashDistributionSpec = (HashDistributionSpec) distributionSpec;
                List<DistributionCol> shuffleColumns = hashDistributionSpec.getShuffleColumns();
                // shuffle columns should be required
                final ColumnRefSet earlyMaterializedColumn = new ColumnRefSet();
                for (DistributionCol col : shuffleColumns) {
                    earlyMaterializedColumn.union(col.getColId());
                }
                recordMaterializedBefore(earlyMaterializedColumn, distributionOperator, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);

            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
            ColumnRefSet requiredColumns = joinOperator.getJoinConditionUsedColumns();
            recordMaterializedBefore(requiredColumns, joinOperator, context);

            return null;
        }

        @Override
        public Void visitPhysicalNestLoopJoin(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);

            PhysicalNestLoopJoinOperator joinOperator = (PhysicalNestLoopJoinOperator) optExpression.getOp();

            ColumnRefSet requiredColumns = joinOperator.getJoinConditionUsedColumns();
            recordMaterializedBefore(requiredColumns, joinOperator, context);

            return null;
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);

            PhysicalFilterOperator filterOperator = (PhysicalFilterOperator) optExpression.getOp();
            recordMaterializedBefore(filterOperator.getUsedColumns(), filterOperator, context);

            return null;
        }

        @Override
        public Void visitPhysicalCTEAnchor(OptExpression optExpression, CollectorContext context) {
            final IdentifyOperator id = new IdentifyOperator((PhysicalOperator) optExpression.getOp());
            context.dependency.computeIfAbsent(id, k -> Sets.newHashSet());

            final CollectorContext cteProducer = context.createChild();
            final OptExpression producer = optExpression.inputAt(0);
            final PhysicalOperator op = (PhysicalOperator) producer.getOp();
            producer.getOp().accept(this, producer, cteProducer);
            context.dependency.get(id).addAll(context.dependency.get(new IdentifyOperator(op)));

            final CollectorContext cte = context.createChild();
            final OptExpression child = optExpression.inputAt(1);
            child.getOp().accept(this, child, cte);

            context.mergeChildContext(cte);

            return null;
        }

        @Override
        public Void visitPhysicalCTEProduce(OptExpression optExpression, CollectorContext context) {
            visitChildren(optExpression, context);
            PhysicalCTEProduceOperator produceOperator = (PhysicalCTEProduceOperator) optExpression.getOp();
            context.cteCtxMap.put(produceOperator.getCteId(), context);
            context.cteProduceMap.put(produceOperator.getCteId(), optExpression);
            return null;
        }

        @Override
        public Void visitPhysicalCTEConsume(OptExpression optExpression, CollectorContext context) {
            Preconditions.checkState(optExpression.getInputs().isEmpty());
            PhysicalCTEConsumeOperator consumeOperator = (PhysicalCTEConsumeOperator) optExpression.getOp();
            final int cteId = consumeOperator.getCteId();
            Preconditions.checkState(context.cteCtxMap.containsKey(cteId));

            final IdentifyOperator id = new IdentifyOperator(consumeOperator);

            context.dependency.computeIfAbsent(id, k -> Sets.newHashSet());
            final OptExpression produce = context.cteProduceMap.get(cteId);
            final IdentifyOperator produceOp = new IdentifyOperator((PhysicalOperator) produce.getOp());
            context.dependency.get(id).addAll(context.dependency.get(produceOp));

            // collect un-materialized columns
            final Map<ColumnRefOperator, ColumnRefOperator> alias = consumeOperator.getCteOutputColumnRefMap();
            context.alias.putAll(alias);
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : alias.entrySet()) {
                final ColumnRefOperator origin = context.getOriginColumnRef(entry.getKey());
                if (context.columnSources.containsKey(origin)) {
                    final IdentifyOperator identifyOperator = context.columnSources.get(origin);
                    context.unMaterializedColumns.computeIfAbsent(identifyOperator, k -> new ColumnRefSet());
                    context.unMaterializedColumns.get(identifyOperator).union(entry.getKey());
                    context.unMaterializedColumns.get(identifyOperator).except(List.of(entry.getValue()));
                }
            }

            return null;
        }
    }

    private static class FetchMergerContext {
        OptimizerContext optimizerContext;
        CollectorContext collectorContext;
        int numFetchOps = 0;
    }

    private void mergeFetchPosition(OptExpression optExpression, CollectorContext collectorContext,
                                    OptimizerContext optimizerContext) {
        final FetchMergerContext context = new FetchMergerContext();
        context.collectorContext = collectorContext;
        context.optimizerContext = optimizerContext;

        final HashSet<IdentifyOperator> pushedScanFetchId = Sets.newHashSet();
        for (Map.Entry<IdentifyOperator, ColumnRefSet> entries : collectorContext.unMaterializedColumns.entrySet()) {
            final IdentifyOperator scanId = entries.getKey();
            final ColumnRefSet values = entries.getValue();
            if (tryPushDownFetch(optExpression, scanId, values, context)) {
                pushedScanFetchId.add(scanId);
            }
        }
        for (IdentifyOperator pushDownedFetchPo : pushedScanFetchId) {
            collectorContext.unMaterializedColumns.remove(pushDownedFetchPo);
        }

        final Operator op = optExpression.getOp();
        op.accept(new FetchMerger(), optExpression, context);
    }

    private static boolean needPushDown(Operator op, FetchMergerContext context) {
        final SessionVariable sv = context.optimizerContext.getSessionVariable();
        final int maxFetchOps = sv.getGlobalLateMaterializeMaxFetchOps();
        final int maxFetchLimit = sv.getGlobalLateMaterializeMaxLimit();
        if (op instanceof PhysicalScanOperator) {
            return true;
        }
        if (context.numFetchOps < maxFetchOps) {
            return !(op.hasLimit() && op.getLimit() < maxFetchLimit);
        }
        return true;
    }

    private static boolean tryPushDownFetch(OptExpression optExpression, IdentifyOperator scanId,
                                            ColumnRefSet columns, FetchMergerContext context) {
        final PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
        if (!needPushDown(op, context)) {
            context.numFetchOps++;
            return false;
        }

        IdentifyOperator id = new IdentifyOperator(op);

        final ColumnRefFactory columnRefFactory = context.collectorContext.columnRefFactory;

        if (op instanceof PhysicalProjectOperator projection) {
            final Map<ColumnRefOperator, ScalarOperator> project = projection.getColumnRefMap();
            final Map<ColumnRefOperator, ScalarOperator> common = projection.getCommonSubOperatorMap();
            ColumnRefSet beforeProjection = new ColumnRefSet();
            for (ColumnRefOperator c : columns.getColumnRefOperators(columnRefFactory)) {
                final ScalarOperator scalarOperator = project.get(c);
                if (scalarOperator == null) {
                    continue;
                }
                Preconditions.checkState(scalarOperator.isColumnRef());
                ColumnRefOperator origin = (ColumnRefOperator) scalarOperator;
                if (common.containsKey(origin)) {
                    origin = (ColumnRefOperator) common.get(origin);
                }
                beforeProjection.union(origin);
            }
            columns = beforeProjection;
        }

        if (op instanceof PhysicalCTEConsumeOperator consumer) {
            ColumnRefSet beforeProjection = new ColumnRefSet();
            final Map<ColumnRefOperator, ColumnRefOperator> project = consumer.getCteOutputColumnRefMap();
            for (ColumnRefOperator c : columns.getColumnRefOperators(columnRefFactory)) {
                final ColumnRefOperator origin = project.get(c);
                if (origin != null) {
                    beforeProjection.union(origin);
                }
            }
            columns = beforeProjection;
        }

        final Table<IdentifyOperator, IdentifyOperator, ColumnRefSet> fetchPositions =
                context.collectorContext.fetchPositions;

        if (!fetchPositions.contains(id, scanId)) {
            fetchPositions.put(id, scanId, new ColumnRefSet());
        }
        final ColumnRefSet columnRefSet = fetchPositions.get(id, scanId);
        Preconditions.checkState(columnRefSet != null);
        columnRefSet.union(columns);

        return true;
    }

    private static class FetchMerger extends OptExpressionVisitor<Void, FetchMergerContext> {

        private Void processChild(OptExpression opt, FetchMergerContext context) {
            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        @Override
        public Void visit(OptExpression optExpression, FetchMergerContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            final IdentifyOperator id = new IdentifyOperator(op);

            final Map<IdentifyOperator, ColumnRefSet> row =
                    context.collectorContext.fetchPositions.row(id);
            if (row.isEmpty()) {
                return processChild(optExpression, context);
            }

            final HashSet<IdentifyOperator> pushedScanFetch = Sets.newHashSet();
            for (Map.Entry<IdentifyOperator, ColumnRefSet> entries : row.entrySet()) {
                final IdentifyOperator scanId = entries.getKey();
                final ColumnRefSet value = entries.getValue();
                int begin = 0;
                if (op instanceof PhysicalCTEAnchorOperator) {
                    begin = 1;
                }
                for (int i = begin; i < optExpression.getInputs().size(); i++) {
                    OptExpression input = optExpression.inputAt(i);
                    final IdentifyOperator cIdx = new IdentifyOperator((PhysicalOperator) input.getOp());
                    final Set<IdentifyOperator> dependency = context.collectorContext.dependency.get(cIdx);
                    if (dependency == null || !dependency.contains(scanId)) {
                        continue;
                    }
                    if (tryPushDownFetch(input, scanId, value, context)) {
                        pushedScanFetch.add(scanId);
                    }
                }

            }
            for (IdentifyOperator pushDownedFetchPo : pushedScanFetch) {
                context.collectorContext.fetchPositions.remove(id, pushDownedFetchPo);
            }

            return processChild(optExpression, context);
        }

        @Override
        public Void visitPhysicalCTEProduce(OptExpression optExpression, FetchMergerContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            final IdentifyOperator id = new IdentifyOperator(op);

            final Map<IdentifyOperator, ColumnRefSet> row =
                    context.collectorContext.fetchPositions.row(id);
            if (row.isEmpty()) {
                return processChild(optExpression, context);
            }

            OptExpression input = optExpression.inputAt(0);

            final HashSet<IdentifyOperator> pushedScanFetch = Sets.newHashSet();
            for (Map.Entry<IdentifyOperator, ColumnRefSet> entries : row.entrySet()) {
                final IdentifyOperator scanId = entries.getKey();
                final ColumnRefSet value = entries.getValue();
                if (tryPushDownFetch(input, scanId, value, context)) {
                    pushedScanFetch.add(scanId);
                }
            }

            for (IdentifyOperator pushDownedFetchPo : pushedScanFetch) {
                context.collectorContext.fetchPositions.remove(id, pushDownedFetchPo);
            }

            return processChild(optExpression, context);
        }

        @Override
        public Void visitPhysicalCTEConsume(OptExpression optExpression, FetchMergerContext context) {
            Preconditions.checkState(optExpression.getInputs().isEmpty());
            final Map<Integer, OptExpression> produceMap = context.collectorContext.cteProduceMap;
            PhysicalCTEConsumeOperator consumeOperator = (PhysicalCTEConsumeOperator) optExpression.getOp();
            final IdentifyOperator id = new IdentifyOperator(consumeOperator);
            final int cteId = consumeOperator.getCteId();
            Preconditions.checkState(produceMap.containsKey(cteId));

            final Map<IdentifyOperator, ColumnRefSet> row =
                    context.collectorContext.fetchPositions.row(id);
            if (row.isEmpty()) {
                return null;
            }

            final HashSet<IdentifyOperator> pushedScanFetch = Sets.newHashSet();
            for (Map.Entry<IdentifyOperator, ColumnRefSet> entries : row.entrySet()) {
                final IdentifyOperator scanId = entries.getKey();
                final ColumnRefSet value = entries.getValue();

                OptExpression input = produceMap.get(cteId);

                if (tryPushDownFetch(input, scanId, value, context)) {
                    pushedScanFetch.add(scanId);
                }

            }

            for (IdentifyOperator pushDownedFetchPo : pushedScanFetch) {
                context.collectorContext.fetchPositions.remove(id, pushDownedFetchPo);
            }

            if (!pushedScanFetch.isEmpty()) {
                OptExpression input = produceMap.get(cteId);
                input.getOp().accept(this, input, context);
            }

            return null;
        }

    }

    public record RowLocator(List<ColumnRefOperator> columns) {
        public ColumnRefOperator getRowSourceId() {
            return columns.get(0);
        }

        public List<ColumnRefOperator> getRemains() {
            return columns.subList(1, columns.size());
        }
    }

    public record UnMaterializedColumns(ColumnRefSet columns, IdentifyOperator scanId, IdentifyOperator rewrited) {

    }

    private static class RewriteContext {
        RewriteContext(OptExpression parent, AliasResolver resolver) {
            this.parent = parent;
            this.resolver = resolver;
            this.cteCtxMap = Maps.newHashMap();
        }

        RewriteContext createChild(OptExpression parent) {
            final RewriteContext rewriteContext = new RewriteContext(parent, resolver);
            rewriteContext.cteCtxMap = cteCtxMap;
            return rewriteContext;
        }

        OptExpression parent = null;

        ColumnRefSet materializedColumns = new ColumnRefSet();
        // RowId Columns
        Map<RowLocator, UnMaterializedColumns> rowIds = Maps.newHashMap();

        final AliasResolver resolver;

        Map<Integer, RewriteContext> cteCtxMap;

        void merge(RewriteContext other) {
            materializedColumns.union(other.materializedColumns);
            rowIds.putAll(other.rowIds);
        }

        Map<RowLocator, UnMaterializedColumns> getRowLocators(ColumnRefSet needMaterialized) {
            Map<RowLocator, UnMaterializedColumns> result = Maps.newHashMap();
            for (Map.Entry<RowLocator, UnMaterializedColumns> entry : this.rowIds.entrySet()) {
                final RowLocator rowLocator = entry.getKey();
                final UnMaterializedColumns u = entry.getValue();
                final IdentifyOperator id = u.scanId();
                final IdentifyOperator rewrited = u.rewrited();

                if (u.columns.isIntersect(needMaterialized)) {
                    final ColumnRefSet set = u.columns.clone();
                    set.intersect(needMaterialized);
                    result.computeIfAbsent(rowLocator,
                            k -> new UnMaterializedColumns(new ColumnRefSet(), id, rewrited));
                    result.get(rowLocator).columns.union(set);
                }
            }

            return result;
        }

        RowLocator recordMaterializedColumns(RowLocator rowLocator, ColumnRefSet materializedColumns) {
            final ColumnRefSet unMaterializedColumns = rowIds.get(rowLocator).columns;
            unMaterializedColumns.except(materializedColumns);
            if (unMaterializedColumns.isEmpty()) {
                rowIds.remove(rowLocator);
                return rowLocator;
            }
            return null;
        }

    }

    private OptExpression rewrite(OptExpression opt, CollectorContext context) {
        final RewriteContext rewriteContext = new RewriteContext(null, new AliasResolver(context.alias));
        opt = opt.getOp().accept(new Rewriter(context), opt, rewriteContext);
        opt.clearAndInitOutputInfo();
        return opt;
    }

    private static void rewriteProperties(OptExpression optExpression, RewriteContext context,
                                          ColumnRefFactory columnRefFactory) {
        LogicalProperty logicalProperty = optExpression.getLogicalProperty();
        List<ColumnRefOperator> outputColumns
                = logicalProperty.getOutputColumns().getColumnRefOperators(columnRefFactory);
        Set<RowLocator> usedRowLocators = Sets.newHashSet();

        outputColumns.removeIf(col -> {
            for (Map.Entry<RowLocator, UnMaterializedColumns> entry : context.rowIds.entrySet()) {
                final UnMaterializedColumns u = entry.getValue();
                final RowLocator rowLocator = entry.getKey();
                if (u.columns.contains(col.getId())) {
                    usedRowLocators.add(rowLocator);
                    return true;
                }
            }
            return false;
        });

        outputColumns.forEach(col -> {
            for (RowLocator rowLocator : context.rowIds.keySet()) {
                if (rowLocator.getRowSourceId().equals(col)) {
                    usedRowLocators.add(rowLocator);
                }
            }
        });

        for (RowLocator usedRowLocator : usedRowLocators) {
            outputColumns.addAll(usedRowLocator.columns);
        }

        final HashMap<RowLocator, UnMaterializedColumns> rowIds = Maps.newHashMap(context.rowIds);
        rowIds.entrySet().removeIf(entry -> !usedRowLocators.contains(entry.getKey()));
        context.rowIds = rowIds;

        logicalProperty.setOutputColumns(new ColumnRefSet(outputColumns));
    }

    private static class Rewriter extends OptExpressionVisitor<OptExpression, RewriteContext> {
        private final CollectorContext collectorContext;

        private Rewriter(CollectorContext context) {
            collectorContext = context;
        }

        private List<OptExpression> visitChildren(OptExpression optExpression, RewriteContext context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                RewriteContext ctx = context.createChild(optExpression);
                inputs.add(input.getOp().accept(this, input, ctx));
                context.merge(ctx);
            }
            return inputs;
        }

        private OptExpression introduceFetch(ColumnRefSet requiredColumns, OptExpression current, RewriteContext context) {
            final OptExpression parent = context.parent;
            ColumnRefSet needMaterialized = new ColumnRefSet();
            if (parent == null) {
                for (UnMaterializedColumns value : context.rowIds.values()) {
                    needMaterialized.union(value.columns);
                }
            } else {
                final PhysicalOperator op = (PhysicalOperator) parent.getOp();
                final IdentifyOperator id = new IdentifyOperator(op);
                if (collectorContext.fetchPositions.containsRow(id)) {
                    for (ColumnRefSet value : collectorContext.fetchPositions.row(id).values()) {
                        needMaterialized.union(value);
                    }
                }
            }
            final ColumnRefFactory columnRefFactory = collectorContext.columnRefFactory;

            final Map<RowLocator, UnMaterializedColumns> rowLocators = context.getRowLocators(needMaterialized);

            if (!rowLocators.isEmpty()) {

                Map<Integer, ColumnDict> globalDictsBuilder = Maps.newHashMap();

                // row id -> rewrited scan operator
                Map<ColumnRefOperator, PhysicalScanOperator> srcIdToScanOperator = new HashMap<>();
                Map<ColumnRefOperator, List<ColumnRefOperator>> srcIdToFetchRefColumns = new HashMap<>();
                Map<ColumnRefOperator, List<ColumnRefOperator>> srcIdToLookUpRefColumns = new HashMap<>();
                // row id -> fetched Columns
                Map<ColumnRefOperator, Set<ColumnRefOperator>> srcIdToLazyColumns = new HashMap<>();
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

                for (Map.Entry<RowLocator, UnMaterializedColumns> entry : rowLocators.entrySet()) {
                    final RowLocator rowLocator = entry.getKey();
                    final UnMaterializedColumns unMaterializedColumns = entry.getValue();
                    final ColumnRefSet materialized = unMaterializedColumns.columns();
                    final PhysicalScanOperator scan = (PhysicalScanOperator) unMaterializedColumns.scanId().get();
                    final PhysicalScanOperator rewrote = (PhysicalScanOperator) unMaterializedColumns.rewrited().get();

                    final ColumnRefOperator rowSourceId = rowLocator.getRowSourceId();
                    final List<ColumnRefOperator> remains = rowLocator.getRemains();
                    srcIdToScanOperator.put(rowSourceId, rewrote);
                    srcIdToFetchRefColumns.put(rowSourceId, remains);

                    // create alias and put it to desc
                    List<ColumnRefOperator> lookupRefColumns = remains.stream().map(c -> {
                        final ColumnRefOperator resolved = context.resolver.resolve(c);
                        Column column = columnRefFactory.getColumn(resolved);
                        Preconditions.checkState(column != null);
                        ColumnRefOperator newColumnRef = columnRefFactory.create(c, c.getType(), c.isNullable());
                        columnRefOperatorColumnMap.put(newColumnRef, column);
                        return newColumnRef;
                    }).toList();

                    srcIdToLookUpRefColumns.put(rowSourceId, lookupRefColumns);

                    final List<ColumnRefOperator> materializedLazyColumns =
                            materialized.getColumnRefOperators(columnRefFactory);
                    srcIdToLazyColumns.put(rowSourceId, new HashSet<>(materializedLazyColumns));

                    final LazyMaterializationSupport handler = LazyMaterializationRegistry.getHandler(scan);
                    Map<ColumnRefOperator, Column> columnRefMap = scan.getColRefToColumnMetaMap();
                    // add all related columns into columnRefOperatorColumnMap
                    for (ColumnRefOperator columnRef : materializedLazyColumns) {
                        final ColumnRefOperator lazyColumn = context.resolver.resolve(columnRef);
                        columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(lazyColumn));
                    }

                    // acquire global dicts
                    for (ColumnRefOperator columnRef : materializedLazyColumns) {
                        final ColumnRefOperator lazyColumn = context.resolver.resolve(columnRef);
                        // acquire global dict
                        final Pair<Integer, ColumnDict> globalDict = handler.getGlobalDict(scan, lazyColumn);
                        if (globalDict != null) {
                            globalDictsBuilder.put(globalDict.first, globalDict.second);
                        }
                    }
                }

                // remove materialized columns and row locators
                ColumnRefSet deletedRowLocatorColumns = new ColumnRefSet();
                ColumnRefSet materialized = new ColumnRefSet();

                rowLocators.forEach((row, unMaterialized) -> {
                    final RowLocator rowLocator = context.recordMaterializedColumns(row, unMaterialized.columns());
                    if (rowLocator != null) {
                        for (ColumnRefOperator column : rowLocator.columns()) {
                            if (!requiredColumns.contains(column)) {
                                deletedRowLocatorColumns.union(column);
                            }
                        }
                    }
                    materialized.union(unMaterialized.columns);
                });

                // create fetch lookup operators
                PhysicalFetchOperator physicalFetchOperator =
                        new PhysicalFetchOperator(srcIdToScanOperator, srcIdToFetchRefColumns, srcIdToLazyColumns);
                PhysicalLookUpOperator physicalLookUpOperator =
                        new PhysicalLookUpOperator(srcIdToScanOperator, srcIdToFetchRefColumns, srcIdToLookUpRefColumns,
                                srcIdToLazyColumns, columnRefOperatorColumnMap);
                final List<Pair<Integer, ColumnDict>> dicts =
                        globalDictsBuilder.entrySet().stream().map(e -> Pair.create(e.getKey(), e.getValue())).toList();
                physicalLookUpOperator.setGlobalDicts(dicts);

                OptExpression lookupOpt = OptExpression.create(physicalLookUpOperator);
                // we just set an empty property, it will be updated at the end
                lookupOpt.setLogicalProperty(new LogicalProperty());

                Map<ColumnRefOperator, ScalarOperator> map = Maps.newHashMap();
                final ColumnRefSet outputColumns = current.getOutputColumns();

                // prune unused row locators

                for (ColumnRefOperator output : outputColumns.getColumnRefOperators(columnRefFactory)) {
                    // if output is row locator must in context. row locators
                    if (!deletedRowLocatorColumns.contains(output)) {
                        map.put(output, output);
                    }
                }

                for (ColumnRefOperator output : materialized.getColumnRefOperators(columnRefFactory)) {
                    map.put(output, output);
                }

                for (RowLocator rowLocator : context.rowIds.keySet()) {
                    for (ColumnRefOperator column : rowLocator.columns()) {
                        map.put(column, column);
                    }
                }

                Projection projection = new Projection(map);
                physicalFetchOperator.setProjection(projection);

                OptExpression fetchOpt = OptExpression.create(physicalFetchOperator, List.of(current, lookupOpt));
                fetchOpt.setLogicalProperty(new LogicalProperty());
                fetchOpt.setStatistics(current.getStatistics());

                current = fetchOpt;
            }
            return current;
        }

        private ColumnRefSet getOutputColumns(OptExpression optExpression) {
            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            return logicalProperty.getOutputColumns();
        }

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {
            List<OptExpression> inputs = visitChildren(optExpression, context);

            ColumnRefSet requiredColumns = getOutputColumns(optExpression);

            optExpression = OptExpression.builder().with(optExpression).setInputs(inputs).build();

            // update output columns
            rewriteProperties(optExpression, context, collectorContext.columnRefFactory);

            optExpression = introduceFetch(requiredColumns, optExpression, context);

            return optExpression;
        }

        public ColumnRefOperator getColumnRefAfterProjection(ColumnRefOperator col,
                                                             Map<ColumnRefOperator, ScalarOperator> common,
                                                             Map<ColumnRefOperator, ? extends ScalarOperator> projection) {
            ColumnRefOperator result = null;
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : common.entrySet()) {
                if (entry.getValue().equals(col)) {
                    col = entry.getKey();
                    break;
                }
            }
            for (Map.Entry<ColumnRefOperator, ? extends ScalarOperator> entry : projection.entrySet()) {
                if (entry.getValue().equals(col)) {
                    result = entry.getKey();
                    break;
                }
            }

            return result;
        }

        @Override
        public OptExpression visitPhysicalProject(OptExpression optExpression, RewriteContext context) {
            final List<OptExpression> inputs = visitChildren(optExpression, context);
            PhysicalProjectOperator op = (PhysicalProjectOperator) optExpression.getOp();

            ColumnRefSet requiredColumns = getOutputColumns(optExpression);

            final ColumnRefFactory columnRefFactory = collectorContext.columnRefFactory;

            final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap = op.getCommonSubOperatorMap();
            final Map<ColumnRefOperator, ScalarOperator> columnRefMap = op.getColumnRefMap();

            final Map<RowLocator, UnMaterializedColumns> rowIds = Maps.newHashMap();

            final ColumnRefSet unMaterialized = new ColumnRefSet();

            for (Map.Entry<RowLocator, UnMaterializedColumns> entry : context.rowIds.entrySet()) {
                final RowLocator rowLocator = entry.getKey();
                final UnMaterializedColumns value = entry.getValue();

                List<ColumnRefOperator> newRowIdColumns = Lists.newArrayList();
                for (ColumnRefOperator column : rowLocator.columns()) {
                    final ColumnRefOperator mapped =
                            columnRefFactory.create(column, column.getType(), column.isNullable());
                    newRowIdColumns.add(mapped);
                    columnRefMap.put(mapped, column);
                    context.resolver.addProjection(mapped, column);
                }

                final ColumnRefSet newUnMaterialized = new ColumnRefSet();
                for (ColumnRefOperator col : value.columns.getColumnRefOperators(columnRefFactory)) {
                    ColumnRefOperator after = getColumnRefAfterProjection(col, commonSubOperatorMap, columnRefMap);
                    context.resolver.addProjection(after, col);
                    // column pruned
                    if (after == null) {
                        continue;
                    }
                    newUnMaterialized.union(after);
                    unMaterialized.union(col);
                }

                final RowLocator projected = new RowLocator(newRowIdColumns);
                rowIds.put(projected, new UnMaterializedColumns(newUnMaterialized, value.scanId(), value.rewrited()));
            }

            Set<ColumnRefOperator> pendingRemovedColumns = Sets.newHashSet();
            commonSubOperatorMap.forEach((k, v) -> {
                if (unMaterialized.contains(k)) {
                    pendingRemovedColumns.add(k);
                } else if (v instanceof ColumnRefOperator col && unMaterialized.contains(col)) {
                    pendingRemovedColumns.add(col);
                }
            });
            columnRefMap.forEach((k, v) -> {
                if (unMaterialized.contains(k)) {
                    pendingRemovedColumns.add(k);
                } else if (v instanceof ColumnRefOperator col && unMaterialized.contains(col)) {
                    pendingRemovedColumns.add(k);
                }
            });

            for (ColumnRefOperator pendingRemovedColumn : pendingRemovedColumns) {
                columnRefMap.remove(pendingRemovedColumn);
                commonSubOperatorMap.remove(pendingRemovedColumn);
            }

            context.rowIds = rowIds;
            optExpression = OptExpression.builder().with(optExpression).setInputs(inputs).build();

            optExpression.setLogicalProperty(new LogicalProperty());
            optExpression.getLogicalProperty().setOutputColumns(new ColumnRefSet(columnRefMap.keySet()));

            optExpression = introduceFetch(requiredColumns, optExpression, context);
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalTableFunction(OptExpression optExpression, RewriteContext context) {
            final List<OptExpression> inputs = visitChildren(optExpression, context);

            ColumnRefSet requiredColumns = getOutputColumns(optExpression);

            // No deferred columns in flight — nothing special to do, use default path.
            if (context.rowIds.isEmpty()) {
                optExpression = OptExpression.builder().with(optExpression).setInputs(inputs).build();
                return introduceFetch(requiredColumns, optExpression, context);
            }

            final PhysicalTableFunctionOperator tfOp = (PhysicalTableFunctionOperator) optExpression.getOp();

            // The child scan may have replaced deferred columns (e.g. PAD) with row-locator
            // columns (row_id) in the data stream. outerColRefs that reference deferred columns
            // are no longer present below this node and must be stripped out; row-locator columns
            // must be threaded through instead so that a FETCH inserted above can use them.
            //
            // Unlike visitPhysicalProject, we do NOT remap RowLocator entries in context.rowIds:
            // table functions pass outer columns through with the same column-ref IDs (no rename),
            // so the existing RowLocator → UnMaterializedColumns mapping stays valid.
            final List<ColumnRefOperator> newOuterColRefs = Lists.newArrayList();
            final Set<RowLocator> usedRowLocators = Sets.newHashSet();

            for (ColumnRefOperator col : tfOp.getOuterColRefs()) {
                boolean deferred = false;
                for (Map.Entry<RowLocator, UnMaterializedColumns> entry : context.rowIds.entrySet()) {
                    if (entry.getValue().columns.contains(col.getId())) {
                        usedRowLocators.add(entry.getKey());
                        deferred = true;
                        break;
                    }
                }
                if (!deferred) {
                    newOuterColRefs.add(col);
                }
            }
            // Inject row-locator columns so they propagate through the table function
            // and remain available for the FETCH node inserted above.
            for (RowLocator rl : usedRowLocators) {
                newOuterColRefs.addAll(rl.columns());
            }

            final PhysicalTableFunctionOperator newTfOp = new PhysicalTableFunctionOperator(
                    tfOp.getFnResultColRefs(), tfOp.getFn(), tfOp.getFnParamColumnRefs(),
                    newOuterColRefs, tfOp.getLimit(), tfOp.getPredicate(), tfOp.getProjection());

            optExpression = OptExpression.builder().with(optExpression).setOp(newTfOp).setInputs(inputs).build();

            rewriteProperties(optExpression, context, collectorContext.columnRefFactory);
            optExpression = introduceFetch(requiredColumns, optExpression, context);
            return optExpression;
        }

        public Set<ColumnRefOperator> getEarlyMaterializedColumns(PhysicalScanOperator scan) {
            IdentifyOperator identifyOperator = new IdentifyOperator(scan);
            final Table<IdentifyOperator, IdentifyOperator, ColumnRefSet> fetchPositions =
                    collectorContext.fetchPositions;

            final ColumnRefSet earlyMaterializedSets = fetchPositions.contains(identifyOperator, identifyOperator) ?
                    fetchPositions.get(identifyOperator, identifyOperator) : new ColumnRefSet();
            Preconditions.checkState(earlyMaterializedSets != null);

            List<ColumnRefOperator> columnRefOperators =
                    earlyMaterializedSets.getColumnRefOperators(collectorContext.columnRefFactory);
            Set<ColumnRefOperator> earlyMaterializedColumns = Sets.newHashSet();

            // process alias
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                final ColumnRefOperator alias = collectorContext.alias.get(columnRefOperator);
                if (alias == null || alias.getId() == columnRefOperator.getId()) {
                    earlyMaterializedColumns.add(columnRefOperator);
                }
            }

            return earlyMaterializedColumns;

        }

        @Override
        public OptExpression visitPhysicalScan(OptExpression optExpression, RewriteContext context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
            IdentifyOperator id = new IdentifyOperator(scanOperator);
            ColumnRefSet requiredColumns = getOutputColumns(optExpression);

            if (!collectorContext.needLookupSources.contains(id)) {
                optExpression = introduceFetch(requiredColumns, optExpression, context);
                return optExpression;
            }

            final Set<ColumnRefOperator> earlyMaterializedColumns = getEarlyMaterializedColumns(scanOperator);
            final Map<ColumnRefOperator, Column> scanColumns = scanOperator.getColRefToColumnMetaMap();

            if (earlyMaterializedColumns.size() == scanOperator.getColRefToColumnMetaMap().size()) {
                // all columns need fetch, no need to rewrite
                context.materializedColumns.union(earlyMaterializedColumns);
                optExpression = introduceFetch(requiredColumns, optExpression, context);
                return optExpression;
            }

            // Cost-based gate: only apply GLM when the byte-cost of the columns that
            // would be deferred exceeds the byte-cost of the row-id locator columns
            // that GLM adds to the scan's output stream.  Skipping GLM when the
            // deferred columns are cheap (e.g. a single INT) avoids paying the
            // 24-byte row-id overhead for no net benefit.
            if (collectorContext.costBasedGlm) {
                // GLM is worthwhile when:
                //   (a) at least one deferred column is a non-numeric / non-date type
                //       (e.g. VARCHAR, CHAR, JSON, ARRAY, HLL, BITMAP …), OR
                //   (b) more than 2 deferred columns are numeric/date — in that case the
                //       combined read savings outweigh the 4-column row-id overhead.
                // In all other cases (≤2 deferred columns, all numeric/date) skip GLM.
                int numericDeferredCount = 0;
                boolean hasNonNumericDeferred = false;
                for (ColumnRefOperator col : scanColumns.keySet()) {
                    if (earlyMaterializedColumns.contains(col)) {
                        continue;
                    }
                    PrimitiveType pt = col.getType().getPrimitiveType();
                    if (!pt.isNumericType() && !pt.isDateType()) {
                        hasNonNumericDeferred = true;
                        break;
                    }
                    numericDeferredCount++;
                }
                if (!hasNonNumericDeferred && numericDeferredCount <= 2) {
                    context.materializedColumns.union(scanColumns.keySet());
                    optExpression = introduceFetch(requiredColumns, optExpression, context);
                    return optExpression;
                }
            }

            final Map<ColumnRefOperator, Column> newOutputs = Maps.newHashMap();

            final ColumnRefFactory columnRefFactory = collectorContext.columnRefFactory;
            final LazyMaterializationSupport handler = LazyMaterializationRegistry.getHandler(scanOperator);
            final List<ColumnRefOperator> rowIdColumns = handler.addRowIdColumns(scanOperator, columnRefFactory);

            // add early materialized columns
            for (ColumnRefOperator earlyMaterializedColumn : earlyMaterializedColumns) {
                newOutputs.put(earlyMaterializedColumn, scanColumns.get(earlyMaterializedColumn));
            }
            // add row id columns
            for (ColumnRefOperator rowIdColumn : rowIdColumns) {
                newOutputs.put(rowIdColumn, columnRefFactory.getColumn(rowIdColumn));
            }

            context.materializedColumns.union(newOutputs.keySet());

            optExpression = handler.updateOutputColumns(optExpression, newOutputs);

            IdentifyOperator newScan = new IdentifyOperator((PhysicalScanOperator) optExpression.getOp());

            final ColumnRefSet columnRefSet = new ColumnRefSet(scanColumns.keySet());
            columnRefSet.except(context.materializedColumns);
            final UnMaterializedColumns unMaterializedColumns = new UnMaterializedColumns(columnRefSet, id, newScan);
            context.rowIds.put(new RowLocator(rowIdColumns), unMaterializedColumns);

            optExpression = introduceFetch(requiredColumns, optExpression, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalCTEProduce(OptExpression optExpression, RewriteContext context) {
            RewriteContext contextChild = context.createChild(optExpression);
            Preconditions.checkState(optExpression.getInputs().size() == 1);
            OptExpression child = optExpression.inputAt(0);
            child = child.getOp().accept(this, child, contextChild);

            optExpression = OptExpression.builder().with(optExpression).setInputs(List.of(child)).build();

            PhysicalCTEProduceOperator produceOperator = (PhysicalCTEProduceOperator) optExpression.getOp();

            rewriteProperties(optExpression, context, collectorContext.columnRefFactory);

            context.cteCtxMap.put(produceOperator.getCteId(), contextChild);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalCTEConsume(OptExpression optExpression, RewriteContext context) {
            Preconditions.checkState(optExpression.getInputs().isEmpty());

            PhysicalCTEConsumeOperator consumeOperator = (PhysicalCTEConsumeOperator) optExpression.getOp();
            ColumnRefSet requiredColumns = getOutputColumns(optExpression);

            final int cteId = consumeOperator.getCteId();
            Preconditions.checkState(context.cteCtxMap.containsKey(cteId));
            final RewriteContext producerCtx = context.cteCtxMap.get(cteId);
            context.merge(producerCtx);

            final Map<ColumnRefOperator, ColumnRefOperator> projection = consumeOperator.getCteOutputColumnRefMap();
            final Map<RowLocator, UnMaterializedColumns> rowIds = Maps.newHashMap();

            final ColumnRefFactory columnRefFactory = collectorContext.columnRefFactory;

            final ColumnRefSet unMaterialized = new ColumnRefSet();

            for (Map.Entry<RowLocator, UnMaterializedColumns> entry : context.rowIds.entrySet()) {
                final RowLocator rowLocator = entry.getKey();
                final UnMaterializedColumns value = entry.getValue();

                List<ColumnRefOperator> newRowIdColumns = Lists.newArrayList();
                for (ColumnRefOperator column : rowLocator.columns()) {
                    final ColumnRefOperator mapped =
                            columnRefFactory.create(column, column.getType(), column.isNullable());
                    newRowIdColumns.add(mapped);
                    projection.put(mapped, column);
                    context.resolver.addProjection(mapped, column);
                }

                final ColumnRefSet newUnMaterialized = new ColumnRefSet();
                for (ColumnRefOperator col : value.columns.getColumnRefOperators(columnRefFactory)) {
                    ColumnRefOperator after = getColumnRefAfterProjection(col, Maps.newHashMap(), projection);
                    if (after == null) {
                        continue;
                    }
                    context.resolver.addProjection(after, col);
                    newUnMaterialized.union(after);
                    unMaterialized.union(col);
                }

                final RowLocator projected = new RowLocator(newRowIdColumns);
                rowIds.put(projected, new UnMaterializedColumns(newUnMaterialized, value.scanId(), value.rewrited()));
            }

            Set<ColumnRefOperator> pendingRemovedColumns = Sets.newHashSet();

            projection.forEach((k, v) -> {
                if (unMaterialized.contains(k)) {
                    pendingRemovedColumns.add(k);
                } else if (unMaterialized.contains(v)) {
                    pendingRemovedColumns.add(k);
                }
            });

            for (ColumnRefOperator pendingRemovedColumn : pendingRemovedColumns) {
                projection.remove(pendingRemovedColumn);
            }

            context.rowIds = rowIds;

            optExpression = OptExpression.builder().with(optExpression).setInputs(List.of()).build();

            optExpression.setLogicalProperty(new LogicalProperty());
            optExpression.getLogicalProperty().setOutputColumns(new ColumnRefSet(projection.keySet()));

            optExpression = introduceFetch(requiredColumns, optExpression, context);
            return optExpression;
        }
    }
}
