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

package com.starrocks.sql.optimizer;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFetchOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLookUpOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// use this to collect all late materialized columns
public class LateMaterializationRewriter {
    private static final Logger LOG = LogManager.getLogger(LateMaterializationRewriter.class);

    public OptExpression rewrite(OptExpression root, OptimizerContext context) {
        LOG.debug("before rewrite, plan : " + root.debugString());
        CollectorContext collectorContext = new CollectorContext();
        collectorContext.columnRefFactory = context.getColumnRefFactory();

        ColumnCollector columnCollector = new ColumnCollector(context);
        OptExpression newRoot = root.getOp().accept(columnCollector, root, collectorContext);
        adjustFetchPositions(collectorContext);

        newRoot = rewritePlan(newRoot, context, collectorContext);
        newRoot.clearAndInitOutputInfo();
        LOG.debug("after PlanRewriter,\n" + newRoot.debugString());
        return newRoot;
    }

    private OptExpression rewritePlan(OptExpression root, OptimizerContext optimizerContext, CollectorContext collectorContext) {
        PlanRewriter rewriter = new PlanRewriter(optimizerContext, collectorContext);

        RewriteContext rewriteContext = new RewriteContext();

        OptExpression newRoot = root.getOp().accept(rewriter, root, rewriteContext);
        if (!collectorContext.unMaterializedColumns.isEmpty()) {
            // if there are still un-materialized columns, should add a fetch at the top of root
            Map<IdentifyOperator, Set<ColumnRefOperator>> columns = collectorContext.unMaterializedColumns;

            Map<ColumnRefOperator, Table> rowidToTable = new HashMap<>();
            Map<ColumnRefOperator, Set<ColumnRefOperator>> rowidToColumns = new HashMap<>();
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

            columns.forEach((identifyOperator, columnRefs) -> {
                PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) identifyOperator.get();
                Table table = scanOperator.getTable();
                ColumnRefOperator rowidColumnRef = rewriteContext.rowIdColumns.get(identifyOperator);
                rowidToTable.put(rowidColumnRef, table);
                rowidToColumns.put(rowidColumnRef, columnRefs);
                Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRef : columnRefs) {
                    columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                }
            });

            PhysicalFetchOperator physicalFetchOperator =
                    new PhysicalFetchOperator(rowidToTable, rowidToColumns, columnRefOperatorColumnMap);

            PhysicalLookUpOperator physicalLookUpOperator =
                    new PhysicalLookUpOperator(rowidToTable, rowidToColumns, columnRefOperatorColumnMap);
            OptExpression lookupOpt = OptExpression.create(physicalLookUpOperator);
            lookupOpt.setLogicalProperty(new LogicalProperty());


            OptExpression fetchOpt = OptExpression.create(physicalFetchOperator, newRoot);
            fetchOpt.setLogicalProperty(new LogicalProperty());
            fetchOpt.setStatistics(newRoot.getStatistics());
            fetchOpt.getInputs().add(lookupOpt);

            return fetchOpt;
        }
        return newRoot;
    }

    private boolean mayFilterData(PhysicalOperator op) {
        // @TODO(silverbullet233): we should base on statistics
        return op instanceof PhysicalJoinOperator
                || op instanceof PhysicalTopNOperator
                || op instanceof PhysicalFilterOperator
                || op instanceof PhysicalLimitOperator;
    }

    private void adjustFetchPositions(CollectorContext context) {
        for (IdentifyOperator scanOperator : context.needLookupSources) {
            List<IdentifyOperator> paths = new ArrayList<>();
            paths.add(scanOperator);
            {
                IdentifyOperator current = scanOperator;
                while (context.parents.containsKey(current)) {
                    current = context.parents.get(current);
                    paths.add(current);
                }
            }
            // iterate paths, try to push fetch
            for (int i = 0; i < paths.size(); i++) {
                IdentifyOperator operator = paths.get(i);
                if (context.fetchPositions.contains(operator, scanOperator)) {
                    // we should check if we can push
                    int idx = i;
                    for (int j = i - 1; j >= 0; j--) {
                        PhysicalOperator op = paths.get(j).get();
                        // if there is an operator that can filter data, we can't push it
                        if (mayFilterData(op)) {
                            break;
                        }

                        if (op instanceof PhysicalDistributionOperator || op instanceof PhysicalOlapScanOperator) {
                            idx = j;
                        }
                    }
                    if (idx < i) {
                        // we can move fetch to operator[idx]
                        IdentifyOperator targetParent = paths.get(idx);
                        Set<ColumnRefOperator> columnRefOperators = context.fetchPositions.get(operator, scanOperator);
                        context.fetchPositions.remove(operator, scanOperator);
                        if (!context.fetchPositions.contains(targetParent, scanOperator)) {
                            context.fetchPositions.put(targetParent, scanOperator, new HashSet<>());
                        }
                        context.fetchPositions.get(targetParent, scanOperator).addAll(columnRefOperators);
                    }

                }
            }
        }
    }

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
            StringBuilder sb = new StringBuilder();
            sb.append(hashCode).append(":").append(physicalOperator);
            return sb.toString();
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
            if (hashCode() != o.hashCode()) {
                return false;
            }
            return physicalOperator.equals(((IdentifyOperator) o).get());
        }
    }


    public static class CollectorContext {
        // for each column, record the first-used position
        Map<ColumnRefOperator, IdentifyOperator> materializedPosition = new HashMap<>();
        // un-materialized columns
        Map<IdentifyOperator, Set<ColumnRefOperator>> unMaterializedColumns = new HashMap<>();
        // which ScanOperator the column comes from
        Map<ColumnRefOperator, IdentifyOperator> columnSources = new HashMap<>();

        // used to record the fetch position. for example,
        // a row in the table (A, B, [C,D]) indicates that
        // the Column C and D from PhysicalOlapScan B should be fetched before PhysicalOperator A
        com.google.common.collect.Table<IdentifyOperator, IdentifyOperator, Set<ColumnRefOperator>>
                fetchPositions = HashBasedTable.create();

        // used to record all olap scan operators that have late-materialized columns
        Set<IdentifyOperator> needLookupSources = new HashSet<>();

        // use this to find path to root
        Map<IdentifyOperator, IdentifyOperator> parents = new HashMap<>();

        // cte id -> cte producer
        Map<Integer, PhysicalCTEProduceOperator> cteProducerMap = new HashMap<>();

        ColumnRefFactory columnRefFactory;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("fetchPositions size " + fetchPositions.size()).append("\n");
            for (IdentifyOperator operator : fetchPositions.rowKeySet()) {
                Map<IdentifyOperator, Set<ColumnRefOperator>> columns = fetchPositions.row(operator);
                sb.append("parent operator [" + operator + "], materialized columns [");
                for (Map.Entry<IdentifyOperator, Set<ColumnRefOperator>> tableColumns : columns.entrySet()) {
                    PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) tableColumns.getKey().get();
                    sb.append("table " + olapScanOperator.getTable().getId() + ": [");
                    for (ColumnRefOperator column : tableColumns.getValue()) {
                        sb.append(column.getId()).append(",");
                    }
                    sb.append("],");
                }
                sb.append("]\n");
            }

            sb.append("un-materialized columns " + unMaterializedColumns.size() + " [");
            for (Map.Entry<IdentifyOperator, Set<ColumnRefOperator>> entry : unMaterializedColumns.entrySet()) {
                PhysicalOlapScanOperator sourceOperator = (PhysicalOlapScanOperator) entry.getKey().get();
                sb.append("table " + sourceOperator.getTable().getId() + ": [");
                for (ColumnRefOperator columnRefOperator : entry.getValue()) {
                    sb.append(columnRefOperator.getId()).append(",");
                }
                sb.append("]\n");
            }
            sb.append("]\n");
            return sb.toString();
        }
    }

    // Traverse the PlanTree from bottom to top,
    // find all the columns that can be lazy read and where they need to be materialized
    public static class ColumnCollector extends OptExpressionVisitor<OptExpression, CollectorContext> {
        private OptimizerContext optimizerContext;

        public ColumnCollector(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        private void materializedBefore(ColumnRefOperator columnRefOperator,
                                        PhysicalOperator physicalOperator, CollectorContext context) {
            if (!context.materializedPosition.containsKey(columnRefOperator)) {
                IdentifyOperator operator = new IdentifyOperator(physicalOperator);

                context.materializedPosition.put(columnRefOperator, operator);
                IdentifyOperator sourceOperator = context.columnSources.get(columnRefOperator);

                context.needLookupSources.add(sourceOperator);
                if (!context.fetchPositions.contains(operator, sourceOperator)) {
                    context.fetchPositions.put(operator, sourceOperator, new HashSet<>());
                }
                context.fetchPositions.get(operator, sourceOperator).add(columnRefOperator);
                if (context.unMaterializedColumns.containsKey(sourceOperator)) {
                    context.unMaterializedColumns.get(sourceOperator).remove(columnRefOperator);
                    if (context.unMaterializedColumns.get(sourceOperator).isEmpty()) {
                        context.unMaterializedColumns.remove(sourceOperator);
                    }

                }
                LOG.debug("materialize column " + columnRefOperator + " before " + operator);
            }
        }

        private void materializedBefore(List<ColumnRefOperator> columnRefOperators,
                                        PhysicalOperator physicalOperator, CollectorContext context) {
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, physicalOperator, context);
                }
            }
        }

        // for operator with projection, we split it into operator -> project
        private OptExpression splitProjection(OptExpression optExpression) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            Projection projection = op.getProjection();
            if (projection != null) {
                // remove projection from the original operator and create a new one
                PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(
                        projection.getColumnRefMap(), projection.getCommonSubOperatorMap());

                op.setProjection(null);
                op.clearRowOutputInfo();

                RowOutputInfo newRowOutputInfo = optExpression.getRowOutputInfo();
                LogicalProperty newLogicalProperty = new LogicalProperty(optExpression.getLogicalProperty());

                newLogicalProperty.setOutputColumns(newRowOutputInfo.getOutputColumnRefSet());

                optExpression.setLogicalProperty(newLogicalProperty);

                OptExpression result = OptExpression.create(projectOperator, optExpression);
                result.setLogicalProperty(optExpression.getLogicalProperty());
                result.setStatistics(optExpression.getStatistics());
                // @TODO(silverbullet233): remove unused columns from statistics?
                return result;
            }

            return optExpression;
        }

        private OptExpression visitChildren(OptExpression optExpression, CollectorContext context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                OptExpression newInput = input.getOp().accept(this, input, context);
                context.parents.put(new IdentifyOperator((PhysicalOperator) newInput.getOp()),
                        new IdentifyOperator((PhysicalOperator) optExpression.getOp()));
                inputs.add(newInput);
            }
            return OptExpression.builder().with(optExpression).setInputs(inputs).build();
        }

        @Override
        public OptExpression visit(OptExpression optExpression, CollectorContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            if (op.getProjection() != null &&
                    !(op instanceof PhysicalProjectOperator && op instanceof PhysicalOlapScanOperator)) {
                OptExpression newRoot = splitProjection(optExpression);
                return newRoot.getOp().accept(this, newRoot, context);
            }
            return visitChildren(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, CollectorContext context) {
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(scanOperator);

            OlapTable scanTable = (OlapTable) scanOperator.getTable();
            if (scanTable.getKeysType() == KeysType.DUP_KEYS || scanTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
                    context.columnSources.put(columnRefOperator, identifyOperator);
                    if (!context.unMaterializedColumns.containsKey(identifyOperator)) {
                        context.unMaterializedColumns.put(identifyOperator, new HashSet<>());
                    }
                    context.unMaterializedColumns.get(identifyOperator).add(columnRefOperator);
                }

                if (scanOperator.getPredicate() != null) {
                    List<ColumnRefOperator> columnRefOperators = scanOperator.getPredicate().getUsedColumns()
                            .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    materializedBefore(columnRefOperators, scanOperator, context);
                }
                if (scanOperator.getProjection() != null) {
                    List<ColumnRefOperator> columnRefOperators = scanOperator.getProjection().getUsedColumns()
                            .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    materializedBefore(columnRefOperators, scanOperator, context);
                }
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            } else {
                optExpression = visitChildren(optExpression, context);
            }
            // for aggregate operator, we should materialize all used columns
            PhysicalHashAggregateOperator aggregateOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns =
                    aggregateOperator.getUsedColumns().getColumnRefOperators(optimizerContext.getColumnRefFactory());
            materializedBefore(usedColumns, aggregateOperator, context);
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalProject(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalProjectOperator projectOperator = (PhysicalProjectOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns = projectOperator.getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory());
            materializedBefore(usedColumns, projectOperator, context);
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalTopN(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            } else {
                optExpression = visitChildren(optExpression, context);
            }
            PhysicalTopNOperator topNOperator = (PhysicalTopNOperator) optExpression.getOp();
            List<Ordering> orderings = topNOperator.getOrderSpec().getOrderDescs();
            List<ColumnRefOperator> usedColumns = orderings.stream()
                    .map(ordering -> ordering.getColumnRef()).collect(Collectors.toList());
            materializedBefore(usedColumns, topNOperator, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDistribution(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);

            PhysicalDistributionOperator distributionOperator = (PhysicalDistributionOperator) optExpression.getOp();
            DistributionSpec distributionSpec = distributionOperator.getDistributionSpec();
            // handle different spec
            switch (distributionSpec.getType()) {
                case SHUFFLE: {
                    HashDistributionSpec hashDistributionSpec = (HashDistributionSpec) distributionSpec;
                    List<DistributionCol> shuffleColumns = hashDistributionSpec.getShuffleColumns();
                    List<ColumnRefOperator> usedColumns = shuffleColumns.stream()
                            .map(col -> context.columnRefFactory.getColumnRef(col.getColId())).collect(Collectors.toList());
                    materializedBefore(usedColumns, distributionOperator, context);
                    break;
                }
                default:
                    break;
            }

            return optExpression;
        }

        private void materializeJoinConditionColumns(PhysicalJoinOperator joinOperator, CollectorContext context) {
            List<ColumnRefOperator> usedColumns = joinOperator
                    .getJoinConditionUsedColumns().getColumnRefOperators(context.columnRefFactory);
            materializedBefore(usedColumns, joinOperator, context);
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            } else {
                optExpression = visitChildren(optExpression, context);
            }

            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
            materializeJoinConditionColumns(joinOperator, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalNestLoopJoin(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            } else {
                optExpression = visitChildren(optExpression, context);
            }

            PhysicalNestLoopJoinOperator joinOperator = (PhysicalNestLoopJoinOperator) optExpression.getOp();
            materializeJoinConditionColumns(joinOperator, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalFilter(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            }
            optExpression = visitChildren(optExpression, context);

            PhysicalFilterOperator filterOperator = (PhysicalFilterOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns = filterOperator.getPredicate()
                    .getUsedColumns().getColumnRefOperators(optimizerContext.getColumnRefFactory());
            materializedBefore(usedColumns, filterOperator, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalCTEProduce(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalCTEProduceOperator produceOperator = (PhysicalCTEProduceOperator) optExpression.getOp();
            context.cteProducerMap.put(produceOperator.getCteId(), produceOperator);
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalCTEConsume(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalCTEConsumeOperator consumeOperator = (PhysicalCTEConsumeOperator) optExpression.getOp();
            Preconditions.checkState(context.cteProducerMap.containsKey(consumeOperator.getCteId()));
            PhysicalCTEProduceOperator produceOperator = context.cteProducerMap.get(consumeOperator.getCteId());
            // for consumer-used columns, we should materialize it before CTEProducer
            List<ColumnRefOperator> usedColumns = new ArrayList<>(consumeOperator.getCteOutputColumnRefMap().values());
            materializedBefore(usedColumns, produceOperator, context);

            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalUnion(OptExpression optExpression, CollectorContext context) {
            if (optExpression.getOp().getProjection() != null) {
                optExpression = splitProjection(optExpression);
                return optExpression.getOp().accept(this, optExpression, context);
            } else {
                optExpression = visitChildren(optExpression, context);
            }
            PhysicalUnionOperator unionOperator = (PhysicalUnionOperator) optExpression.getOp();
            // all child output column should materialize
            List<List<ColumnRefOperator>> childOutputColumns = unionOperator.getChildOutputColumns();
            childOutputColumns.forEach(outputColumns -> {
                materializedBefore(outputColumns, unionOperator, context);
            });
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDecode(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalDecodeOperator decodeOperator = (PhysicalDecodeOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns = decodeOperator.getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory());
            materializedBefore(usedColumns, decodeOperator, context);
            return optExpression;
        }
    }

    public static class RewriteContext {
        // row id column of each olap scan operator
        Map<IdentifyOperator, ColumnRefOperator> rowIdColumns = new HashMap<>();
        // already fetched columns
        Set<ColumnRefOperator> fetchedColumns = new HashSet<>();
    }

    // rewrite PlanTree from bottom to top, what will do:
    // 1. add ROW_ID column in related OlapScanOperator;
    // 2. insert FetchOperator under the related Operator to read the required columns
    // 3. update Projection and LogicalProperty if necessary
    public static class PlanRewriter extends OptExpressionVisitor<OptExpression, RewriteContext> {
        private OptimizerContext optimizerContext;
        private CollectorContext collectorContext;

        public PlanRewriter(OptimizerContext optimizerContext, CollectorContext collectorContext) {
            this.optimizerContext = optimizerContext;
            this.collectorContext = collectorContext;
        }

        private List<OptExpression> visitChildren(OptExpression optExpression, RewriteContext context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                RewriteContext ctx = new RewriteContext();
                inputs.add(input.getOp().accept(this, input, ctx));
                context.rowIdColumns.putAll(ctx.rowIdColumns);
                context.fetchedColumns.addAll(ctx.fetchedColumns);
            }
            return inputs;
        }


        public void updateProjection(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(op);

            Projection projection = op.getProjection();
            if (projection != null) {
                // 1. collect all column ref operators that only contain fetched column,
                Set<ColumnRefOperator> pendingRemovedColumnRefs = new HashSet<>();
                projection.getColumnRefMap().forEach(((columnRefOperator, scalarOperator) -> {
                    List<ColumnRefOperator> usedColumns =
                            scalarOperator.getUsedColumns().getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    boolean allFetched = usedColumns.stream().allMatch(col -> {
                        if (!collectorContext.columnSources.containsKey(col)) {
                            // if not original column, ignore it
                            return true;
                        }
                        return context.fetchedColumns.contains(col);
                    });
                    // if not all columns are fetched, we should remove it from projection
                    if (!allFetched) {
                        pendingRemovedColumnRefs.add(columnRefOperator);
                    }
                }));

                Map<IdentifyOperator, Set<ColumnRefOperator>> map1 = collectorContext.fetchPositions.row(identifyOperator);

                projection.getColumnRefMap().entrySet().removeIf(entry -> {
                    ColumnRefOperator columnRef = entry.getKey();
                    if (pendingRemovedColumnRefs.contains(columnRef)) {
                        return true;
                    }
                    return false;
                });

                context.rowIdColumns.values().forEach(columnRef -> {
                    projection.getColumnRefMap().put(columnRef, columnRef);
                });
                collectorContext.fetchPositions.row(identifyOperator).putAll(map1);
            }

            // update logical property
            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
            List<ColumnRefOperator> outputColumns
                    = logicalProperty.getOutputColumns().getColumnRefOperators(columnRefFactory);
            List<ColumnRefOperator> newOutputColumns = outputColumns.stream().filter(columnRefOperator -> {
                if (!columnRefFactory.getColumnRefToColumns().containsKey(columnRefOperator)) {
                    // not original column, should keep
                    return true;
                }
                if (!context.fetchedColumns.contains(columnRefOperator)) {
                    return false;
                }
                return true;
            }).collect(Collectors.toList());
            // 2. add necessary row id
            context.rowIdColumns.values().forEach(entry -> {
                newOutputColumns.add(entry);
            });
            logicalProperty.setOutputColumns(new ColumnRefSet(newOutputColumns));
        }

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator physicalOperator = (PhysicalOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(physicalOperator);

            List<OptExpression> inputs = visitChildren(optExpression, context);

            updateProjection(optExpression, context);

            OptExpression result = null;
            if (collectorContext.fetchPositions.containsRow(identifyOperator)) {
                // if there are lazy-materialized columns, we should insert FetchOperator
                Map<IdentifyOperator, Set<ColumnRefOperator>> columns = collectorContext.fetchPositions.row(identifyOperator);
                // row id -> table
                Map<ColumnRefOperator, Table> rowidToTables = new HashMap<>();
                // row id -> fetched Columns
                Map<ColumnRefOperator, Set<ColumnRefOperator>> rowidToColumns = new HashMap<>();
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

                columns.forEach((op, columnRefs) -> {
                    PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) op.get();
                    Table table = scanOperator.getTable();
                    ColumnRefOperator rowIdColumnRef = context.rowIdColumns.get(op);
                    rowidToTables.put(rowIdColumnRef, table);
                    rowidToColumns.put(rowIdColumnRef, columnRefs);
                    Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                    for (ColumnRefOperator columnRef : columnRefs) {
                        columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                    }
                });
                // update fetched Columns
                context.fetchedColumns.addAll(columnRefOperatorColumnMap.keySet());

                PhysicalFetchOperator physicalFetchOperator =
                        new PhysicalFetchOperator(rowidToTables, rowidToColumns, columnRefOperatorColumnMap);

                PhysicalLookUpOperator physicalLookUpOperator =
                        new PhysicalLookUpOperator(rowidToTables, rowidToColumns, columnRefOperatorColumnMap);

                OptExpression lookupOpt = OptExpression.create(physicalLookUpOperator);
                // we just set an empty property, it will be updated at the end
                lookupOpt.setLogicalProperty(new LogicalProperty());

                if (physicalOperator instanceof PhysicalJoinOperator) {
                    // for join operator, we always put fetch operator to the probe side
                    OptExpression optFromProbeSide = inputs.get(0);
                    OptExpression fetchOpt = OptExpression.create(
                            physicalFetchOperator, Arrays.asList(optFromProbeSide, lookupOpt));
                    fetchOpt.setLogicalProperty(new LogicalProperty());
                    fetchOpt.setStatistics(optFromProbeSide.getStatistics());
                    inputs.set(0, fetchOpt);

                    result = OptExpression.builder().with(optExpression).setInputs(inputs).build();
                } else {
                    inputs.add(lookupOpt);

                    OptExpression fetchOpt = OptExpression.create(physicalFetchOperator, inputs);
                    fetchOpt.setLogicalProperty(new LogicalProperty());
                    fetchOpt.setStatistics(optExpression.getStatistics());

                    result = OptExpression.builder().with(optExpression).setInputs(Arrays.asList(fetchOpt)).build();
                }
            }

            if (result == null) {
                result = OptExpression.builder().with(optExpression).setInputs(inputs).build();
            }
            return result;
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, RewriteContext context) {
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(scanOperator);
            if (!collectorContext.needLookupSources.contains(identifyOperator)) {
                return optExpression;
            }

            Set<ColumnRefOperator> columnRefOperators =
                    collectorContext.fetchPositions.contains(identifyOperator, identifyOperator) ?
                    collectorContext.fetchPositions.get(identifyOperator, identifyOperator) : new HashSet<>();

            if (columnRefOperators.size() == scanOperator.getColRefToColumnMetaMap().size()) {
                // all column need fetch, no need to rewrite
                context.fetchedColumns.addAll(columnRefOperators);
                return optExpression;
            }

            // modify output columns
            Map<ColumnRefOperator, Column> newColumnRefMap =
                    scanOperator.getColRefToColumnMetaMap().entrySet().stream()
                            .filter(entry -> columnRefOperators.contains(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            context.fetchedColumns.addAll(newColumnRefMap.keySet());
            // generate row id column
            Column rowIdColumn = new Column("ROW_ID", Type.ROW_ID, false);
            ColumnRefOperator columnRefOperator = optimizerContext.getColumnRefFactory().create("ROW_ID", Type.ROW_ID, false);
            newColumnRefMap.put(columnRefOperator, rowIdColumn);
            context.rowIdColumns.put(identifyOperator, columnRefOperator);

            // build a new optExpressions
            PhysicalOlapScanOperator.Builder builder = PhysicalOlapScanOperator.builder().withOperator(scanOperator);
            builder.setColRefToColumnMetaMap(newColumnRefMap);

            OptExpression result = OptExpression.builder().with(optExpression).setOp(builder.build()).build();
            LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
            newProperty.setOutputColumns(new ColumnRefSet(newColumnRefMap.keySet()));
            result.setLogicalProperty(newProperty);
            return result;
        }
    }

}
