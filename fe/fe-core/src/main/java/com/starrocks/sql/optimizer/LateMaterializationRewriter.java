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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLookUpOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.Type;
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

// Rewrites a PhysicalPlan so columns are materialized lazily. The rewriter
// discovers which scan outputs can stay deferred, tracks where they must be
// fetched, and injects the necessary fetch / lookup operators while keeping
// projections and logical properties consistent.
public class LateMaterializationRewriter {
    private static final Logger LOG = LogManager.getLogger(LateMaterializationRewriter.class);

    private static final String ROW_SOURCE_ID = "_row_source_id";
    private static final String SCAN_RANGE_ID = "_scan_range_id";
    private static final String ROW_ID = "_row_id";

    public OptExpression rewrite(OptExpression root, OptimizerContext context) {
        CollectorContext collectorContext = new CollectorContext();
        collectorContext.columnRefFactory = context.getColumnRefFactory();

        ColumnCollector columnCollector = new ColumnCollector(context);
        OptExpression newRoot = root.getOp().accept(columnCollector, root, collectorContext);
        adjustFetchPositions(collectorContext);

        newRoot = rewritePlan(newRoot, context, collectorContext);
        newRoot.clearAndInitOutputInfo();
        return newRoot;
    }

    private OptExpression rewritePlan(OptExpression root, OptimizerContext optimizerContext, CollectorContext collectorContext) {
        PlanRewriter rewriter = new PlanRewriter(optimizerContext, collectorContext);

        RewriteContext rewriteContext = new RewriteContext();

        OptExpression newRoot = root.getOp().accept(rewriter, root, rewriteContext);
        if (!collectorContext.unMaterializedColumns.isEmpty()) {
            // if there are still un-materialized columns, should add a fetch at the top of root
            Map<IdentifyOperator, Set<ColumnRefOperator>> columns = collectorContext.unMaterializedColumns;

            Map<ColumnRefOperator, Table> rowIdToTable = new HashMap<>();
            Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToFetchRefColumns = new HashMap<>();
            Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToLookUpRefColumns = new HashMap<>();
            Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns = new HashMap<>();
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

            columns.forEach((identifyOperator, columnRefs) -> {
                PhysicalScanOperator scanOperator = (PhysicalScanOperator) identifyOperator.get();
                Table table = scanOperator.getTable();
                ColumnRefOperator rowidColumnRef = rewriteContext.rowIdColumns.get(identifyOperator);
                rowIdToTable.put(rowidColumnRef, table);

                List<ColumnRefOperator> fetchRefColumns = rewriteContext.rowIdRefColumns.get(identifyOperator);
                rowIdToFetchRefColumns.put(rowidColumnRef, fetchRefColumns);
                // create specific column ref operators for lookup side
                List<ColumnRefOperator> lookupRefColumns = fetchRefColumns.stream().map(columnRefOperator -> {
                    Column column = collectorContext.columnRefFactory.getColumn(columnRefOperator);
                    ColumnRefOperator result = optimizerContext.getColumnRefFactory()
                            .create(columnRefOperator, columnRefOperator.getType(), columnRefOperator.isNullable());
                    columnRefOperatorColumnMap.put(result, column);
                    return result;

                }).collect(Collectors.toList());
                rowIdToLookUpRefColumns.put(rowidColumnRef, lookupRefColumns);

                rowIdToLazyColumns.put(rowidColumnRef, columnRefs);

                Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRef : columnRefs) {
                    columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                }
            });

            PhysicalFetchOperator physicalFetchOperator = new PhysicalFetchOperator(
                    rowIdToTable, rowIdToFetchRefColumns, rowIdToLazyColumns, columnRefOperatorColumnMap);

            PhysicalLookUpOperator physicalLookUpOperator = new PhysicalLookUpOperator(
                    rowIdToTable, rowIdToFetchRefColumns, rowIdToLookUpRefColumns,
                    rowIdToLazyColumns, columnRefOperatorColumnMap);
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
        // @TODO(silverbullet233): we can use statistics to estimate it
        return op instanceof PhysicalJoinOperator
                || op instanceof PhysicalTopNOperator
                || op instanceof PhysicalFilterOperator
                || op instanceof PhysicalLimitOperator;
    }

    // Moves fetch points as far up the operator path as allowed by data
    // filtering semantics to minimize redundant materialization.
    private void adjustFetchPositions(CollectorContext context) {
        if (context.needLookupSources.isEmpty()) {
            return;
        }
        // build path
        for (IdentifyOperator scanOperator : context.needLookupSources) {
            List<IdentifyOperator> paths = new ArrayList<>();
            paths.add(scanOperator);
            {
                IdentifyOperator current = scanOperator;
                while (context.parents.containsKey(current)) {
                    current = context.parents.get(current);
                    paths.add(current);
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < paths.size(); i++) {
                    sb.append(paths.get(i)).append("->");
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

                        if (op instanceof PhysicalDistributionOperator || op instanceof PhysicalScanOperator) {
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

        // used to record all scan operators that have late-materialized columns
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
                    PhysicalScanOperator olapScanOperator = (PhysicalScanOperator) tableColumns.getKey().get();
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
                PhysicalScanOperator sourceOperator = (PhysicalScanOperator) entry.getKey().get();
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
    // Bottom-up traversal that identifies lazy columns and records where they
    // first need to be materialized for correctness.
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
                    !(op instanceof PhysicalProjectOperator && op instanceof PhysicalIcebergScanOperator)) {
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
                if (scanOperator.getOutputColumns().isEmpty()) {
                    return optExpression;
                }
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
                    columnRefOperators.forEach(columnRefOperator -> {
                        if (context.columnSources.containsKey(columnRefOperator)) {
                            materializedBefore(columnRefOperator, scanOperator, context);
                        }
                    });
                }
                if (scanOperator.getProjection() != null) {
                    List<ColumnRefOperator> columnRefOperators = scanOperator.getProjection().getUsedColumns()
                            .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    columnRefOperators.forEach(columnRefOperator -> {
                        if (context.columnSources.containsKey(columnRefOperator)) {
                            materializedBefore(columnRefOperator, scanOperator, context);
                        }
                    });
                }
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalIcebergScan(OptExpression optExpression, CollectorContext context) {

            PhysicalIcebergScanOperator scanOperator = (PhysicalIcebergScanOperator) optExpression.getOp();
            if (scanOperator.getOutputColumns().isEmpty()) {
                return optExpression;
            }
            IdentifyOperator identifyOperator = new IdentifyOperator(scanOperator);

            IcebergTable scanTable = (IcebergTable) scanOperator.getTable();
            if (scanTable.getFormatVersion() >= 3 && scanTable.isParquetFormat()) {
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
                    context.columnSources.put(columnRefOperator, identifyOperator);
                    if (!context.unMaterializedColumns.containsKey(identifyOperator)) {
                        context.unMaterializedColumns.put(identifyOperator, new HashSet<>());
                    }
                    context.unMaterializedColumns.get(identifyOperator).add(columnRefOperator);
                }

                List<ColumnRefOperator> predicateUsedColumns = scanOperator.getScanOperatorPredicates().getUsedColumns()
                        .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                predicateUsedColumns.forEach(columnRefOperator -> {
                    if (context.columnSources.containsKey(columnRefOperator)) {
                        materializedBefore(columnRefOperator, scanOperator, context);
                    }
                });
                if (scanOperator.getProjection() != null) {
                    List<ColumnRefOperator> columnRefOperators = scanOperator.getProjection().getUsedColumns()
                            .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    columnRefOperators.forEach(columnRefOperator -> {
                        if (context.columnSources.containsKey(columnRefOperator)) {
                            materializedBefore(columnRefOperator, scanOperator, context);
                        }
                    });
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
            for (ColumnRefOperator columnRefOperator : usedColumns) {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, aggregateOperator, context);
                }
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalProject(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalProjectOperator projectOperator = (PhysicalProjectOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns = projectOperator.getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory());
            usedColumns.forEach(columnRefOperator -> {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, projectOperator, context);
                }
            });
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
            // we have split projection
            Preconditions.checkState(topNOperator.getProjection() == null);
            List<Ordering> orderings = topNOperator.getOrderSpec().getOrderDescs();
            for (Ordering ordering : orderings) {
                ColumnRefOperator columnRefOperator = ordering.getColumnRef();
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, topNOperator, context);
                }
            }

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
                    // shuffle columns should be required
                    for (DistributionCol col : shuffleColumns) {
                        ColumnRefOperator columnRefOperator = context.columnRefFactory.getColumnRef(col.getColId());
                        if (context.columnSources.containsKey(columnRefOperator)) {
                            materializedBefore(columnRefOperator, distributionOperator, context);
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            return optExpression;
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

            ColumnRefSet requiredColumns = joinOperator.getJoinConditionUsedColumns();
            List<ColumnRefOperator> columnRefOperators = requiredColumns.getColumnRefOperators(context.columnRefFactory);
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, joinOperator, context);
                }
            }
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

            ColumnRefSet requiredColumns = joinOperator.getJoinConditionUsedColumns();
            List<ColumnRefOperator> columnRefOperators = requiredColumns.getColumnRefOperators(context.columnRefFactory);
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, joinOperator, context);
                }
            }
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
            filterOperator.getPredicate().getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory()).forEach(columnRefOperator -> {
                        if (context.columnSources.containsKey(columnRefOperator)) {
                            materializedBefore(columnRefOperator, filterOperator, context);
                        }
                    });

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
            consumeOperator.getCteOutputColumnRefMap().values().forEach(columnRefOperator -> {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, produceOperator, context);
                }
            });
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
                outputColumns.forEach(columnRefOperator -> {
                    if (context.columnSources.containsKey(columnRefOperator)) {
                        materializedBefore(columnRefOperator, unionOperator, context);
                    }
                });
            });
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDecode(OptExpression optExpression, CollectorContext context) {
            optExpression = visitChildren(optExpression, context);
            PhysicalDecodeOperator decodeOperator = (PhysicalDecodeOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns = decodeOperator.getUsedColumns()
                    .getColumnRefOperators(optimizerContext.getColumnRefFactory());
            usedColumns.forEach(columnRefOperator -> {
                if (context.columnSources.containsKey(columnRefOperator)) {
                    materializedBefore(columnRefOperator, decodeOperator, context);
                }
            });
            return optExpression;
        }
    }

    public static class RewriteContext {
        // row id column ref of each scan operator
        Map<IdentifyOperator, ColumnRefOperator> rowIdColumns = new HashMap<>();
        // ref columns of each scan operator
        Map<IdentifyOperator, List<ColumnRefOperator>> rowIdRefColumns = new HashMap<>();
        // already fetched columns
        Set<ColumnRefOperator> fetchedColumns = new HashSet<>();

        public RewriteContext() {
        }
    }

    // rewrite PlanTree from bottom to top, what will do:
    // 1. add ROW_ID column in related OlapScanOperator;
    // 2. insert FetchOperator under the related Operator to read the required columns
    // 3. update Projection and LogicalProperty if necessary
    // @TODO support all operators
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
                // merge ctx
                context.rowIdColumns.putAll(ctx.rowIdColumns);
                context.rowIdRefColumns.putAll(ctx.rowIdRefColumns);
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
                context.rowIdRefColumns.values().forEach(columnRefs -> {
                    // make sure all row id needed columns in projection
                    columnRefs.forEach(columnRef -> {
                        projection.getColumnRefMap().put(columnRef, columnRef);
                    });
                });
                context.rowIdColumns.values().forEach(columnRefOperator -> {
                    projection.getColumnRefMap().put(columnRefOperator, columnRefOperator);
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
            context.rowIdRefColumns.values().forEach(columnRefs -> {
                columnRefs.forEach(columnRef -> {
                    newOutputColumns.add(columnRef);
                });
            });
            context.rowIdColumns.values().forEach(columnRef -> {
                newOutputColumns.add(columnRef);
            });
            logicalProperty.setOutputColumns(new ColumnRefSet(newOutputColumns));
        }

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator physicalOperator = (PhysicalOperator) optExpression.getOp();
            IdentifyOperator identifyOperator = new IdentifyOperator(physicalOperator);

            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            List<OptExpression> inputs = visitChildren(optExpression, context);

            updateProjection(optExpression, context);

            OptExpression result = null;
            if (collectorContext.fetchPositions.containsRow(identifyOperator)) {
                // if there are lazy-materialized columns, we should insert FetchOperator
                Map<IdentifyOperator, Set<ColumnRefOperator>> columns = collectorContext.fetchPositions.row(identifyOperator);
                // row id -> table
                Map<ColumnRefOperator, Table> rowIdToTables = new HashMap<>();
                Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToFetchRefColumns = new HashMap<>();
                Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToLookUpRefColumns = new HashMap<>();
                // row id -> fetched Columns
                Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns = new HashMap<>();
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

                columns.forEach((op, columnRefs) -> {
                    PhysicalScanOperator scanOperator = (PhysicalScanOperator) op.get();
                    Table table = scanOperator.getTable();
                    ColumnRefOperator rowIdColumnRef = context.rowIdColumns.get(op);
                    rowIdToTables.put(rowIdColumnRef, table);
                    List<ColumnRefOperator> fetchRefColumns = context.rowIdRefColumns.get(op);
                    rowIdToFetchRefColumns.put(rowIdColumnRef, fetchRefColumns);
                    List<ColumnRefOperator> lookupRefColumns = fetchRefColumns.stream().map(columnRefOperator -> {
                        Column column = collectorContext.columnRefFactory.getColumn(columnRefOperator);
                        ColumnRefOperator newColumnRef =  collectorContext.columnRefFactory
                                .create(columnRefOperator, columnRefOperator.getType(), columnRefOperator.isNullable());
                        columnRefOperatorColumnMap.put(newColumnRef, column);
                        return newColumnRef;
                    }).collect(Collectors.toList());
                    rowIdToLookUpRefColumns.put(rowIdColumnRef, lookupRefColumns);

                    rowIdToLazyColumns.put(rowIdColumnRef, columnRefs);
                    Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                    // add all related columns into columnRefOperatorColumnMap
                    for (ColumnRefOperator columnRef : columnRefs) {
                        columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                    }

                });
                // update fetched Columns
                context.fetchedColumns.addAll(columnRefOperatorColumnMap.keySet());

                PhysicalFetchOperator physicalFetchOperator = new PhysicalFetchOperator(
                        rowIdToTables, rowIdToFetchRefColumns, rowIdToLazyColumns, columnRefOperatorColumnMap);
                PhysicalLookUpOperator physicalLookUpOperator = new PhysicalLookUpOperator(
                        rowIdToTables, rowIdToFetchRefColumns, rowIdToLookUpRefColumns,
                        rowIdToLazyColumns, columnRefOperatorColumnMap);

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
        public OptExpression visitPhysicalIcebergScan(OptExpression optExpression, RewriteContext context) {
            PhysicalIcebergScanOperator scanOperator = (PhysicalIcebergScanOperator) optExpression.getOp();
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

            ColumnRefOperator rowIdColumnRef = null;
            for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
                ColumnRefOperator columnRefOperator = entry.getKey();
                Column column = entry.getValue();
                if (column.getName().equalsIgnoreCase(ROW_ID)) {
                    rowIdColumnRef = columnRefOperator;
                    break;
                }
            }

            if (rowIdColumnRef == null) {
                // _row_id column is not existed, we should add it
                Column rowIdColumn = new Column(ROW_ID, Type.BIGINT, true);
                ColumnRefOperator columnRefOperator = optimizerContext.getColumnRefFactory()
                        .create(ROW_ID, Type.BIGINT, true);
                optimizerContext.getColumnRefFactory()
                        .updateColumnRefToColumns(columnRefOperator, rowIdColumn, scanOperator.getTable());
                newColumnRefMap.put(columnRefOperator, rowIdColumn);
                rowIdColumnRef = columnRefOperator;
            } else {
                newColumnRefMap.put(rowIdColumnRef, scanOperator.getColRefToColumnMetaMap().get(rowIdColumnRef));
            }

            // generate row source id to distinguish scan operator
            Column rowSourceIdColumn = new Column(ROW_SOURCE_ID, Type.INT, true);
            ColumnRefOperator rowSourceIdColumnRef =
                    optimizerContext.getColumnRefFactory().create(ROW_SOURCE_ID, Type.INT, true);
            optimizerContext.getColumnRefFactory()
                    .updateColumnRefToColumns(rowSourceIdColumnRef, rowSourceIdColumn, scanOperator.getTable());
            newColumnRefMap.put(rowSourceIdColumnRef, rowSourceIdColumn);

            Column scanRangeIdColumn = new Column(SCAN_RANGE_ID, Type.INT, true);
            ColumnRefOperator scanRangeIdColumnRef =
                    optimizerContext.getColumnRefFactory().create(SCAN_RANGE_ID, Type.INT, true);
            optimizerContext.getColumnRefFactory()
                    .updateColumnRefToColumns(scanRangeIdColumnRef, scanRangeIdColumn, scanOperator.getTable());
            newColumnRefMap.put(scanRangeIdColumnRef, scanRangeIdColumn);

            context.rowIdColumns.put(identifyOperator, rowSourceIdColumnRef);
            context.rowIdRefColumns.put(identifyOperator, Arrays.asList(scanRangeIdColumnRef, rowIdColumnRef));


            context.fetchedColumns.addAll(newColumnRefMap.keySet());
            // build a new optExpressions
            PhysicalIcebergScanOperator.Builder builder = PhysicalIcebergScanOperator.builder().withOperator(scanOperator);
            builder.setColRefToColumnMetaMap(newColumnRefMap);
            builder.setGlobalDicts(scanOperator.getGlobalDicts());
            builder.setGlobalDictsExpr(scanOperator.getGlobalDictsExpr());

            OptExpression result = OptExpression.builder().with(optExpression).setOp(builder.build()).build();
            LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
            newProperty.setOutputColumns(new ColumnRefSet(newColumnRefMap.keySet()));
            result.setLogicalProperty(newProperty);
            return result;
        }
    }

}
