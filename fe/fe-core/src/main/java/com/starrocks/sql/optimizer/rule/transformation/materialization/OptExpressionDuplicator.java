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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;
import java.util.Set;

// used in SPJG mv union rewrite
// OptExpressionDuplicator will duplicate the OptExpression tree,
// and rewrite all ColumnRefOperator from bottom to up,
// and put the ColumnRefOperator mapping in columnMapping
// Now only logical Scan/Filter/Projection/Join/Aggregation operator are supported
public class OptExpressionDuplicator {
    private final ColumnRefFactory columnRefFactory;
    // old ColumnRefOperator -> new ColumnRefOperator
    private final Map<ColumnRefOperator, ColumnRefOperator> columnMapping;
    private final ReplaceColumnRefRewriter rewriter;
    private final Table partitionByTable;
    private final Column partitionColumn;
    private final boolean partialPartitionRewrite;
    private final OptimizerContext optimizerContext;

    public OptExpressionDuplicator(MaterializationContext materializationContext) {
        this.columnRefFactory = materializationContext.getQueryRefFactory();
        this.columnMapping = Maps.newHashMap();
        this.rewriter = new ReplaceColumnRefRewriter(columnMapping);
        Pair<Table, Column> partitionInfo = materializationContext.getMv().getDirectTableAndPartitionColumn();
        this.partitionByTable = partitionInfo == null ? null : partitionInfo.first;
        this.partitionColumn = partitionInfo == null ? null : partitionInfo.second;
        this.partialPartitionRewrite = !materializationContext.getMvUpdateInfo().getMvToRefreshPartitionNames().isEmpty();
        this.optimizerContext = materializationContext.getOptimizerContext();
    }

    public OptExpressionDuplicator(ColumnRefFactory columnRefFactory, OptimizerContext optimizerContext) {
        this.columnRefFactory = columnRefFactory;
        this.columnMapping = Maps.newHashMap();
        this.rewriter = new ReplaceColumnRefRewriter(columnMapping);
        this.partitionByTable = null;
        this.partitionColumn = null;
        this.partialPartitionRewrite = false;
        this.optimizerContext = optimizerContext;
    }

    public OptExpression duplicate(OptExpression source) {
        OptExpressionDuplicatorVisitor visitor = new OptExpressionDuplicatorVisitor(optimizerContext, columnMapping, rewriter);
        return source.getOp().accept(visitor, source, null);
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getColumnMapping() {
        return columnMapping;
    }

    public List<ColumnRefOperator> getMappedColumns(List<ColumnRefOperator> originColumns) {
        List<ColumnRefOperator> newColumnRefs = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originColumns) {
            newColumnRefs.add(columnMapping.get(columnRef));
        }
        return newColumnRefs;
    }

    class OptExpressionDuplicatorVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;
        private final Map<Integer, Integer> cteIdMapping = Maps.newHashMap();
        private final Map<ColumnRefOperator, ColumnRefOperator> columnMapping;
        private final ReplaceColumnRefRewriter rewriter;

        OptExpressionDuplicatorVisitor(OptimizerContext optimizerContext,
                                       Map<ColumnRefOperator, ColumnRefOperator> columnMapping,
                                       ReplaceColumnRefRewriter rewriter) {
            this.optimizerContext = optimizerContext;
            this.columnMapping = columnMapping;
            this.rewriter = rewriter;
        }

        private List<OptExpression> processChildren(OptExpression optExpression) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            return inputs;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    ((LogicalScanOperator) optExpression.getOp()).getColRefToColumnMetaMap();
            ImmutableMap.Builder<ColumnRefOperator, Column> columnRefColumnMapBuilder = new ImmutableMap.Builder<>();
            Map<Integer, Integer> relationIdMapping = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, Column> entry : columnRefOperatorColumnMap.entrySet()) {
                ColumnRefOperator key = entry.getKey();
                ColumnRefOperator newColumnRef = columnRefFactory.create(key, key.getType(), key.isNullable());
                columnRefColumnMapBuilder.put(newColumnRef, entry.getValue());
                columnMapping.put(entry.getKey(), newColumnRef);
                columnRefFactory.updateColumnRefToColumns(newColumnRef, columnRefFactory.getColumn(key),
                        columnRefFactory.getColumnRefToTable().get(key));
                Integer newRelationId = relationIdMapping.computeIfAbsent(columnRefFactory.getRelationId(key.getId()),
                        k -> columnRefFactory.getNextRelationId());
                columnRefFactory.updateColumnToRelationIds(newColumnRef.getId(), newRelationId);
            }
            LogicalScanOperator.Builder scanBuilder = (LogicalScanOperator.Builder) opBuilder;

            Map<Column, ColumnRefOperator> columnMetaToColRefMap =
                    ((LogicalScanOperator) optExpression.getOp()).getColumnMetaToColRefMap();
            ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = new ImmutableMap.Builder<>();
            for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColRefMap.entrySet()) {
                ColumnRefOperator key = entry.getValue();
                ColumnRefOperator mapped = columnMapping.computeIfAbsent(key,
                        k -> columnRefFactory.create(k, k.getType(), k.isNullable()));
                columnRefFactory.updateColumnRefToColumns(mapped, columnRefFactory.getColumn(key),
                        columnRefFactory.getColumnRefToTable().get(key));
                Integer newRelationId = relationIdMapping.computeIfAbsent(columnRefFactory.getRelationId(key.getId()),
                        k -> columnRefFactory.getNextRelationId());
                columnRefFactory.updateColumnToRelationIds(mapped.getId(), newRelationId);
                columnMetaToColRefMapBuilder.put(entry.getKey(), mapped);
            }
            ImmutableMap<Column, ColumnRefOperator> newColumnMetaToColRefMap = columnMetaToColRefMapBuilder.build();
            scanBuilder.setColumnMetaToColRefMap(newColumnMetaToColRefMap);

            // process HashDistributionSpec
            if (optExpression.getOp() instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator olapScan = (LogicalOlapScanOperator) optExpression.getOp();
                if (olapScan.getDistributionSpec() instanceof HashDistributionSpec) {
                    HashDistributionSpec newHashDistributionSpec =
                            processHashDistributionSpec((HashDistributionSpec) olapScan.getDistributionSpec(),
                            columnRefFactory, columnMapping);
                    LogicalOlapScanOperator.Builder olapScanBuilder = (LogicalOlapScanOperator.Builder) scanBuilder;
                    olapScanBuilder.setDistributionSpec(newHashDistributionSpec);
                }
            }

            processCommon(opBuilder);

            if (partialPartitionRewrite
                    && optExpression.getOp() instanceof LogicalOlapScanOperator
                    && partitionByTable != null) {
                // maybe partition column is not in the output columns, should add it

                LogicalOlapScanOperator olapScan = (LogicalOlapScanOperator) optExpression.getOp();
                OlapTable table = (OlapTable) olapScan.getTable();
                if (table.getId() == partitionByTable.getId()) {
                    if (!columnRefOperatorColumnMap.containsValue(partitionColumn)) {
                        ColumnRefOperator partitionColumnRef = newColumnMetaToColRefMap.get(partitionColumn);
                        columnRefColumnMapBuilder.put(partitionColumnRef, partitionColumn);
                    }
                }
            }
            ImmutableMap<ColumnRefOperator, Column> newColumnRefColumnMap = columnRefColumnMapBuilder.build();
            scanBuilder.setColRefToColumnMetaMap(newColumnRefColumnMap);

            return OptExpression.create(opBuilder.build());
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            processCommon(opBuilder);

            LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = processProjection(projectOperator.getColumnRefMap());
            LogicalProjectOperator.Builder projectBuilder = (LogicalProjectOperator.Builder) opBuilder;
            projectBuilder.setColumnRefMap(newColumnRefMap);
            return OptExpression.create(projectBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            List<ColumnRefOperator> newGroupKeys = Lists.newArrayList();
            for (ColumnRefOperator groupKey : aggregationOperator.getGroupingKeys()) {
                ColumnRefOperator mapped = getOrCreateColRef(groupKey);
                newGroupKeys.add(mapped);
            }
            LogicalAggregationOperator.Builder aggregationBuilder = (LogicalAggregationOperator.Builder) opBuilder;
            aggregationBuilder.setGroupingKeys(newGroupKeys);
            Map<ColumnRefOperator, CallOperator> newAggregates = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                ColumnRefOperator mapped = getOrCreateColRef(entry.getKey());
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                Preconditions.checkState(newValue instanceof CallOperator);
                newAggregates.put(mapped, (CallOperator) newValue);
            }
            aggregationBuilder.setAggregations(newAggregates);

            List<ColumnRefOperator> newPartitionColumns = Lists.newArrayList();
            List<ColumnRefOperator> partitionColumns = aggregationOperator.getPartitionByColumns();
            if (partitionColumns != null) {
                for (ColumnRefOperator columnRef : partitionColumns) {
                    ColumnRefOperator mapped = getOrCreateColRef(columnRef);
                    newPartitionColumns.add(mapped);
                }
            }
            aggregationBuilder.setPartitionByColumns(newPartitionColumns);
            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            processCommon(opBuilder);

            ScalarOperator onpredicate = joinOperator.getOnPredicate();
            if (onpredicate != null) {
                ScalarOperator newOnPredicate = rewriter.rewrite(onpredicate);
                LogicalJoinOperator.Builder joinBuilder = (LogicalJoinOperator.Builder) opBuilder;
                joinBuilder.setOnPredicate(newOnPredicate);
            }
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();
            if (filterOperator.getPredicate() != null) {
                ScalarOperator newPredicate = rewriter.rewrite(filterOperator.getPredicate());
                LogicalFilterOperator.Builder filterBuilder = (LogicalFilterOperator.Builder) opBuilder;
                filterBuilder.setPredicate(newPredicate);
            }
            return OptExpression.create(opBuilder.build(), inputs);
        }

        private void processSetOperator(LogicalSetOperator setOperator,
                                        LogicalSetOperator.Builder opBuilder) {
            List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList();
            for (List<ColumnRefOperator> childColRefs : setOperator.getChildOutputColumns()) {
                List<ColumnRefOperator> newChildColRefs = Lists.newArrayList();
                for (ColumnRefOperator colRef : childColRefs) {
                    newChildColRefs.add(getNewColRef(colRef));
                }
                newChildOutputColumns.add(newChildColRefs);
            }

            List<ColumnRefOperator> newOutputColumnRefOp = Lists.newArrayList();
            for (ColumnRefOperator colRef : setOperator.getOutputColumnRefOp()) {
                ColumnRefOperator newColumnRef = getOrCreateColRef(colRef);
                newOutputColumnRefOp.add(newColumnRef);
            }
            opBuilder.setChildOutputColumns(newChildOutputColumns);
            opBuilder.setOutputColumnRefOp(newOutputColumnRefOp);
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalIntersect(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalExcept(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalWindowOperator windowOperator = (LogicalWindowOperator) optExpression.getOp();
            LogicalWindowOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(windowOperator);

            // partitions
            List<ScalarOperator> newPartitions = Lists.newArrayList();
            for (ScalarOperator partition : windowOperator.getPartitionExpressions()) {
                ScalarOperator newPartition = getNewScalarOp(partition);
                newPartitions.add(newPartition);
            }
            opBuilder.setPartitionExpressions(newPartitions);

            // ordering
            List<Ordering> newOrderings = Lists.newArrayList();
            for (Ordering ordering : windowOperator.getOrderByElements()) {
                ColumnRefOperator newColRef = getOrCreateColRef(ordering.getColumnRef());
                Ordering newOrdering = new Ordering(newColRef, ordering.isAscending(),
                        ordering.isNullsFirst());
                newOrderings.add(newOrdering);
            }
            opBuilder.setOrderByElements(newOrderings);

            // enforceSortColumns
            List<Ordering> newEnforceSortColumns = Lists.newArrayList();
            for (Ordering ordering : windowOperator.getEnforceSortColumns()) {
                ColumnRefOperator newColRef = getOrCreateColRef(ordering.getColumnRef());
                Ordering newOrdering = new Ordering(newColRef, ordering.isAscending(),
                        ordering.isNullsFirst());
                newEnforceSortColumns.add(newOrdering);
            }
            opBuilder.setEnforceSortColumns(newEnforceSortColumns);

            // window call
            Map<ColumnRefOperator, CallOperator> newWindowCalls = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> e : windowOperator.getWindowCall().entrySet()) {
                ColumnRefOperator newColumnRef = getOrCreateColRef(e.getKey());
                ScalarOperator newCall = getNewScalarOp(e.getValue());
                Preconditions.checkState(newCall instanceof CallOperator);
                newWindowCalls.put(newColumnRef, (CallOperator) newCall);
            }
            opBuilder.setWindowCall(newWindowCalls);

            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalTopN(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalTopNOperator topNOperator = (LogicalTopNOperator) optExpression.getOp();
            LogicalTopNOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(topNOperator);

            // partition bys
            if (topNOperator.getPartitionByColumns() != null) {
                List<ColumnRefOperator> newPartitionBys = Lists.newArrayList();
                for (ColumnRefOperator partitionBy : topNOperator.getPartitionByColumns()) {
                    ColumnRefOperator newColRef = getNewColRef(partitionBy);
                    newPartitionBys.add(newColRef);
                }
                opBuilder.setPartitionByColumns(newPartitionBys);
            }

            // ordering
            List<Ordering> newOrderings = Lists.newArrayList();
            for (Ordering ordering : topNOperator.getOrderByElements()) {
                ColumnRefOperator newColRef = getNewColRef(ordering.getColumnRef());
                Ordering newOrdering = new Ordering(newColRef, ordering.isAscending(),
                        ordering.isNullsFirst());
                newOrderings.add(newOrdering);
            }
            opBuilder.setOrderByElements(newOrderings);

            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalLimit(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalValues(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalValuesOperator valuesOperator = (LogicalValuesOperator) optExpression.getOp();
            LogicalValuesOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(valuesOperator);

            // column refs
            List<ColumnRefOperator> newColRefs = Lists.newArrayList();
            for (ColumnRefOperator colRef : valuesOperator.getColumnRefSet()) {
                ColumnRefOperator newColumnRef = columnRefFactory.create(colRef, colRef.getType(), colRef.isNullable());
                columnMapping.put(colRef, newColumnRef);
                newColRefs.add(newColumnRef);
            }
            opBuilder.setColumnRefSet(newColRefs);
            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalTableFunction(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
            LogicalTableFunctionOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(tableFunctionOperator);

            // fnResultColRef
            List<ColumnRefOperator> newFnResultColRefs = Lists.newArrayList();
            for (ColumnRefOperator fnResultColRef : tableFunctionOperator.getFnResultColRefs()) {
                ColumnRefOperator newColRef = getOrCreateColRef(fnResultColRef);
                newFnResultColRefs.add(newColRef);
            }
            opBuilder.setFnResultColRefs(newFnResultColRefs);

            // outerColRef
            List<ColumnRefOperator> newOuterColRefs = Lists.newArrayList();
            for (ColumnRefOperator outerColRef : tableFunctionOperator.getOuterColRefs()) {
                ColumnRefOperator newColRef = getOrCreateColRef(outerColRef);
                newOuterColRefs.add(newColRef);
            }
            opBuilder.setOuterColRefs(newOuterColRefs);

            // fnParamColumnProject
            List<Pair<ColumnRefOperator, ScalarOperator>> newFnParamColumnProject = Lists.newArrayList();
            for (Pair<ColumnRefOperator, ScalarOperator> p : tableFunctionOperator.getFnParamColumnProject()) {
                ColumnRefOperator newColumnRef = getOrCreateColRef(p.first);
                ScalarOperator newScalarOp = getNewScalarOp(p.second);
                newFnParamColumnProject.add(Pair.create(newColumnRef, newScalarOp));
            }
            opBuilder.setFnParamColumnProject(newFnParamColumnProject);

            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalRepeat(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) optExpression.getOp();
            LogicalRepeatOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(repeatOperator);

            // fnResultColRef
            List<ColumnRefOperator> newOutputGroupings = Lists.newArrayList();
            for (ColumnRefOperator outputGrouping : repeatOperator.getOutputGrouping()) {
                ColumnRefOperator newColRef = getOrCreateColRef(outputGrouping);
                newOutputGroupings.add(newColRef);
            }
            opBuilder.setOutputGrouping(newOutputGroupings);

            // fnResultColRef
            List<List<ColumnRefOperator>> newRepeatColumnRefList = Lists.newArrayList();
            for (List<ColumnRefOperator> repeatColumnRefs : repeatOperator.getRepeatColumnRef()) {
                List<ColumnRefOperator> newRepeatColumnRefs = Lists.newArrayList();
                for (ColumnRefOperator repeatColumnRef : repeatColumnRefs) {
                    ColumnRefOperator newColRef = getNewColRef(repeatColumnRef);
                    newRepeatColumnRefs.add(newColRef);
                }
                newRepeatColumnRefList.add(newRepeatColumnRefs);
            }
            opBuilder.setRepeatColumnRefList(newRepeatColumnRefList);

            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);

            LogicalCTEAnchorOperator cteAnchorOperator = (LogicalCTEAnchorOperator) optExpression.getOp();
            LogicalCTEAnchorOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteAnchorOperator);

            opBuilder.setCteId(getOrCreateCteId(cteAnchorOperator.getCteId()));

            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEProduce(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalCTEProduceOperator cteProduceOperator = (LogicalCTEProduceOperator) optExpression.getOp();
            LogicalCTEProduceOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteProduceOperator);
            opBuilder.setCteId(getOrCreateCteId(cteProduceOperator.getCteId()));
            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalCTEConsumeOperator cteConsumeOperator = (LogicalCTEConsumeOperator) optExpression.getOp();
            LogicalCTEConsumeOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteConsumeOperator);
            opBuilder.setCteId(getOrCreateCteId(cteConsumeOperator.getCteId()));

            // cteOutputColumnRefMap
            Map<ColumnRefOperator, ColumnRefOperator> newCteOutputColumnRefMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : cteConsumeOperator.getCteOutputColumnRefMap().entrySet()) {
                newCteOutputColumnRefMap.put(getOrCreateColRef(e.getKey()), getOrCreateColRef(e.getValue()));
            }
            opBuilder.setCteOutputColumnRefMap(newCteOutputColumnRefMap);

            processCommon(opBuilder);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return null;
        }

        // rewrite shuffle columns and joinEquivalentColumns in HashDistributionSpec
        // because the columns ids have changed
        private HashDistributionSpec processHashDistributionSpec(
                HashDistributionSpec originSpec,
                ColumnRefFactory columnRefFactory,
                Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {

            // HashDistributionDesc
            List<DistributionCol> newColumns = Lists.newArrayList();
            for (DistributionCol column : originSpec.getShuffleColumns()) {
                ColumnRefOperator oldRefOperator = columnRefFactory.getColumnRef(column.getColId());
                ColumnRefOperator newRefOperator = columnMapping.get(oldRefOperator);
                Preconditions.checkNotNull(newRefOperator);
                newColumns.add(new DistributionCol(newRefOperator.getId(), column.isNullStrict()));
            }
            Preconditions.checkState(newColumns.size() == originSpec.getShuffleColumns().size());
            HashDistributionDesc hashDistributionDesc =
                    new HashDistributionDesc(newColumns, originSpec.getHashDistributionDesc().getSourceType());

            EquivalentDescriptor equivDesc = originSpec.getEquivDesc();

            EquivalentDescriptor newEquivDesc = new EquivalentDescriptor(equivDesc.getTableId(), equivDesc.getPartitionIds());
            updateDistributionUnionFind(newEquivDesc.getNullRelaxUnionFind(), equivDesc.getNullStrictUnionFind());
            updateDistributionUnionFind(newEquivDesc.getNullStrictUnionFind(), equivDesc.getNullRelaxUnionFind());

            return new HashDistributionSpec(hashDistributionDesc, newEquivDesc);
        }

        private void updateDistributionUnionFind(UnionFind<DistributionCol> newUnionFind,
                                                UnionFind<DistributionCol> oldUnionFind) {

            for (Set<DistributionCol> distributionColSet : oldUnionFind.getAllGroups()) {
                DistributionCol first = null;
                for (DistributionCol next : distributionColSet) {
                    if (first == null) {
                        first = next;
                    }
                    ColumnRefOperator firstCol = columnRefFactory.getColumnRef(first.getColId());
                    ColumnRefOperator newFirstCol = columnMapping.get(firstCol).cast();

                    ColumnRefOperator nextCol = columnRefFactory.getColumnRef(next.getColId());
                    ColumnRefOperator newNextCol = columnMapping.get(nextCol).cast();
                    newUnionFind.union(first.updateColId(newFirstCol.getId()), next.updateColId(newNextCol.getId()));
                }
            }
        }

        private void processCommon(Operator.Builder opBuilder) {
            // first process predicate, then projection
            ScalarOperator predicate = opBuilder.getPredicate();
            if (predicate != null) {
                ScalarOperator newPredicate = rewriter.rewrite(predicate);
                opBuilder.setPredicate(newPredicate);
            }
            Projection projection = opBuilder.getProjection();
            if (projection != null) {
                Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = processProjection(projection.getColumnRefMap());
                Projection newProjection = new Projection(newColumnRefMap);
                opBuilder.setProjection(newProjection);
            }
        }

        private Map<ColumnRefOperator, ScalarOperator> processProjection(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                ColumnRefOperator column = entry.getKey();
                if (!columnMapping.containsKey(column)) {
                    ColumnRefOperator newColumnRef = columnRefFactory.create(column, column.getType(), column.isNullable());
                    columnMapping.put(column, newColumnRef);
                }
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                newColumnRefMap.put(columnMapping.get(column), newValue);
            }
            return newColumnRefMap;
        }

        private ScalarOperator getNewScalarOp(ScalarOperator scalarOperator) {
            return rewriter.rewrite(scalarOperator);
        }

        private ColumnRefOperator getNewColRef(ColumnRefOperator columnRef) {
            Preconditions.checkState(columnMapping.containsKey(columnRef));
            return columnMapping.get(columnRef);
        }

        private ColumnRefOperator getOrCreateColRef(ColumnRefOperator columnRef) {
            return columnMapping.computeIfAbsent(columnRef,
                    k -> columnRefFactory.create(k, k.getType(), k.isNullable()));
        }

        private int getOrCreateCteId(int cteId) {
            return cteIdMapping.computeIfAbsent(cteId, k -> optimizerContext.getCteContext().getNextCteId());
        }
    }
}
