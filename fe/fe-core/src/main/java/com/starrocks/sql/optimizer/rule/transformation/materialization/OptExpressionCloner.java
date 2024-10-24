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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Cloner is used to clone OptExpression without changing original column ref ids.
 * NOTE:
 * - ensure each logical operator is cloned which is a different object from input operator.
 * - ensure each scalar operator is cloned which is a different object from input operator.
 */
public class OptExpressionCloner {
    private static final OptExpressionCloneVisitor INSTANCE = new OptExpressionCloneVisitor();

    public static OptExpression clone(OptExpression src) {
        return src.getOp().accept(INSTANCE, src, null);
    }

    static class OptExpressionCloneVisitor extends OptExpressionVisitor<OptExpression, Void> {
        public OptExpressionCloneVisitor() {
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

            LogicalScanOperator scanOperator = (LogicalScanOperator) optExpression.getOp();
            opBuilder.withOperator(scanOperator);

            ImmutableMap.Builder<ColumnRefOperator, Column> columnRefColumnMapBuilder = new ImmutableMap.Builder<>();
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
            for (Map.Entry<ColumnRefOperator, Column> entry : columnRefOperatorColumnMap.entrySet()) {
                columnRefColumnMapBuilder.put((ColumnRefOperator) entry.getKey().clone(), entry.getValue());
            }
            LogicalScanOperator.Builder scanBuilder = (LogicalScanOperator.Builder) opBuilder;

            Map<Column, ColumnRefOperator> columnMetaToColRefMap = scanOperator.getColumnMetaToColRefMap();
            ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = new ImmutableMap.Builder<>();
            for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColRefMap.entrySet()) {
                columnMetaToColRefMapBuilder.put(entry.getKey(), (ColumnRefOperator) entry.getValue().clone());
            }
            ImmutableMap<Column, ColumnRefOperator> newColumnMetaToColRefMap = columnMetaToColRefMapBuilder.build();
            scanBuilder.setColumnMetaToColRefMap(newColumnMetaToColRefMap);

            // process HashDistributionSpec
            if (scanOperator instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator olapScan = (LogicalOlapScanOperator) scanOperator;
                LogicalOlapScanOperator.Builder olapScanBuilder = (LogicalOlapScanOperator.Builder) scanBuilder;
                if (olapScan.getDistributionSpec() instanceof HashDistributionSpec) {
                    HashDistributionSpec newHashDistributionSpec =
                            processHashDistributionSpec((HashDistributionSpec) olapScan.getDistributionSpec());
                    olapScanBuilder.setDistributionSpec(newHashDistributionSpec);
                }

                List<ScalarOperator> prunedPartitionPredicates = olapScan.getPrunedPartitionPredicates();
                if (prunedPartitionPredicates != null && !prunedPartitionPredicates.isEmpty()) {
                    List<ScalarOperator> newPrunedPartitionPredicates = Lists.newArrayList();
                    for (ScalarOperator predicate : prunedPartitionPredicates) {
                        newPrunedPartitionPredicates.add(predicate.clone());
                    }
                    olapScanBuilder.setPrunedPartitionPredicates(newPrunedPartitionPredicates);
                }
                if (olapScan.getSelectedPartitionId() != null) {
                    olapScanBuilder.setSelectedPartitionId(Lists.newArrayList(olapScan.getSelectedPartitionId()));
                }
                if (olapScan.getPartitionNames() != null) {
                    olapScanBuilder.setPartitionNames(new PartitionNames(olapScan.getPartitionNames()));
                }
                if (olapScan.getSelectedTabletId() != null) {
                    olapScanBuilder.setSelectedTabletId(Lists.newArrayList(olapScan.getSelectedTabletId()));
                }
            }

            processCommon(opBuilder, scanOperator);

            ImmutableMap<ColumnRefOperator, Column> newColumnRefColumnMap = columnRefColumnMapBuilder.build();
            scanBuilder.setColRefToColumnMetaMap(newColumnRefColumnMap);

            // process external table scan operator's predicates
            LogicalScanOperator newScanOperator = (LogicalScanOperator) opBuilder.build();
            if (!(scanOperator instanceof LogicalOlapScanOperator)) {
                processExternalTableScanOperator(newScanOperator);
            }
            return OptExpression.create(newScanOperator);
        }

        private void processExternalTableScanOperator(LogicalScanOperator newScanOperator) {
            try {
                ScanOperatorPredicates scanOperatorPredicates = newScanOperator.getScanOperatorPredicates();
                newScanOperator.setScanOperatorPredicates(scanOperatorPredicates.clone());
            } catch (AnalysisException e) {
                // ignore exception
            }
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            processCommon(opBuilder, (LogicalOperator) optExpression.getOp());

            LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = projectOperator.getColumnRefMap()
                    .entrySet()
                    .stream()
                    .map(e -> Pair.create(e.getKey().clone(), e.getValue().clone()))
                    .collect(Collectors.toMap(x -> (ColumnRefOperator) x.first, x -> x.second));
            LogicalProjectOperator.Builder projectBuilder = (LogicalProjectOperator.Builder) opBuilder;
            projectBuilder.setColumnRefMap(newColumnRefMap);
            return OptExpression.create(projectBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);

            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            LogicalAggregationOperator.Builder aggregationBuilder = (LogicalAggregationOperator.Builder) opBuilder;
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            // group by keys
            List<ColumnRefOperator> clonedGroupingKeys = cloneColumnRefs(aggregationOperator.getGroupingKeys());
            aggregationBuilder.setGroupingKeys(clonedGroupingKeys);
            // aggregations
            Map<ColumnRefOperator, CallOperator> newAggregations = aggregationOperator.getAggregations()
                    .entrySet()
                    .stream()
                    .map(entry -> Pair.create(entry.getKey().clone(), entry.getValue().clone()))
                    .collect(Collectors.toMap(x -> (ColumnRefOperator) x.first, x -> (CallOperator) x.second));
            aggregationBuilder.setAggregations(newAggregations);

            List<ColumnRefOperator> partitionColumns = aggregationOperator.getPartitionByColumns();
            if (partitionColumns != null) {
                List<ColumnRefOperator> newPartitionColumns = cloneColumnRefs(partitionColumns);
                aggregationBuilder.setPartitionByColumns(newPartitionColumns);
            }
            processCommon(opBuilder, aggregationOperator);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            processCommon(opBuilder, joinOperator);

            ScalarOperator onPredicate = joinOperator.getOnPredicate();
            if (onPredicate != null) {
                LogicalJoinOperator.Builder joinBuilder = (LogicalJoinOperator.Builder) opBuilder;
                joinBuilder.setOnPredicate(onPredicate.clone());
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
                filterOperator.setPredicate(filterOperator.getPredicate().clone());
            }
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalAssertOneRow(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            processCommon(opBuilder, (LogicalOperator) optExpression.getOp());
            return OptExpression.create(opBuilder.build(), inputs);
        }

        private void processSetOperator(LogicalSetOperator setOperator,
                                        LogicalSetOperator.Builder opBuilder) {
            List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList(setOperator.getChildOutputColumns());
            opBuilder.setChildOutputColumns(newChildOutputColumns);

            List<ColumnRefOperator> newOutputColumnRefOp = Lists.newArrayList(setOperator.getOutputColumnRefOp());
            opBuilder.setOutputColumnRefOp(newOutputColumnRefOp);
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder, setOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalIntersect(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder, setOperator);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalExcept(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator setOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(setOperator);
            processSetOperator(setOperator, opBuilder);
            processCommon(opBuilder, setOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalWindowOperator windowOperator = (LogicalWindowOperator) optExpression.getOp();
            LogicalWindowOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(windowOperator);

            // partitions
            List<ScalarOperator> newPartitions = windowOperator.getPartitionExpressions()
                    .stream()
                    .map(ScalarOperator::clone)
                    .collect(Collectors.toList());
            opBuilder.setPartitionExpressions(newPartitions);

            // ordering
            List<Ordering> newOrderings = Lists.newArrayList(windowOperator.getOrderByElements());
            opBuilder.setOrderByElements(newOrderings);

            // enforceSortColumns
            List<Ordering> newEnforceSortColumns = Lists.newArrayList(windowOperator.getEnforceSortColumns());
            opBuilder.setEnforceSortColumns(newEnforceSortColumns);

            // window call
            Map<ColumnRefOperator, CallOperator> newWindowCalls = windowOperator.getWindowCall()
                    .entrySet()
                    .stream()
                    .map(entry -> Pair.create(entry.getKey().clone(), entry.getValue().clone()))
                    .collect(Collectors.toMap(x -> (ColumnRefOperator) x.first, x -> (CallOperator) x.second));
            opBuilder.setWindowCall(newWindowCalls);

            processCommon(opBuilder, windowOperator);

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
                List<ColumnRefOperator> newPartitionBys = cloneColumnRefs(topNOperator.getPartitionByColumns());
                opBuilder.setPartitionByColumns(newPartitionBys);
            }

            // ordering
            List<Ordering> newOrderings = Lists.newArrayList(topNOperator.getOrderByElements());
            opBuilder.setOrderByElements(newOrderings);

            processCommon(opBuilder, topNOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalLimit(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalSetOperator limitOperator = (LogicalSetOperator) optExpression.getOp();
            LogicalSetOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(limitOperator);
            processSetOperator(limitOperator, opBuilder);
            processCommon(opBuilder, limitOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalValues(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalValuesOperator valuesOperator = (LogicalValuesOperator) optExpression.getOp();
            LogicalValuesOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(valuesOperator);

            // column refs
            List<ColumnRefOperator> newColRefs = cloneColumnRefs(valuesOperator.getColumnRefSet());
            opBuilder.setColumnRefSet(newColRefs);
            processCommon(opBuilder, valuesOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalTableFunction(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) optExpression.getOp();
            LogicalTableFunctionOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(tableFunctionOperator);

            // fnResultColRef
            List<ColumnRefOperator> newFnResultColRefs = cloneColumnRefs(tableFunctionOperator.getFnResultColRefs());
            opBuilder.setFnResultColRefs(newFnResultColRefs);

            // outerColRef
            List<ColumnRefOperator> newOuterColRefs = cloneColumnRefs(tableFunctionOperator.getOuterColRefs());
            opBuilder.setOuterColRefs(newOuterColRefs);

            // fnParamColumnProject
            List<Pair<ColumnRefOperator, ScalarOperator>> newFnParamColumnProject =
                    tableFunctionOperator.getFnParamColumnProject()
                            .stream()
                            .map(pair -> Pair.create((ColumnRefOperator) pair.first.clone(), pair.second.clone()))
                            .collect(Collectors.toList());
            opBuilder.setFnParamColumnProject(newFnParamColumnProject);

            processCommon(opBuilder, tableFunctionOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalRepeat(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) optExpression.getOp();
            LogicalRepeatOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(repeatOperator);

            // fnResultColRef
            List<ColumnRefOperator> newOutputGroupings = cloneColumnRefs(repeatOperator.getOutputGrouping());
            opBuilder.setOutputGrouping(newOutputGroupings);

            // fnResultColRef
            List<List<ColumnRefOperator>> newRepeatColumnRefList = Lists.newArrayList();
            for (List<ColumnRefOperator> repeatColumnRefs : repeatOperator.getRepeatColumnRef()) {
                List<ColumnRefOperator> newRepeatColumnRefs = cloneColumnRefs(repeatColumnRefs);
                newRepeatColumnRefList.add(newRepeatColumnRefs);
            }
            opBuilder.setRepeatColumnRefList(newRepeatColumnRefList);

            processCommon(opBuilder, repeatOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalCTEAnchorOperator cteAnchorOperator = (LogicalCTEAnchorOperator) optExpression.getOp();
            LogicalCTEAnchorOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteAnchorOperator);
            opBuilder.setCteId(cteAnchorOperator.getCteId());
            processCommon(opBuilder, cteAnchorOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEProduce(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalCTEProduceOperator cteProduceOperator = (LogicalCTEProduceOperator) optExpression.getOp();
            LogicalCTEProduceOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteProduceOperator);
            opBuilder.setCteId(cteProduceOperator.getCteId());
            processCommon(opBuilder, cteProduceOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = processChildren(optExpression);
            LogicalCTEConsumeOperator cteConsumeOperator = (LogicalCTEConsumeOperator) optExpression.getOp();
            LogicalCTEConsumeOperator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(cteConsumeOperator);
            opBuilder.setCteId(cteConsumeOperator.getCteId());

            // cteOutputColumnRefMap
            Map<ColumnRefOperator, ColumnRefOperator> newCteOutputColumnRefMap =
                    cteConsumeOperator.getCteOutputColumnRefMap()
                            .entrySet()
                            .stream()
                            .map(entry -> Pair.create((ColumnRefOperator) entry.getKey().clone(),
                                    (ColumnRefOperator) entry.getValue().clone()))
                            .collect(Collectors.toMap(x -> x.first, x -> x.second));
            opBuilder.setCteOutputColumnRefMap(newCteOutputColumnRefMap);

            processCommon(opBuilder, cteConsumeOperator);
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return null;
        }

        private HashDistributionSpec processHashDistributionSpec(HashDistributionSpec originSpec) {
            return new HashDistributionSpec(originSpec.getHashDistributionDesc(), originSpec.getEquivDesc());
        }

        private List<ColumnRefOperator> cloneColumnRefs(List<ColumnRefOperator> columnRefList) {
            return columnRefList.stream()
                    .map(ColumnRefOperator::clone)
                    .map(ColumnRefOperator.class::cast)
                    .collect(Collectors.toList());
        }

        private void processCommon(Operator.Builder opBuilder, LogicalOperator op) {
            // first process predicate, then projection
            ScalarOperator predicate = op.getPredicate();
            if (predicate != null) {
                opBuilder.setPredicate(predicate.clone());
            }
            Projection projection = op.getProjection();
            if (projection != null) {
                Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap(projection.getColumnRefMap());
                Projection newProjection = new Projection(newColumnRefMap);
                opBuilder.setProjection(newProjection);
            }
        }
    }
}