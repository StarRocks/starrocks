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
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
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
    private final Map<ColumnRefOperator, ScalarOperator> columnMapping;
    private final ReplaceColumnRefRewriter rewriter;
    private final Table partitionByTable;
    private final Column partitionColumn;
    private final boolean partialPartitionRewrite;

    public OptExpressionDuplicator(MaterializationContext materializationContext) {
        this.columnRefFactory = materializationContext.getQueryRefFactory();
        this.columnMapping = Maps.newHashMap();
        this.rewriter = new ReplaceColumnRefRewriter(columnMapping);
        Pair<Table, Column> partitionInfo = materializationContext.getMv().getBaseTableAndPartitionColumn();
        this.partitionByTable = partitionInfo == null ? null : partitionInfo.first;
        this.partitionColumn = partitionInfo == null ? null : partitionInfo.second;
        this.partialPartitionRewrite = !materializationContext.getMvPartitionNamesToRefresh().isEmpty();
    }

    public OptExpression duplicate(OptExpression source) {
        OptExpressionDuplicatorVisitor visitor = new OptExpressionDuplicatorVisitor();
        return source.getOp().accept(visitor, source, null);
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnMapping() {
        return columnMapping;
    }

    public List<ColumnRefOperator> getMappedColumns(List<ColumnRefOperator> originColumns) {
        List<ColumnRefOperator> newColumnRefs = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originColumns) {
            newColumnRefs.add((ColumnRefOperator) columnMapping.get(columnRef));
        }
        return newColumnRefs;
    }

    class OptExpressionDuplicatorVisitor extends OptExpressionVisitor<OptExpression, Void> {
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
                ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(key,
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
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
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
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            List<ColumnRefOperator> newGroupKeys = Lists.newArrayList();
            for (ColumnRefOperator groupKey : aggregationOperator.getGroupingKeys()) {
                ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(groupKey,
                        k -> columnRefFactory.create(k, k.getType(), k.isNullable()));
                newGroupKeys.add(mapped);
            }
            LogicalAggregationOperator.Builder aggregationBuilder = (LogicalAggregationOperator.Builder) opBuilder;
            aggregationBuilder.setGroupingKeys(newGroupKeys);
            Map<ColumnRefOperator, CallOperator> newAggregates = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(entry.getKey(),
                        k -> columnRefFactory.create(k, k.getType(), k.isNullable()));
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                Preconditions.checkState(newValue instanceof CallOperator);
                newAggregates.put(mapped, (CallOperator) newValue);
            }
            aggregationBuilder.setAggregations(newAggregates);

            List<ColumnRefOperator> newPartitionColumns = Lists.newArrayList();
            List<ColumnRefOperator> partitionColumns = aggregationOperator.getPartitionByColumns();
            if (partitionColumns != null) {
                for (ColumnRefOperator columnRef : partitionColumns) {
                    ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(columnRef,
                            k -> columnRefFactory.create(k, k.getType(), k.isNullable()));
                    newPartitionColumns.add(mapped);
                }
            }
            aggregationBuilder.setPartitionByColumns(newPartitionColumns);
            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
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
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
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

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return null;
        }

        // rewrite shuffle columns and joinEquivalentColumns in HashDistributionSpec
        // because the columns ids have changed
        private HashDistributionSpec processHashDistributionSpec(
                HashDistributionSpec originSpec,
                ColumnRefFactory columnRefFactory,
                Map<ColumnRefOperator, ScalarOperator> columnMapping) {

            // HashDistributionDesc
            List<DistributionCol> newColumns = Lists.newArrayList();
            for (DistributionCol column : originSpec.getShuffleColumns()) {
                ColumnRefOperator oldRefOperator = columnRefFactory.getColumnRef(column.getColId());
                ColumnRefOperator newRefOperator = columnMapping.get(oldRefOperator).cast();
                Preconditions.checkNotNull(newRefOperator);
                newColumns.add(new DistributionCol(newRefOperator.getId(), column.isNullStrict()));
            }
            Preconditions.checkState(newColumns.size() == originSpec.getShuffleColumns().size());
            HashDistributionDesc hashDistributionDesc =
                    new HashDistributionDesc(newColumns, originSpec.getHashDistributionDesc().getSourceType());

            EquivalentDescriptor equivDesc = originSpec.getEquivDesc();

            EquivalentDescriptor newEquivDesc = new EquivalentDescriptor(equivDesc.getTableId(), equivDesc.getPartitionIds());
            updateDistributionUnionFind(equivDesc.getNullStrictUnionFind());
            updateDistributionUnionFind(equivDesc.getNullRelaxUnionFind());

            return new HashDistributionSpec(hashDistributionDesc, newEquivDesc);
        }

        public void updateDistributionUnionFind(UnionFind<DistributionCol> unionFind) {
            UnionFind<DistributionCol> tmp = unionFind.copy();
            unionFind.clear();
            for (Set<DistributionCol> distributionColSet : tmp.getAllGroups()) {
                DistributionCol first = null;
                for (DistributionCol next : distributionColSet) {
                    if (first == null) {
                        first = next;
                    }
                    ColumnRefOperator firstCol = columnRefFactory.getColumnRef(first.getColId());
                    ColumnRefOperator newFirstCol = columnMapping.get(firstCol).cast();

                    ColumnRefOperator nextCol = columnRefFactory.getColumnRef(next.getColId());
                    ColumnRefOperator newNextCol = columnMapping.get(nextCol).cast();
                    unionFind.union(first.updateColId(newFirstCol.getId()), next.updateColId(newNextCol.getId()));
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
                newColumnRefMap.put((ColumnRefOperator) columnMapping.get(column), newValue);
            }
            return newColumnRefMap;
        }
    }
}
