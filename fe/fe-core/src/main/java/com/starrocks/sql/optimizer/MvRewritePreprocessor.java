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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getMvPartialPartitionPredicates;

public class MvRewritePreprocessor {
    private final ConnectContext connectContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final OptimizerContext context;
    private final OptExpression logicOperatorTree;

    public MvRewritePreprocessor(ConnectContext connectContext,
                                 ColumnRefFactory queryColumnRefFactory,
                                 OptimizerContext context,
                                 OptExpression logicOperatorTree) {
        this.connectContext = connectContext;
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.context = context;
        this.logicOperatorTree = logicOperatorTree;
    }

    public void prepareMvCandidatesForPlan() {
        List<Table> queryTables = MvUtils.getAllTables(logicOperatorTree);

        // get all related materialized views, include nested mvs
        Set<MaterializedView> relatedMvs =
                MvUtils.getRelatedMvs(connectContext.getSessionVariable().getNestedMvRewriteMaxLevel(), queryTables);

        Set<ColumnRefOperator> originQueryColumns = Sets.newHashSet(queryColumnRefFactory.getColumnRefs());
        for (MaterializedView mv : relatedMvs) {
            if (!mv.isActive()) {
                continue;
            }

            MaterializedView.MvRewriteContext mvRewriteContext = mv.getPlanContext();
            if (mvRewriteContext == null) {
                // build mv query logical plan
                MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();
                mvRewriteContext = mvOptimizer.optimize(mv, connectContext);
                mv.setPlanContext(mvRewriteContext);
            }
            if (!mvRewriteContext.isValidMvPlan()) {
                continue;
            }

            Set<String> partitionNamesToRefresh = mv.getPartitionNamesToRefreshForMv();
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                if (!partitionNamesToRefresh.isEmpty()) {
                    continue;
                }
            } else if (!mv.getPartitionNames().isEmpty() &&
                    partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
                // if the mv is partitioned, and all partitions need refresh,
                // then it can not be an candidate
                continue;
            }

            OptExpression mvPlan = mvRewriteContext.getLogicalPlan();
            ScalarOperator mvPartialPartitionPredicates = null;
            if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
                // when mv is partitioned and there are some refreshed partitions,
                // when should calculate latest partition range predicates for partition-by base table
                mvPartialPartitionPredicates = getMvPartialPartitionPredicates(mv, mvPlan, partitionNamesToRefresh);
                if (mvPartialPartitionPredicates == null) {
                    continue;
                }
            }

            List<Table> baseTables = MvUtils.getAllTables(mvPlan);
            List<Table> intersectingTables = baseTables.stream().filter(queryTables::contains).collect(Collectors.toList());
            MaterializationContext materializationContext =
                    new MaterializationContext(context, mv, mvPlan, queryColumnRefFactory,
                            mv.getPlanContext().getRefFactory(), partitionNamesToRefresh,
                            baseTables, originQueryColumns, intersectingTables, mvPartialPartitionPredicates);
            List<ColumnRefOperator> mvOutputColumns = mv.getPlanContext().getOutputColumns();
            // generate scan mv plan here to reuse it in rule applications
            LogicalOlapScanOperator scanMvOp = createScanMvOperator(materializationContext);
            materializationContext.setScanMvOperator(scanMvOp);
            String dbName = connectContext.getGlobalStateMgr().getDb(mv.getDbId()).getFullName();
            connectContext.getDumpInfo().addTable(dbName, mv);
            // should keep the sequence of schema
            List<ColumnRefOperator> scanMvOutputColumns = Lists.newArrayList();
            for (Column column : mv.getFullSchema()) {
                scanMvOutputColumns.add(scanMvOp.getColumnReference(column));
            }
            Preconditions.checkState(mvOutputColumns.size() == scanMvOutputColumns.size());

            // construct output column mapping from mv sql to mv scan operator
            // eg: for mv1 sql define: select a, (b + 1) as c2, (a * b) as c3 from table;
            // select sql plan output columns:    a, b + 1, a * b
            //                                    |    |      |
            //                                    v    v      V
            // mv scan operator output columns:  a,   c2,    c3
            Map<ColumnRefOperator, ColumnRefOperator> outputMapping = Maps.newHashMap();
            for (int i = 0; i < mvOutputColumns.size(); i++) {
                outputMapping.put(mvOutputColumns.get(i), scanMvOutputColumns.get(i));
            }
            materializationContext.setOutputMapping(outputMapping);
            context.addCandidateMvs(materializationContext);
        }
    }

    /**
     * Make a LogicalOlapScanOperator by using MV's schema which includes:
     *  - partition infos.
     *  - distribution infos.
     *  - original MV's predicates which can be deduced from MV opt expression and be used
     *       for partition/distribution pruning.
     */
    private LogicalOlapScanOperator createScanMvOperator(MaterializationContext mvContext) {
        final MaterializedView mv = mvContext.getMv();

        final ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();

        final ColumnRefFactory columnRefFactory = mvContext.getQueryRefFactory();
        int relationId = columnRefFactory.getNextRelationId();
        for (Column column : mv.getFullSchema()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
        }
        final Map<Column, ColumnRefOperator> columnMetaToColRefMap = columnMetaToColRefMapBuilder.build();

        // construct distribution
        final Set<Integer>  mvPartitionDistributionColumnRef = Sets.newHashSet();
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        // only hash distribution is supported
        Preconditions.checkState(distributionInfo instanceof HashDistributionInfo);
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
        List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
        List<Integer> hashDistributeColumns = new ArrayList<>();
        for (Column distributedColumn : distributedColumns) {
            hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
        }
        final HashDistributionDesc hashDistributionDesc =
                new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);

        // construct partition
        List<Long> selectPartitionIds = Lists.newArrayList();
        List<Long> selectTabletIds = Lists.newArrayList();
        Set<String> excludedPartitions = mv.getPartitionNamesToRefreshForMv();
        List<String> selectedPartitionNames = Lists.newArrayList();
        for (Partition p : mv.getPartitions()) {
            if (!excludedPartitions.contains(p.getName()) && p.hasData()) {
                selectPartitionIds.add(p.getId());
                selectedPartitionNames.add(p.getName());
                MaterializedIndex materializedIndex = p.getIndex(mv.getBaseIndexId());
                selectTabletIds.addAll(materializedIndex.getTabletIdsInOrder());
            }
        }
        final PartitionNames partitionNames = new PartitionNames(false, selectedPartitionNames);

        // NOTE:
        // - To partition/distribution prune, need filter predicates that belong to MV.
        // - Those predicates are only used for partition/distribution pruning and don't affect the real
        // query compute.
        // - after partition/distribution pruning, those predicates should be removed from mv rewrite result.
        final OptExpression mvExpression = mvContext.getMvExpression();
        final List<ScalarOperator> conjuncts = MvUtils.getAllPredicates(mvExpression);
        final ColumnRefSet mvOutputColumnRefSet = mvExpression.getOutputColumns();
        final List<ScalarOperator> mvConjuncts = Lists.newArrayList();

        // Construct partition/distribution key column refs to filter conjunctions which need to retain.
        Set<String> mvPruneKeyColNames = Sets.newHashSet();
        distributedColumns.stream().forEach(distKey -> mvPruneKeyColNames.add(distKey.getName()));
        mv.getPartitionNames().stream().forEach(partName -> mvPruneKeyColNames.add(partName));
        final Set<Integer> mvPruneColumnIdSet = mvOutputColumnRefSet.getStream().map(
                        id -> mvContext.getMvColumnRefFactory().getColumnRef(id))
                .filter(colRef -> mvPruneKeyColNames.contains(colRef.getName()))
                .map(colRef -> colRef.getId())
                .collect(Collectors.toSet());
        // Case1: keeps original predicates which belong to MV table(which are not pruned after mv's partition pruning)
        for (ScalarOperator conj : conjuncts) {
            // ignore binary predicates which cannot be used for pruning.
            if (conj instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator conjOp = (BinaryPredicateOperator) conj;
                if (conjOp.getChild(0).isColumnRef() && conjOp.getChild(1).isColumnRef()) {
                    continue;
                }
            }
            final List<Integer> conjColumnRefOperators =
                    Utils.extractColumnRef(conj).stream().map(ref -> ref.getId()).collect(Collectors.toList());
            if (mvPruneColumnIdSet.containsAll(conjColumnRefOperators)) {
                mvConjuncts.add(conj);
            }
        }
        // Case2: compensated partition predicates which are pruned after mv's partition pruning.
        // Compensate partition predicates and add them into mv predicate.
        final ScalarOperator mvPartitionPredicate =
                MvUtils.compensatePartitionPredicate(mvExpression, mvContext.getMvColumnRefFactory());
        if (!ConstantOperator.TRUE.equals(mvPartitionPredicate)) {
            mvConjuncts.add(mvPartitionPredicate);
        }

        return new LogicalOlapScanOperator(mv,
                colRefToColumnMetaMapBuilder.build(),
                columnMetaToColRefMap,
                DistributionSpec.createHashDistributionSpec(hashDistributionDesc),
                Operator.DEFAULT_LIMIT,
                Utils.compoundAnd(mvConjuncts),
                mv.getBaseIndexId(),
                selectPartitionIds,
                partitionNames,
                selectTabletIds,
                Lists.newArrayList());
    }
}
