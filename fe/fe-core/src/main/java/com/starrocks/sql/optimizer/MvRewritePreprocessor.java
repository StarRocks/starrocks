// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
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
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        List<Table> tables = MvUtils.getAllTables(logicOperatorTree);

        // get all related materialized views, include nested mvs
        Set<MaterializedView> relatedMvs =
                MvUtils.getRelatedMvs(connectContext.getSessionVariable().getNestedMvRewriteMaxLevel(), tables);

        for (MaterializedView mv : relatedMvs) {
            if (!mv.isActive()) {
                continue;
            }
            Set<String> partitionNamesToRefresh = mv.getPartitionNamesToRefreshForMv();
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                if (!partitionNamesToRefresh.isEmpty()) {
                    continue;
                }
            } else if (partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
                // if the mv is partitioned, and all partitions need refresh,
                // then it can not be an candidate
                continue;
            }

            // 1. build mv query logical plan
            ColumnRefFactory mvColumnRefFactory = new ColumnRefFactory();
            MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();
            OptExpression mvPlan = mvOptimizer.optimize(mv, mvColumnRefFactory, connectContext, partitionNamesToRefresh);
            if (!MvUtils.isValidMVPlan(mvPlan)) {
                continue;
            }

            List<ColumnRefOperator> mvOutputColumns = mvOptimizer.getOutputExpressions();
            MaterializationContext materializationContext =
                    new MaterializationContext(mv, mvPlan, queryColumnRefFactory, mvColumnRefFactory, partitionNamesToRefresh);
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

    private LogicalOlapScanOperator createScanMvOperator(MaterializationContext materializationContext) {
        MaterializedView mv = materializationContext.getMv();

        ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();
        ImmutableList.Builder<ColumnRefOperator> outputVariablesBuilder = ImmutableList.builder();

        ColumnRefFactory columnRefFactory = materializationContext.getQueryRefFactory();
        int relationId = columnRefFactory.getNextRelationId();
        for (Column column : mv.getFullSchema()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            outputVariablesBuilder.add(columnRef);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
        }

        Map<Column, ColumnRefOperator> columnMetaToColRefMap = columnMetaToColRefMapBuilder.build();
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo instanceof HashDistributionInfo);
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
        List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
        List<Integer> hashDistributeColumns = new ArrayList<>();
        for (Column distributedColumn : distributedColumns) {
            hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
        }

        HashDistributionDesc hashDistributionDesc =
                new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);

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
<<<<<<< HEAD
        PartitionNames partitionNames = new PartitionNames(false, selectedPartitionNames);
        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(mv,
=======
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
>>>>>>> a52bb9384 ([BugFix] Fix rewrite nested mv bugs when mv's partition/distribution predicates is not null (#18375))
                colRefToColumnMetaMapBuilder.build(),
                columnMetaToColRefMap,
                DistributionSpec.createHashDistributionSpec(hashDistributionDesc),
                Operator.DEFAULT_LIMIT,
                null,
                mv.getBaseIndexId(),
                selectPartitionIds,
                partitionNames,
                selectTabletIds,
                Lists.newArrayList());
        return scanOperator;
    }
}
