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
        PartitionNames partitionNames = new PartitionNames(false, selectedPartitionNames);
        return new LogicalOlapScanOperator(mv,
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
    }
}
