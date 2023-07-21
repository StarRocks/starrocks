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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getMvPartialPartitionPredicates;

public class MvRewritePreprocessor {
    private static final Logger LOG = LogManager.getLogger(MvRewritePreprocessor.class);
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
        Set<Table> queryTables = MvUtils.getAllTables(logicOperatorTree).stream().collect(Collectors.toSet());
        // get all related materialized views, include nested mvs
        Set<MaterializedView> relatedMvs =
                MvUtils.getRelatedMvs(connectContext.getSessionVariable().getNestedMvRewriteMaxLevel(), queryTables);
        prepareRelatedMVs(queryTables, relatedMvs);
    }

    public void prepareSyncMvCandidatesForPlan() {
        Set<Table> queryTables = MvUtils.getAllTables(logicOperatorTree).stream().collect(Collectors.toSet());

        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        // get all related materialized views, include nested mvs
        for (Table table : queryTables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            for (MaterializedIndexMeta indexMeta : olapTable.getVisibleIndexMetas()) {
                long indexId = indexMeta.getIndexId();
                if (indexMeta.getIndexId() == olapTable.getBaseIndexId()) {
                    continue;
                }
                // Old sync mv may not contain the index define sql.
                if (Strings.isNullOrEmpty(indexMeta.getViewDefineSql())) {
                    continue;
                }

                // To avoid adding optimization times, only put the mv with complex expressions into materialized views.
                if (!MVUtils.containComplexExpresses(indexMeta)) {
                    continue;
                }

                try {
                    long dbId = indexMeta.getDbId();
                    String viewDefineSql = indexMeta.getViewDefineSql();
                    String mvName = olapTable.getIndexNameById(indexId);
                    Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

                    // distribution info
                    DistributionInfo baseTableDistributionInfo = olapTable.getDefaultDistributionInfo();
                    DistributionInfo mvDistributionInfo = baseTableDistributionInfo.copy();
                    Set<String> mvColumnNames =
                            indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toSet());
                    if (baseTableDistributionInfo.getType() == DistributionInfoType.HASH) {
                        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) baseTableDistributionInfo;
                        Set<String> distributedColumns =
                                hashDistributionInfo.getDistributionColumns().stream().map(Column::getName)
                                        .collect(Collectors.toSet());
                        // NOTE: SyncMV's column may not be equal to base table's exactly.
                        List<Column> newDistributionColumns = Lists.newArrayList();
                        for (Column mvColumn : indexMeta.getSchema()) {
                            if (distributedColumns.contains(mvColumn.getName())) {
                                newDistributionColumns.add(mvColumn);
                            }
                        }
                        // Set random distribution info if sync mv' columns may not contain distribution keys,
                        if (newDistributionColumns.size() != distributedColumns.size()) {
                            mvDistributionInfo = new RandomDistributionInfo();
                        } else {
                            ((HashDistributionInfo) mvDistributionInfo).setDistributionColumns(newDistributionColumns);
                        }
                    }
                    // partition info
                    PartitionInfo basePartitionInfo = olapTable.getPartitionInfo();
                    PartitionInfo mvPartitionInfo = basePartitionInfo;
                    // Set single partition if sync mv' columns do not contain partition by columns.
                    if (basePartitionInfo.isPartitioned()) {
                        if (basePartitionInfo.getPartitionColumns().stream()
                                .anyMatch(x -> !mvColumnNames.contains(x.getName())) ||
                                !(basePartitionInfo instanceof ExpressionRangePartitionInfo)) {
                            mvPartitionInfo = new SinglePartitionInfo();
                        }
                    }
                    // refresh schema
                    MaterializedView.MvRefreshScheme mvRefreshScheme =
                            new MaterializedView.MvRefreshScheme(MaterializedView.RefreshType.SYNC);
                    MaterializedView mv = new MaterializedView(db, mvName, indexMeta, olapTable,
                            mvPartitionInfo, mvDistributionInfo, mvRefreshScheme);
                    mv.setViewDefineSql(viewDefineSql);
                    mv.setBaseIndexId(indexId);
                    relatedMvs.add(mv);
                } catch (Exception e) {
                    LOG.warn("error happens when parsing create sync materialized view stmt [{}] use new parser",
                            indexId, e);
                }
            }
        }
        prepareRelatedMVs(queryTables, relatedMvs);
    }

    private void prepareRelatedMVs(Set<Table> queryTables, Set<MaterializedView> relatedMvs) {
        String queryExcludingMVNames = connectContext.getSessionVariable().getQueryExcludingMVNames();
        String queryIncludingMVNames = connectContext.getSessionVariable().getQueryincludingMVNames();
        if (!Strings.isNullOrEmpty(queryExcludingMVNames) || !Strings.isNullOrEmpty(queryIncludingMVNames)) {
            Set<String> queryExcludingMVNamesSet = Sets.newHashSet(queryExcludingMVNames.split(","));
            Set<String> queryIncludingMVNamesSet = Sets.newHashSet(queryIncludingMVNames.split(","));
            relatedMvs = relatedMvs.stream()
                    .filter(mv -> queryIncludingMVNamesSet.contains(mv.getName()))
                    .filter(mv -> !queryExcludingMVNamesSet.contains(mv.getName()))
                    .collect(Collectors.toSet());
        }
        if (relatedMvs.isEmpty()) {
            logMVPrepare(connectContext, "No Related MVs for the query plan");
            return;
        }

        Set<ColumnRefOperator> originQueryColumns = Sets.newHashSet(queryColumnRefFactory.getColumnRefs());
        for (MaterializedView mv : relatedMvs) {
            try {
                preprocessMv(mv, queryTables, originQueryColumns);
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                LOG.warn("preprocess mv {} failed for query tables:{}", mv.getName(), tableNames, e);
            }
        }
        if (relatedMvs.isEmpty()) {
            logMVPrepare(connectContext, "No Related MVs after process");
            return;
        }
        // all base table related mvs
        List<String> relatedMvNames = relatedMvs.stream().map(mv -> mv.getName()).collect(Collectors.toList());
        // all mvs that match SPJG pattern and can ben used to try mv rewrite
        List<String> candidateMvNames = context.getCandidateMvs().stream()
                .map(materializationContext -> materializationContext.getMv().getName()).collect(Collectors.toList());

        logMVPrepare(connectContext, "RelatedMVs: %s, CandidateMVs: %s", relatedMvNames, candidateMvNames);
    }

    private void preprocessMv(MaterializedView mv, Set<Table> queryTables, Set<ColumnRefOperator> originQueryColumns) {
        if (!mv.isActive()) {
            logMVPrepare(connectContext, mv, "MV is not active: %s", mv.getName());
            return;
        }

        MaterializedView.MVRewriteContextCache mvRewriteContextCache = mv.getPlanContext();
        if (mvRewriteContextCache == null) {
            // build mv query logical plan
            MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();
            // optimize the sql by rule and disable rule based materialized view rewrite
            OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
            optimizerConfig.disableRuleSet(RuleSetType.PARTITION_PRUNE);
            optimizerConfig.disableRuleSet(RuleSetType.SINGLE_TABLE_MV_REWRITE);
            optimizerConfig.disableRule(RuleType.TF_REWRITE_GROUP_BY_COUNT_DISTINCT);
            // For sync mv, no rewrite query by original sync mv rule to avoid useless rewrite.
            if (mv.getRefreshScheme().isSync()) {
                optimizerConfig.disableRule(RuleType.TF_MATERIALIZED_VIEW);
            }
            optimizerConfig.setMVRewritePlan(true);

            mvRewriteContextCache = mvOptimizer.optimize(mv, connectContext, optimizerConfig);
            mv.setPlanContext(mvRewriteContextCache);
        }
        if (!mvRewriteContextCache.isValidMvPlan()) {
            logMVPrepare(connectContext, mv, "MV plan is not valid: %s, plan:\n %s",
                    mv.getName(), mvRewriteContextCache.getLogicalPlan().explain());
            return;
        }

        Set<String> partitionNamesToRefresh = mv.getPartitionNamesToRefreshForMv();
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            if (!partitionNamesToRefresh.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(base.getTable()));
                    sb.append(String.format("base table %s version: %s; ", base, versionInfo));
                }
                LOG.info("[MV PREPARE] MV {} is outdated, stale partitions {}, detailed version info: {}",
                        mv.getName(), partitionNamesToRefresh, sb.toString());
                return;
            }
        } else if (!mv.getPartitionNames().isEmpty() && partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
            // if the mv is partitioned, and all partitions need refresh,
            // then it can not be a candidate

            StringBuilder sb = new StringBuilder();
            for (BaseTableInfo base : mv.getBaseTableInfos()) {
                String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(base.getTable()));
                sb.append(String.format("base table %s version: %s; ", base, versionInfo));
            }
            LOG.info("[MV PREPARE] MV {} is outdated and all its partitions need to be " +
                    "refreshed: {}, detailed info: {}", mv.getName(), partitionNamesToRefresh, sb.toString());
            return;
        }

        OptExpression mvPlan = mvRewriteContextCache.getLogicalPlan();
        ScalarOperator mvPartialPartitionPredicates = null;
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
            // when mv is partitioned and there are some refreshed partitions,
            // when should calculate the latest partition range predicates for partition-by base table
            mvPartialPartitionPredicates = getMvPartialPartitionPredicates(mv, mvPlan, partitionNamesToRefresh);
            if (mvPartialPartitionPredicates == null) {
                logMVPrepare(connectContext, mv, "Partitioned MV %s is outdated which contains some partitions " +
                        "to be refreshed:%s", mv.getName(), partitionNamesToRefresh);
                return;
            }
        }

        // Add mv info into dump info
        if (connectContext.getDumpInfo() != null) {
            String dbName = connectContext.getGlobalStateMgr().getDb(mv.getDbId()).getFullName();
            connectContext.getDumpInfo().addTable(dbName, mv);
        }

        List<Table> baseTables = MvUtils.getAllTables(mvPlan);
        List<Table> intersectingTables = baseTables.stream().filter(queryTables::contains).collect(Collectors.toList());
        MaterializationContext materializationContext =
                new MaterializationContext(context, mv, mvPlan, queryColumnRefFactory,
                        mv.getPlanContext().getRefFactory(), partitionNamesToRefresh,
                        baseTables, originQueryColumns, intersectingTables, mvPartialPartitionPredicates);
        List<ColumnRefOperator> mvOutputColumns = mv.getPlanContext().getOutputColumns();
        // generate scan mv plan here to reuse it in rule applications
        LogicalOlapScanOperator scanMvOp = createScanMvOperator(materializationContext, partitionNamesToRefresh);
        materializationContext.setScanMvOperator(scanMvOp);
        // should keep the sequence of schema
        List<ColumnRefOperator> scanMvOutputColumns = Lists.newArrayList();
        for (Column column : mv.getBaseSchema()) {
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
        logMVPrepare(connectContext, mv, "Prepare MV %s success", mv.getName());
    }

    /**
     * Make a LogicalOlapScanOperator by using MV's schema which includes:
     * - partition infos.
     * - distribution infos.
     * - original MV's predicates which can be deduced from MV opt expression and be used
     * for partition/distribution pruning.
     */
    private LogicalOlapScanOperator createScanMvOperator(MaterializationContext mvContext,
                                                         Set<String> excludedPartitions) {
        final MaterializedView mv = mvContext.getMv();

        final ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();

        final ColumnRefFactory columnRefFactory = mvContext.getQueryRefFactory();
        int relationId = columnRefFactory.getNextRelationId();

        // first add base schema to avoid replaced in full schema.
        Set<String> columnNames = Sets.newHashSet();
        for (Column column : mv.getBaseSchema()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
            columnNames.add(column.getName());
        }
        for (Column column : mv.getFullSchema()) {
            if (columnNames.contains(column.getName())) {
                continue;
            }
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
        }
        final Map<Column, ColumnRefOperator> columnMetaToColRefMap = columnMetaToColRefMapBuilder.build();

        // construct partition
        List<Long> selectPartitionIds = Lists.newArrayList();
        List<Long> selectTabletIds = Lists.newArrayList();
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

        return LogicalOlapScanOperator.builder()
                .setTable(mv)
                .setColRefToColumnMetaMap(colRefToColumnMetaMapBuilder.build())
                .setColumnMetaToColRefMap(columnMetaToColRefMap)
                .setDistributionSpec(getTableDistributionSpec(mvContext, columnMetaToColRefMap))
                .setSelectedIndexId(mv.getBaseIndexId())
                .setSelectedPartitionId(selectPartitionIds)
                .setPartitionNames(partitionNames)
                .setSelectedTabletId(selectTabletIds)
                .setHintsTabletIds(Collections.emptyList())
                .setHasTableHints(false)
                .setUsePkIndex(false)
                .build();
    }

    private DistributionSpec getTableDistributionSpec(
            MaterializationContext mvContext, Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        final MaterializedView mv = mvContext.getMv();

        DistributionSpec distributionSpec = null;
        // construct distribution
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        if (distributionInfo.getType() == DistributionInfoType.HASH) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
            List<Integer> hashDistributeColumns = new ArrayList<>();
            for (Column distributedColumn : distributedColumns) {
                hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
            }
            final HashDistributionDesc hashDistributionDesc =
                    new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);
            distributionSpec = DistributionSpec.createHashDistributionSpec(hashDistributionDesc);
        } else if (distributionInfo.getType() == DistributionInfoType.RANDOM) {
            distributionSpec = DistributionSpec.createAnyDistributionSpec();
        }

        return distributionSpec;
    }

}
