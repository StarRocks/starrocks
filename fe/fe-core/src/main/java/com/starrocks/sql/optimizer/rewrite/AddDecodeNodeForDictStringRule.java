// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * For a low cardinality string column with global dict, we will rewrite the plan to
 * speed up query with global dict.
 *
 * 1. Check the olap scan nodes have low cardinality global dict string column
 * 2. Replace the string column with the dict encoded int column
 * 3. Bottom up traverse the plan tree, if the operator could apply global dict, then
 *    Replace the string column with the dict encoded int column,
 *    else insert the decode operator into the tree
 * 4. The decode operator will translate the encoded int column to string column
 *
 * The concrete example could refer to testDecodeNodeRewriteXXX in PlanFragmentTest
 */
public class AddDecodeNodeForDictStringRule implements PhysicalOperatorTreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(AddDecodeNodeForDictStringRule.class);

    private final Map<Long, List<Integer>> tableIdToStringColumnIds = Maps.newHashMap();

    static class DecodeContext {
        Map<Long, List<Integer>> tableIdToStringColumnIds;
        ColumnRefFactory columnRefFactory;
        Map<Integer, Integer> stringColumnIdToDictColumnIds;
        boolean hasChanged = false;

        public DecodeContext(Map<Long, List<Integer>> tableIdToStringColumnIds, ColumnRefFactory columnRefFactory) {
            this.tableIdToStringColumnIds = tableIdToStringColumnIds;
            this.columnRefFactory = columnRefFactory;
            stringColumnIdToDictColumnIds = Maps.newHashMap();
        }
    }

    private static class DecodeVisitor extends OptExpressionVisitor<OptExpression, DecodeContext> {
        @Override
        public OptExpression visit(OptExpression optExpression, DecodeContext context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                context.hasChanged = false;
                OptExpression childExpr = optExpression.inputAt(i);
                OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
                if (context.hasChanged) {
                    insertDecodeExpr(optExpression, Collections.singletonList(newChildExpr), i, context);
                }
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDecode(OptExpression optExpression, DecodeContext context) {
            context.hasChanged = false;
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, DecodeContext context) {
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            long tableId = scanOperator.getTable().getId();
            if (context.tableIdToStringColumnIds.containsKey(scanOperator.getTable().getId())) {
                Map<ColumnRefOperator, Column> newColRefToColumnMetaMap =
                        Maps.newHashMap(scanOperator.getColRefToColumnMetaMap());

                List<ColumnRefOperator> newOutputColumns =
                        Lists.newArrayList(scanOperator.getOutputColumns());

                for (Integer columnId : context.tableIdToStringColumnIds.get(tableId)) {
                    ColumnRefOperator stringColumn = context.columnRefFactory.getColumnRef(columnId);
                    if (!newOutputColumns.contains(stringColumn)) {
                        continue;
                    }

                    // TODO(kks): support this later
                    if (scanOperator.getPredicate() != null &&
                            scanOperator.getPredicate().getUsedColumns().contains(columnId)) {
                        continue;
                    }

                    ColumnRefOperator newDictColumn = context.columnRefFactory.create(
                            stringColumn.getName(), Type.INT, stringColumn.isNullable());

                    newOutputColumns.remove(stringColumn);
                    newOutputColumns.add(newDictColumn);

                    Column oldColumn = scanOperator.getColRefToColumnMetaMap().get(stringColumn);
                    Column newColumn = new Column(oldColumn.getName(), Type.INT);

                    newColRefToColumnMetaMap.remove(stringColumn);
                    newColRefToColumnMetaMap.put(newDictColumn, newColumn);

                    ColumnDict dict = IDictManager.getInstance().getGlobalDict(tableId, stringColumn.getName());
                    scanOperator.addGlobalDictColumns(new Pair<>(newDictColumn.getId(), dict));

                    context.stringColumnIdToDictColumnIds.put(columnId, newDictColumn.getId());
                    context.hasChanged = true;
                }

                if (context.hasChanged) {
                    PhysicalOlapScanOperator newOlapScan = new PhysicalOlapScanOperator(
                            scanOperator.getTable(),
                            newOutputColumns,
                            newColRefToColumnMetaMap,
                            scanOperator.getDistributionSpec(),
                            scanOperator.getLimit(),
                            scanOperator.getPredicate(),
                            scanOperator.getSelectedIndexId(),
                            scanOperator.getSelectedPartitionId(),
                            scanOperator.getSelectedTabletId());
                    newOlapScan.setPreAggregation(scanOperator.isPreAggregation());

                    OptExpression result =  new OptExpression(newOlapScan);
                    result.setLogicalProperty(optExpression.getLogicalProperty());
                    result.setStatistics(optExpression.getStatistics());
                    return result;
                }
            }
            return optExpression;
        }

        private PhysicalProjectOperator rewriteProjectOperator(PhysicalProjectOperator projectOperator,
                                                               DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(projectOperator.getColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
                if (kv.getValue() instanceof ColumnRefOperator) {
                    ColumnRefOperator stringColumn = (ColumnRefOperator) kv.getValue();
                    if (context.stringColumnIdToDictColumnIds.containsKey(stringColumn.getId())) {
                        Integer columnId = context.stringColumnIdToDictColumnIds.get(stringColumn.getId());
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                        newProjectMap.put(dictColumn, dictColumn);
                        newProjectMap.remove(stringColumn);

                        newStringToDicts.put(stringColumn.getId(), dictColumn.getId());
                    }
                }
            }

            Map<ColumnRefOperator, ScalarOperator> newCommonProjectMap =
                    Maps.newHashMap(projectOperator.getCommonSubOperatorMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getCommonSubOperatorMap()
                    .entrySet()) {
                if (kv.getValue() instanceof ColumnRefOperator) {
                    ColumnRefOperator stringColumn = (ColumnRefOperator) kv.getValue();
                    if (context.stringColumnIdToDictColumnIds.containsKey(stringColumn.getId())) {
                        Integer columnId = context.stringColumnIdToDictColumnIds.get(stringColumn.getId());
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                        newCommonProjectMap.put(dictColumn, dictColumn);
                        newCommonProjectMap.remove(stringColumn);

                        newStringToDicts.put(stringColumn.getId(), dictColumn.getId());
                    }
                }
            }
            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasChanged = false;
            }
            return new PhysicalProjectOperator(newProjectMap, newCommonProjectMap);
        }

        private PhysicalDistributionOperator rewriteDistribution(PhysicalDistributionOperator exchangeOperator,
                                                                 DecodeContext context) {
            HashDistributionSpec hashDistributionSpec = (HashDistributionSpec) exchangeOperator.getDistributionSpec();

            List<Integer> shuffledColumns = Lists.newArrayList();
            for (Integer columnId : hashDistributionSpec.getHashDistributionDesc().getColumns()) {
                if (context.stringColumnIdToDictColumnIds.containsKey(columnId)) {
                    Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnId);
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                    shuffledColumns.add(dictColumn.getId());

                    ColumnRefOperator stringColumn = context.columnRefFactory.getColumnRef(columnId);
                    for (Map.Entry<Long, List<Integer>> kv : context.tableIdToStringColumnIds.entrySet()) {
                        if (kv.getValue().contains(columnId)) {
                            ColumnDict dict = IDictManager.getInstance().getGlobalDict(kv.getKey(),
                                    stringColumn.getName());
                            exchangeOperator.addGlobalDictColumns(new Pair<>(dictColumn.getId(), dict));
                        }
                    }
                } else {
                    shuffledColumns.add(columnId);
                }
            }
            exchangeOperator.setDistributionSpec(
                    new HashDistributionSpec(new HashDistributionDesc(shuffledColumns,
                            hashDistributionSpec.getHashDistributionDesc().getSourceType())));
            return exchangeOperator;
        }

        private PhysicalHashAggregateOperator rewriteAggOperator(PhysicalHashAggregateOperator aggOperator,
                                                                 DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap(aggOperator.getAggregations());
            for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggOperator.getAggregations().entrySet()) {
                if (kv.getValue().getFnName().equals(FunctionSet.COUNT) && !kv.getValue().getChildren().isEmpty()) {
                    int columnId = kv.getValue().getUsedColumns().getFirstId();
                    if (context.stringColumnIdToDictColumnIds.containsKey(columnId)) {
                        Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnId);
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                        CallOperator newCall = new CallOperator(kv.getValue().getFnName(), kv.getValue().getType(),
                                Collections.singletonList(dictColumn));
                        newAggMap.put(kv.getKey(), newCall);
                    }
                }
            }

            List<ColumnRefOperator> newGroupBys = Lists.newArrayList();
            for (ColumnRefOperator groupBy : aggOperator.getGroupBys()) {
                if (context.stringColumnIdToDictColumnIds.containsKey(groupBy.getId())) {
                    Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(groupBy.getId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                    newGroupBys.add(dictColumn);

                    newStringToDicts.put(groupBy.getId(), dictColumn.getId());
                } else {
                    newGroupBys.add(groupBy);
                }
            }

            List<ColumnRefOperator> newPartitionsBy = Lists.newArrayList();
            for (ColumnRefOperator groupBy : aggOperator.getPartitionByColumns()) {
                if (context.stringColumnIdToDictColumnIds.containsKey(groupBy.getId())) {
                    Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(groupBy.getId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);

                    newPartitionsBy.add(dictColumn);
                } else {
                    newPartitionsBy.add(groupBy);
                }
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            return new PhysicalHashAggregateOperator(aggOperator.getType(),
                    newGroupBys,
                    newPartitionsBy, newAggMap,
                    aggOperator.getSingleDistinctFunctionPos(),
                    aggOperator.isSplit(),
                    aggOperator.getLimit(),
                    aggOperator.getPredicate());
        }

        @Override
        public OptExpression visitPhysicalProject(OptExpression projectExpr, DecodeContext context) {
            OptExpression childExpr = projectExpr.inputAt(0);
            context.hasChanged = false;
            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            if (context.hasChanged) {
                PhysicalProjectOperator projectOperator = (PhysicalProjectOperator) projectExpr.getOp();
                if (projectOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    PhysicalProjectOperator newProjectOper = rewriteProjectOperator(projectOperator,
                            context);
                    OptExpression result = OptExpression.create(newProjectOper, newChildExpr);
                    result.setStatistics(projectExpr.getStatistics());
                    result.setLogicalProperty(projectExpr.getLogicalProperty());
                    return result;
                } else {
                    insertDecodeExpr(projectExpr, Collections.singletonList(newChildExpr), 0, context);
                }
            }
            return projectExpr;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression aggExpr, DecodeContext context) {
            OptExpression childExpr = aggExpr.inputAt(0);
            context.hasChanged = false;
            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            if (context.hasChanged) {
                PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) aggExpr.getOp();
                if (aggOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    PhysicalHashAggregateOperator newAggOper = rewriteAggOperator(aggOperator,
                            context);
                    OptExpression result = OptExpression.create(newAggOper, newChildExpr);
                    result.setStatistics(aggExpr.getStatistics());
                    result.setLogicalProperty(aggExpr.getLogicalProperty());
                    return result;
                } else {
                    insertDecodeExpr(aggExpr, Collections.singletonList(newChildExpr), 0, context);
                }
            }

            return aggExpr;
        }

        @Override
        public OptExpression visitPhysicalDistribution(OptExpression exchangeExpr, DecodeContext context) {
            OptExpression childExpr = exchangeExpr.inputAt(0);
            context.hasChanged = false;
            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            if (context.hasChanged) {
                PhysicalDistributionOperator exchangeOperator = (PhysicalDistributionOperator) exchangeExpr.getOp();
                if (!(exchangeOperator.getDistributionSpec() instanceof HashDistributionSpec)) {
                    return exchangeExpr;
                }
                if (exchangeOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    PhysicalDistributionOperator newExchangeOper = rewriteDistribution(exchangeOperator,
                            context);

                    OptExpression result = OptExpression.create(newExchangeOper, newChildExpr);
                    result.setStatistics(exchangeExpr.getStatistics());
                    result.setLogicalProperty(exchangeExpr.getLogicalProperty());
                    return result;
                } else {
                    insertDecodeExpr(exchangeExpr, Collections.singletonList(newChildExpr), 0, context);
                }
            }
            return exchangeExpr;
        }
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!ConnectContext.get().getSessionVariable().isEnableLowCardinalityOptimize()) {
            return root;
        }
        List<LogicalOlapScanOperator> scanOperators = taskContext.getAllScanOperators();

        for (LogicalOlapScanOperator scanOperator : scanOperators) {
            OlapTable table = (OlapTable) scanOperator.getTable();
            long version = table.getPartitions().stream().map(Partition::getVisibleVersion)
                    .max(Long::compareTo).orElse(0L);
            List<ColumnRefOperator> outputColumns = scanOperator.getOutputColumns();
            for (ColumnRefOperator column : outputColumns) {
                // Condition 1:
                if (!column.getType().isVarchar()) {
                    continue;
                }

                ColumnStatistic columnStatistic = Catalog.getCurrentStatisticStorage().
                        getColumnStatistic(table, column.getName());
                // Condition 2: the varchar column is low cardinality string column
                if (!FeConstants.USE_MOCK_DICT_MANAGER && (columnStatistic.isUnknown() ||
                        columnStatistic.getDistinctValuesCount() > CacheDictManager.LOW_CARDINALITY_THRESHOLD)) {
                    LOG.debug("{} isn't low cardinality string column", column.getName());
                    continue;
                }

                // Condition 3: the varchar column has collected global dict
                if (IDictManager.getInstance().hasGlobalDict(table.getId(), column.getName(), version)) {
                    if (!tableIdToStringColumnIds.containsKey(table.getId())) {
                        List<Integer> integers = Lists.newArrayList();
                        integers.add(column.getId());
                        tableIdToStringColumnIds.put(table.getId(), integers);
                    } else {
                        tableIdToStringColumnIds.get(table.getId()).add(column.getId());
                    }
                } else {
                    LOG.debug("{} doesn't have global dict", column.getName());
                }
            }
        }

        if (tableIdToStringColumnIds.isEmpty()) {
            return root;
        }

        DecodeContext context = new DecodeContext(tableIdToStringColumnIds, taskContext.getOptimizerContext().
                getColumnRefFactory());

        OptExpression rewriteExpr = root.getOp().accept(new DecodeVisitor(), root, context);
        if (context.hasChanged) {
            return generateDecodeOExpr(context, Collections.singletonList(rewriteExpr));
        }
        return rewriteExpr;
    }

    public static void insertDecodeExpr(OptExpression parentExpr, List<OptExpression> childExpr,
                                        int index,
                                        DecodeContext context) {
        OptExpression decodeExp = generateDecodeOExpr(context, childExpr);
        parentExpr.setChild(index, decodeExp);

        context.hasChanged = false;
        context.stringColumnIdToDictColumnIds.clear();
    }

    private static OptExpression generateDecodeOExpr(DecodeContext context, List<OptExpression> childExpr) {
        ImmutableMap.Builder<Integer, Integer> builder = new ImmutableMap.Builder<>();
        for (Integer id : context.stringColumnIdToDictColumnIds.keySet()) {
            builder.put(context.stringColumnIdToDictColumnIds.get(id), id);
        }
        ImmutableMap<Integer, Integer> dictIdsToStringIds = builder.build();
        PhysicalDecodeOperator decodeOperator = new PhysicalDecodeOperator(dictIdsToStringIds);
        OptExpression result = OptExpression.create(decodeOperator, childExpr);
        result.setStatistics(childExpr.get(0).getStatistics());
        result.setLogicalProperty(childExpr.get(0).getLogicalProperty());
        return result;
    }
}