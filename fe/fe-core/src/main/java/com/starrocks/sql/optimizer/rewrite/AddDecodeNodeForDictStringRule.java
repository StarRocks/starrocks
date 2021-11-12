// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
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

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ_FOR_NULL;

/**
 * For a low cardinality string column with global dict, we will rewrite the plan to
 * speed up query with global dict.
 * <p>
 * 1. Check the olap scan nodes have low cardinality global dict string column
 * 2. Replace the string column with the dict encoded int column
 * 3. Bottom up traverse the plan tree, if the operator could apply global dict, then
 * Replace the string column with the dict encoded int column,
 * else insert the decode operator into the tree
 * 4. The decode operator will translate the encoded int column to string column
 * <p>
 * The concrete example could refer to DecodeRewriteTest
 */
public class AddDecodeNodeForDictStringRule implements PhysicalOperatorTreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(AddDecodeNodeForDictStringRule.class);

    private final Map<Long, List<Integer>> tableIdToStringColumnIds = Maps.newHashMap();

    private static final Type ID_TYPE = Type.INT;

    static class DecodeContext {
        // The parent operators whether need the child operators to encode
        boolean needEncode = false;
        // The child operators whether have encoded
        boolean hasEncoded = false;
        final ColumnRefFactory columnRefFactory;
        final Map<Long, List<Integer>> tableIdToStringColumnIds;
        // For the low cardinality string columns that have applied global dict optimization
        Map<Integer, Integer> stringColumnIdToDictColumnIds;
        // The string functions have applied global dict optimization
        Map<ColumnRefOperator, ScalarOperator> stringFunctions;
        // The global dict need to pass to BE in this fragment
        List<Pair<Integer, ColumnDict>> globalDicts;
        // When parent operator must need origin string column, we need to disable
        // global dict optimization for this column
        ColumnRefSet disableDictOptimizeColumns;

        public DecodeContext(Map<Long, List<Integer>> tableIdToStringColumnIds, ColumnRefFactory columnRefFactory) {
            this.tableIdToStringColumnIds = tableIdToStringColumnIds;
            this.columnRefFactory = columnRefFactory;
            stringColumnIdToDictColumnIds = Maps.newHashMap();
            stringFunctions = Maps.newHashMap();
            globalDicts = Lists.newArrayList();
            disableDictOptimizeColumns = new ColumnRefSet();
        }

        public void clear() {
            stringColumnIdToDictColumnIds.clear();
            stringFunctions.clear();
            hasEncoded = false;
        }

        public DecodeContext merge(DecodeContext other) {
            if (!other.hasEncoded) {
                return this;
            }
            this.hasEncoded = true;
            this.stringColumnIdToDictColumnIds.putAll(other.stringColumnIdToDictColumnIds);
            this.stringFunctions.putAll(other.stringFunctions);
            this.disableDictOptimizeColumns = other.disableDictOptimizeColumns;
            for (Pair<Integer, ColumnDict> dict : other.globalDicts) {
                if (!this.globalDicts.contains(dict)) {
                    this.globalDicts.add(dict);
                }
            }
            return this;
        }
    }

    private static class DecodeVisitor extends OptExpressionVisitor<OptExpression, DecodeContext> {

        public static boolean couldApplyDictOptimize(ScalarOperator operator) {
            return operator.accept(new CouldApplyDictOptimizeVisitor(), null);
        }

        public static boolean isSimpleStrictPredicate(ScalarOperator operator) {
            return operator.accept(new IsSimpleStrictPredicateVisitor(), null);
        }

        private void visitProjectionBefore(OptExpression optExpression, DecodeContext context) {
            if (optExpression.getOp().getProjection() != null) {
                context.needEncode = true;
                Projection projection = optExpression.getOp().getProjection();
                projection.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns);
            }
        }

        public OptExpression visitProjectionAfter(OptExpression optExpression, DecodeContext context) {
            if (context.hasEncoded && optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                if (projection.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    Projection newProjection = rewriteProjectOperator(projection, context);
                    optExpression.getOp().setProjection(newProjection);
                    return optExpression;
                }
                context.clear();
            }
            return optExpression;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);

            for (int i = 0; i < optExpression.arity(); ++i) {
                context.hasEncoded = false;
                OptExpression childExpr = optExpression.inputAt(i);
                visitProjectionBefore(childExpr, context);

                OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
                if (context.hasEncoded) {
                    insertDecodeExpr(optExpression, Collections.singletonList(newChildExpr), i, context);
                } else {
                    optExpression.setChild(i, newChildExpr);
                }
            }
            return visitProjectionAfter(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalDecode(OptExpression optExpression, DecodeContext context) {
            context.hasEncoded = false;
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);

            if (!context.needEncode) {
                return optExpression;
            }

            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            long tableId = scanOperator.getTable().getId();
            if (context.tableIdToStringColumnIds.containsKey(scanOperator.getTable().getId())) {
                Map<ColumnRefOperator, Column> newColRefToColumnMetaMap =
                        Maps.newHashMap(scanOperator.getColRefToColumnMetaMap());

                List<ColumnRefOperator> newOutputColumns =
                        Lists.newArrayList(scanOperator.getOutputColumns());

                List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
                List<ColumnRefOperator> globalDictStringColumns = Lists.newArrayList();
                Map<Integer, Integer> dictStringIdToIntIds = Maps.newHashMap();
                ScalarOperator newPredicate = scanOperator.getPredicate();

                for (Integer columnId : context.tableIdToStringColumnIds.get(tableId)) {
                    if (context.disableDictOptimizeColumns.contains(columnId)) {
                        continue;
                    }

                    ColumnRefOperator stringColumn = context.columnRefFactory.getColumnRef(columnId);
                    if (!scanOperator.getColRefToColumnMetaMap().containsKey(stringColumn)) {
                        continue;
                    }

                    ColumnRefOperator newDictColumn = null;
                    boolean couldEncoded = true;
                    if (scanOperator.getPredicate() != null &&
                            scanOperator.getPredicate().getUsedColumns().contains(columnId)) {
                        List<ScalarOperator> predicates = Utils.extractConjuncts(scanOperator.getPredicate());
                        for (ScalarOperator predicate : predicates) {
                            if (predicate.getUsedColumns().contains(columnId)) {
                                if (couldApplyDictOptimize(predicate)) {
                                    if (newDictColumn == null) {
                                        newDictColumn = context.columnRefFactory.create(
                                                stringColumn.getName(), ID_TYPE, stringColumn.isNullable());
                                    }

                                    // For simple predicate, our olap scan node handle it by string,
                                    // So we couldn't rewrite it.
                                    if (isSimpleStrictPredicate(predicate)) {
                                        globalDictStringColumns.add(stringColumn);
                                        dictStringIdToIntIds.put(stringColumn.getId(), newDictColumn.getId());
                                    } else {
                                        // Rewrite the predicate
                                        Map<ColumnRefOperator, ScalarOperator> rewriteMap =
                                                Maps.newHashMapWithExpectedSize(1);
                                        rewriteMap.put(stringColumn, newDictColumn);
                                        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
                                        ScalarOperator rewritePredicate = predicate.accept(rewriter, null);
                                        Preconditions.checkState(rewritePredicate.equals(predicate));
                                    }
                                } else {
                                    globalDictStringColumns.remove(stringColumn);
                                    dictStringIdToIntIds.remove(stringColumn.getId());
                                    couldEncoded = false;
                                    break;
                                }
                            }
                        }
                        newPredicate = Utils.compoundAnd(predicates);
                    }

                    if (!couldEncoded) {
                        continue;
                    }

                    if (newDictColumn == null) {
                        newDictColumn = context.columnRefFactory.create(
                                stringColumn.getName(), ID_TYPE, stringColumn.isNullable());
                    }

                    newOutputColumns.remove(stringColumn);
                    newOutputColumns.add(newDictColumn);

                    Column oldColumn = scanOperator.getColRefToColumnMetaMap().get(stringColumn);
                    Column newColumn = new Column(oldColumn.getName(), ID_TYPE, oldColumn.isAllowNull());

                    newColRefToColumnMetaMap.remove(stringColumn);
                    newColRefToColumnMetaMap.put(newDictColumn, newColumn);

                    ColumnDict dict = IDictManager.getInstance().getGlobalDict(tableId, stringColumn.getName());
                    globalDicts.add(new Pair<>(newDictColumn.getId(), dict));

                    context.stringColumnIdToDictColumnIds.put(columnId, newDictColumn.getId());
                    context.hasEncoded = true;
                }

                if (context.hasEncoded) {
                    PhysicalOlapScanOperator newOlapScan = new PhysicalOlapScanOperator(
                            scanOperator.getTable(),
                            newOutputColumns,
                            newColRefToColumnMetaMap,
                            scanOperator.getDistributionSpec(),
                            scanOperator.getLimit(),
                            newPredicate,
                            scanOperator.getSelectedIndexId(),
                            scanOperator.getSelectedPartitionId(),
                            scanOperator.getSelectedTabletId(),
                            scanOperator.getProjection());
                    newOlapScan.setPreAggregation(scanOperator.isPreAggregation());
                    newOlapScan.setGlobalDicts(globalDicts);
                    newOlapScan.setGlobalDictStringColumns(globalDictStringColumns);
                    newOlapScan.setDictStringIdToIntIds(dictStringIdToIntIds);
                    context.globalDicts = globalDicts;

                    OptExpression result = new OptExpression(newOlapScan);
                    result.setLogicalProperty(optExpression.getLogicalProperty());
                    result.setStatistics(optExpression.getStatistics());
                    return visitProjectionAfter(result, context);
                }
            }
            return visitProjectionAfter(optExpression, context);
        }

        private Projection rewriteProjectOperator(Projection projectOperator,
                                                  DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            Map<ColumnRefOperator, ScalarOperator> newCommonProjectMap =
                    Maps.newHashMap(projectOperator.getCommonSubOperatorMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getCommonSubOperatorMap()
                    .entrySet()) {
                rewriteOneScalarOperatorForProjection(kv.getKey(), kv.getValue(), context,
                        newCommonProjectMap, newStringToDicts);
            }

            context.stringColumnIdToDictColumnIds.putAll(newStringToDicts);

            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(projectOperator.getColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
                if (kv.getValue() instanceof ColumnRefOperator) {
                    ColumnRefOperator stringColumn = (ColumnRefOperator) kv.getValue();
                    // If we rewrite the common project map, we need to change the value in project map
                    if (projectOperator.getCommonSubOperatorMap().containsKey(stringColumn) &&
                            !newCommonProjectMap.containsKey(stringColumn)) {
                        int dictColumnId = newStringToDicts.get(stringColumn.getId());
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                        newProjectMap.put(dictColumn, dictColumn);
                        newProjectMap.remove(kv.getKey());
                        newStringToDicts.put(kv.getKey().getId(), dictColumnId);
                    } else {
                        rewriteOneScalarOperatorForProjection(kv.getKey(), kv.getValue(), context,
                                newProjectMap, newStringToDicts);
                    }
                } else {
                    rewriteOneScalarOperatorForProjection(kv.getKey(), kv.getValue(), context,
                            newProjectMap, newStringToDicts);
                }
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasEncoded = false;
            }
            return new Projection(newProjectMap, newCommonProjectMap);
        }

        private void rewriteOneScalarOperatorForProjection(ColumnRefOperator oldStringColumn,
                                                           ScalarOperator operator,
                                                           DecodeContext context,
                                                           Map<ColumnRefOperator, ScalarOperator> newProjectMap,
                                                           Map<Integer, Integer> newStringToDicts) {
            if (operator instanceof ColumnRefOperator) {
                ColumnRefOperator stringColumn = (ColumnRefOperator) operator;
                if (context.stringColumnIdToDictColumnIds.containsKey(stringColumn.getId())) {
                    Integer columnId = context.stringColumnIdToDictColumnIds.get(stringColumn.getId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                    newProjectMap.put(dictColumn, dictColumn);
                    newProjectMap.remove(stringColumn);

                    newStringToDicts.put(stringColumn.getId(), dictColumn.getId());
                }
            } else if (operator instanceof CallOperator) {
                CallOperator callOperator = (CallOperator) operator;
                if (!couldApplyDictOptimize(callOperator)) {
                    return;
                }

                int stringColumnId = callOperator.getUsedColumns().getFirstId();
                if (context.stringColumnIdToDictColumnIds.containsKey(stringColumnId)) {
                    ColumnRefOperator oldStringArgColumn = context.columnRefFactory.getColumnRef(stringColumnId);
                    Integer columnId =
                            context.stringColumnIdToDictColumnIds.get(callOperator.getUsedColumns().getFirstId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                    ColumnRefOperator newDictColumn = context.columnRefFactory.create(
                            oldStringColumn.getName(), ID_TYPE, oldStringColumn.isNullable());

                    Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMapWithExpectedSize(1);
                    rewriteMap.put(oldStringArgColumn, dictColumn);
                    ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
                    // will modify the operator, must use clone
                    ScalarOperator newCallOperator = callOperator.clone().accept(rewriter, null);
                    newCallOperator.setType(ID_TYPE);

                    newProjectMap.put(newDictColumn, newCallOperator);
                    newProjectMap.remove(oldStringColumn);

                    context.stringFunctions.put(newDictColumn, newCallOperator);

                    newStringToDicts.put(oldStringColumn.getId(), newDictColumn.getId());
                }
            }
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
                } else {
                    shuffledColumns.add(columnId);
                }
            }
            exchangeOperator.setDistributionSpec(
                    new HashDistributionSpec(new HashDistributionDesc(shuffledColumns,
                            hashDistributionSpec.getHashDistributionDesc().getSourceType())));
            exchangeOperator.setGlobalDicts(context.globalDicts);
            return exchangeOperator;
        }

        private PhysicalHashAggregateOperator rewriteAggOperator(PhysicalHashAggregateOperator aggOperator,
                                                                 DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap(aggOperator.getAggregations());
            for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggOperator.getAggregations().entrySet()) {
                if ((kv.getValue().getFnName().equals(FunctionSet.COUNT) && !kv.getValue().getChildren().isEmpty())
                        || kv.getValue().getFnName().equals(FunctionSet.MULTI_DISTINCT_COUNT)) {
                    int columnId = kv.getValue().getUsedColumns().getFirstId();
                    if (context.stringColumnIdToDictColumnIds.containsKey(columnId)) {
                        Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnId);
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                        CallOperator newCall = new CallOperator(kv.getValue().getFnName(), kv.getValue().getType(),
                                Collections.singletonList(dictColumn), kv.getValue().getFunction(),
                                kv.getValue().isDistinct());
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

            if (aggOperator.getPredicate() != null) {
                ColumnRefSet columns = aggOperator.getPredicate().getUsedColumns();
                for (Integer stringId : context.stringColumnIdToDictColumnIds.keySet()) {
                    Preconditions.checkState(!columns.contains(stringId));
                }
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasEncoded = false;
            }
            return new PhysicalHashAggregateOperator(aggOperator.getType(),
                    newGroupBys,
                    newPartitionsBy, newAggMap,
                    aggOperator.getSingleDistinctFunctionPos(),
                    aggOperator.isSplit(),
                    aggOperator.getLimit(),
                    aggOperator.getPredicate(),
                    aggOperator.getProjection());
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);
            context.needEncode = true;

            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
            joinOperator.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns);

            DecodeContext mergeContext = new DecodeContext(
                    context.tableIdToStringColumnIds, context.columnRefFactory);
            for (int i = 0; i < optExpression.arity(); ++i) {
                context.clear();
                OptExpression childExpr = optExpression.inputAt(i);
                OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
                optExpression.setChild(i, newChildExpr);
                if (context.hasEncoded) {
                    if (joinOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                        mergeContext.merge(context);
                    } else {
                        insertDecodeExpr(optExpression, Collections.singletonList(newChildExpr), i, context);
                    }
                }
            }

            context.clear();
            context.merge(mergeContext);
            return visitProjectionAfter(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression aggExpr, DecodeContext context) {
            visitProjectionBefore(aggExpr, context);
            context.needEncode = true;

            OptExpression childExpr = aggExpr.inputAt(0);
            context.hasEncoded = false;

            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            if (context.hasEncoded) {
                PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) aggExpr.getOp();
                if (aggOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    PhysicalHashAggregateOperator newAggOper = rewriteAggOperator(aggOperator,
                            context);
                    OptExpression result = OptExpression.create(newAggOper, newChildExpr);
                    result.setStatistics(aggExpr.getStatistics());
                    result.setLogicalProperty(aggExpr.getLogicalProperty());
                    return visitProjectionAfter(result, context);
                } else {
                    insertDecodeExpr(aggExpr, Collections.singletonList(newChildExpr), 0, context);
                    return visitProjectionAfter(aggExpr, context);
                }
            }
            aggExpr.setChild(0, newChildExpr);
            return visitProjectionAfter(aggExpr, context);
        }

        @Override
        public OptExpression visitPhysicalDistribution(OptExpression exchangeExpr, DecodeContext context) {
            visitProjectionBefore(exchangeExpr, context);

            OptExpression childExpr = exchangeExpr.inputAt(0);
            context.hasEncoded = false;

            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            if (context.hasEncoded) {
                PhysicalDistributionOperator exchangeOperator = (PhysicalDistributionOperator) exchangeExpr.getOp();
                if (!(exchangeOperator.getDistributionSpec() instanceof HashDistributionSpec)) {
                    exchangeOperator.setGlobalDicts(context.globalDicts);
                    exchangeExpr.setChild(0, newChildExpr);
                    return visitProjectionAfter(exchangeExpr, context);
                }
                if (exchangeOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet())) {
                    PhysicalDistributionOperator newExchangeOper = rewriteDistribution(exchangeOperator,
                            context);

                    OptExpression result = OptExpression.create(newExchangeOper, newChildExpr);
                    result.setStatistics(exchangeExpr.getStatistics());
                    result.setLogicalProperty(exchangeExpr.getLogicalProperty());
                    return visitProjectionAfter(result, context);
                } else {
                    insertDecodeExpr(exchangeExpr, Collections.singletonList(newChildExpr), 0, context);
                    return visitProjectionAfter(exchangeExpr, context);
                }
            }
            exchangeExpr.setChild(0, newChildExpr);
            return visitProjectionAfter(exchangeExpr, context);
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
            long version = table.getPartitions().stream().map(Partition::getVisibleVersionTime)
                    .max(Long::compareTo).orElse(0L);
            for (ColumnRefOperator column : scanOperator.getColRefToColumnMetaMap().keySet()) {
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
        if (context.hasEncoded) {
            return generateDecodeOExpr(context, Collections.singletonList(rewriteExpr));
        }
        return rewriteExpr;
    }

    public static void insertDecodeExpr(OptExpression parentExpr, List<OptExpression> childExpr,
                                        int index,
                                        DecodeContext context) {
        OptExpression decodeExp = generateDecodeOExpr(context, childExpr);
        parentExpr.setChild(index, decodeExp);

        context.clear();
    }

    private static OptExpression generateDecodeOExpr(DecodeContext context, List<OptExpression> childExpr) {
        Map<Integer, Integer> dictToStrings = Maps.newHashMap();
        for (Integer id : context.stringColumnIdToDictColumnIds.keySet()) {
            int dictId = context.stringColumnIdToDictColumnIds.get(id);
            // For SQL: select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*)
            // from supplier group by S_ADDRESS
            // The project map is: 11::upper -> 12:upper
            // The project common map is: 12::upper -> upper(2:S_ADDRESS)
            // So the string column 11 and 12 will refer to the same int column
            // So we need check duplicate here
            if (!dictToStrings.containsKey(dictId)) {
                dictToStrings.put(dictId, id);
            }
        }
        PhysicalDecodeOperator decodeOperator = new PhysicalDecodeOperator(ImmutableMap.copyOf(dictToStrings),
                context.stringFunctions);
        OptExpression result = OptExpression.create(decodeOperator, childExpr);
        result.setStatistics(childExpr.get(0).getStatistics());
        result.setLogicalProperty(childExpr.get(0).getLogicalProperty());
        return result;
    }

    private static class CouldApplyDictOptimizeVisitor extends ScalarOperatorVisitor<Boolean, Void> {

        public CouldApplyDictOptimizeVisitor() {
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }

        @Override
        public Boolean visitCall(CallOperator call, Void context) {
            if (call.getUsedColumns().cardinality() > 1) {
                return false;
            }
            return call.getFunction().isCouldApplyDictOptimize();
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL) {
                return false;
            }
            if (predicate.getUsedColumns().cardinality() > 1) {
                return false;
            }
            if (!predicate.getChild(1).isConstant()) {
                return false;
            }
            return predicate.getChild(0).isColumnRef();
        }

        @Override
        public Boolean visitInPredicate(InPredicateOperator predicate, Void context) {
            return predicate.getChild(0).isColumnRef() &&
                    predicate.allValuesMatch(ScalarOperator::isConstantRef);
        }

        @Override
        public Boolean visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return predicate.getChild(0).isColumnRef();
        }

        @Override
        public Boolean visitCastOperator(CastOperator operator, Void context) {
            return false;
        }

        @Override
        public Boolean visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            return false;
        }

        @Override
        public Boolean visitVariableReference(ColumnRefOperator variable, Void context) {
            return true;
        }

        @Override
        public Boolean visitConstant(ConstantOperator literal, Void context) {
            return true;
        }

        @Override
        public Boolean visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            return true;
        }
    }

    // The predicate no function all, this implementation is consistent with BE olap scan node
    private static class IsSimpleStrictPredicateVisitor extends ScalarOperatorVisitor<Boolean, Void> {

        public IsSimpleStrictPredicateVisitor() {
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL) {
                return false;
            }
            if (predicate.getUsedColumns().cardinality() > 1) {
                return false;
            }
            if (!predicate.getChild(1).isConstant()) {
                return false;
            }
            return predicate.getChild(0).isColumnRef();
        }

        @Override
        public Boolean visitInPredicate(InPredicateOperator predicate, Void context) {
            return predicate.getChild(0).isColumnRef() &&
                    predicate.allValuesMatch(ScalarOperator::isConstantRef);
        }

        @Override
        public Boolean visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return predicate.getChild(0).isColumnRef();
        }
    }
}