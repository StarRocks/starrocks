// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
        final Set<Integer> allStringColumnIds;
        // For the low cardinality string columns that have applied global dict optimization
        Map<Integer, Integer> stringColumnIdToDictColumnIds;
        // The string functions have applied global dict optimization
        Map<ColumnRefOperator, ScalarOperator> stringFunctions;
        // The global dict need to pass to BE in this fragment
        List<Pair<Integer, ColumnDict>> globalDicts;
        // When parent operator must need origin string column, we need to disable
        // global dict optimization for this column
        ColumnRefSet disableDictOptimizeColumns;
        // For multi-stage aggregation of count distinct, in addition to local aggregation,
        // other stages need to be rewritten as well
        Set<Integer> needRewriteMultiCountDistinctColumns;

        public DecodeContext(Map<Long, List<Integer>> tableIdToStringColumnIds, ColumnRefFactory columnRefFactory) {
            this.tableIdToStringColumnIds = tableIdToStringColumnIds;
            this.columnRefFactory = columnRefFactory;
            stringColumnIdToDictColumnIds = Maps.newHashMap();
            stringFunctions = Maps.newHashMap();
            globalDicts = Lists.newArrayList();
            disableDictOptimizeColumns = new ColumnRefSet();
            allStringColumnIds = Sets.newHashSet();
            needRewriteMultiCountDistinctColumns = Sets.newHashSet();
            for (List<Integer> ids : tableIdToStringColumnIds.values()) {
                allStringColumnIds.addAll(ids);
            }
        }

        public void clear() {
            stringColumnIdToDictColumnIds.clear();
            stringFunctions.clear();
            hasEncoded = false;
            needRewriteMultiCountDistinctColumns.clear();
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

    public static class DecodeVisitor extends OptExpressionVisitor<OptExpression, DecodeContext> {

        public static boolean couldApplyDictOptimize(ScalarOperator operator) {
            return operator.getUsedColumns().cardinality() == 1 &&
                    operator.accept(new CouldApplyDictOptimizeVisitor(), null);
        }

        public static boolean isSimpleStrictPredicate(ScalarOperator operator) {
            return operator.accept(new IsSimpleStrictPredicateVisitor(), null);
        }

        private void visitProjectionBefore(OptExpression optExpression, DecodeContext context) {
            if (optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                context.needEncode = context.needEncode || projection.needApplyStringDict(context.allStringColumnIds);
                if (context.needEncode) {
                    projection.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns);
                }
            }
        }

        private boolean projectionNeedDecode(OptExpression optExpression, DecodeContext context,
                                             Projection projection) {
            Set<Integer> stringColumnIds = context.stringColumnIdToDictColumnIds.keySet();
            Collection<Integer> dictColumnIds = context.stringColumnIdToDictColumnIds.values();
            // if projection has not support operator in dict column,
            // Decode node will be inserted
            if (projection.hasUnsupportedDictOperator(stringColumnIds)) {
                return true;
            }

            final ColumnRefSet projectOutputs = new ColumnRefSet(projection.getOutputColumns());
            final Set<Integer> globalDictIds =
                    context.globalDicts.stream().map(a -> a.first).collect(Collectors.toSet());

            // for each input in Projection,
            // if input was dict column ,but we couldn't find it in global dict keys and input column
            // is not generated by this project.
            // (This means that the column is generated by the global dictionary through the expression)
            // We insert a decode node to avoid not finding the input column
            // We Needn't handle common sub operator. because ScalarOperatorsReuseRule run after AddDecodeNodeForDictStringRule
            for (ScalarOperator operator : projection.getColumnRefMap().values()) {
                final ColumnRefSet usedColumns = operator.getUsedColumns();
                for (int cid : usedColumns.getColumnIds()) {
                    final Integer dictId = context.stringColumnIdToDictColumnIds.get(cid);
                    if (dictId != null && !globalDictIds.contains(dictId) && dictColumnIds.contains(dictId) &&
                            !projectOutputs.contains(dictId)) {
                        Preconditions.checkState(usedColumns.cardinality() == 1);
                        return true;
                    }
                }
            }

            return false;
        }

        public OptExpression visitProjectionAfter(OptExpression optExpression, DecodeContext context) {
            if (context.hasEncoded && optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                Set<Integer> stringColumnIds = context.stringColumnIdToDictColumnIds.keySet();

                if (projectionNeedDecode(optExpression, context, projection)) {
                    // child has dict columns
                    OptExpression decodeExp = generateDecodeOExpr(context, Collections.singletonList(optExpression));
                    decodeExp.getOp().setProjection(optExpression.getOp().getProjection());
                    optExpression.getOp().setProjection(null);
                    context.clear();
                    return decodeExp;
                } else if (projection.couldApplyStringDict(stringColumnIds)) {
                    Projection newProjection = rewriteProjectOperator(projection, context);
                    optExpression.getOp().setProjection(newProjection);
                    return optExpression;
                } else {
                    context.clear();
                }
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
        public OptExpression visitPhysicalLimit(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);
            OptExpression childExpr = optExpression.inputAt(0);
            context.hasEncoded = false;

            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            optExpression.setChild(0, newChildExpr);
            return visitProjectionAfter(optExpression, context);
        }

        public OptExpression visitPhysicalTopN(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);
            // top N node
            PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpression.getOp();
            context.needEncode = topN.couldApplyStringDict(context.allStringColumnIds);
            if (context.needEncode) {
                topN.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns,
                        context.allStringColumnIds);
            }

            context.hasEncoded = false;
            OptExpression childExpr = optExpression.inputAt(0);
            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);

            Set<Integer> stringColumns = context.stringColumnIdToDictColumnIds.keySet();
            boolean needRewrite = !stringColumns.isEmpty() &&
                    topN.couldApplyStringDict(stringColumns);

            if (context.hasEncoded || needRewrite) {
                if (needRewrite) {
                    PhysicalTopNOperator newTopN = rewriteTopNOperator(topN,
                            context);
                    newTopN.getUsedColumns();
                    LogicalProperty logicalProperty = optExpression.getLogicalProperty();
                    rewriteLogicProperty(logicalProperty, context.stringColumnIdToDictColumnIds);
                    OptExpression result = OptExpression.create(newTopN, newChildExpr);
                    result.setStatistics(optExpression.getStatistics());
                    result.setLogicalProperty(optExpression.getLogicalProperty());
                    return visitProjectionAfter(result, context);
                } else {
                    insertDecodeExpr(optExpression, Collections.singletonList(newChildExpr), 0, context);
                    return visitProjectionAfter(optExpression, context);
                }
            }
            optExpression.setChild(0, newChildExpr);
            return visitProjectionAfter(optExpression, context);
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
                List<ScalarOperator> predicates = Utils.extractConjuncts(scanOperator.getPredicate());

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
                        // If there is an unsupported expression in any of the low cardinality columns,
                        // we disable low cardinality optimization.
                        // TODO(stdpain):
                        // If one of the two low-cardinality columns is supported and the other is not,
                        // we could make the first column apply low cardinality optimization
                        boolean couldApply = predicates.stream()
                                .allMatch(predicate -> !predicate.getUsedColumns().contains(columnId) ||
                                        couldApplyDictOptimize(predicate));
                        if (!couldApply) {
                            globalDictStringColumns.remove(stringColumn);
                            dictStringIdToIntIds.remove(stringColumn.getId());
                            couldEncoded = false;
                        } else {
                            for (int i = 0; i < predicates.size(); i++) {
                                ScalarOperator predicate = predicates.get(i);
                                if (predicate.getUsedColumns().contains(columnId)) {
                                    Preconditions.checkState(couldApplyDictOptimize(predicate));
                                    if (newDictColumn == null) {
                                        newDictColumn = context.columnRefFactory.create(
                                                stringColumn.getName(), ID_TYPE, stringColumn.isNullable());
                                    }

                                    final DictMappingOperator newCallOperator =
                                            new DictMappingOperator(newDictColumn, predicate.clone(),
                                                    predicate.getType());

                                    predicates.set(i, newCallOperator);
                                }
                            }
                        }
                    }

                    if (!couldEncoded) {
                        continue;
                    }

                    if (newDictColumn == null) {
                        newDictColumn = context.columnRefFactory.create(
                                stringColumn.getName(), ID_TYPE, stringColumn.isNullable());
                    }
                    if (newOutputColumns.contains(stringColumn)) {
                        newOutputColumns.remove(stringColumn);
                        newOutputColumns.add(newDictColumn);
                    }

                    Column oldColumn = scanOperator.getColRefToColumnMetaMap().get(stringColumn);
                    Column newColumn = new Column(oldColumn.getName(), ID_TYPE, oldColumn.isAllowNull());

                    newColRefToColumnMetaMap.remove(stringColumn);
                    newColRefToColumnMetaMap.put(newDictColumn, newColumn);

                    Optional<ColumnDict> dict =
                            IDictManager.getInstance().getGlobalDict(tableId, stringColumn.getName());
                    if (dict != null && dict.isPresent()) {
                        globalDicts.add(new Pair<>(newDictColumn.getId(), dict.get()));
                    }

                    context.stringColumnIdToDictColumnIds.put(columnId, newDictColumn.getId());
                    context.hasEncoded = true;
                }

                newPredicate = Utils.compoundAnd(predicates);
                if (context.hasEncoded) {
                    PhysicalOlapScanOperator newOlapScan = new PhysicalOlapScanOperator(
                            scanOperator.getTable(),
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
                    // set output columns because of the projection is not encoded but the colRefToColumnMetaMap has encoded.
                    // There need to set right output columns
                    newOlapScan.setOutputColumns(newOutputColumns);
                    context.globalDicts = globalDicts;

                    OptExpression result = new OptExpression(newOlapScan);
                    result.setLogicalProperty(optExpression.getLogicalProperty());
                    result.setStatistics(optExpression.getStatistics());
                    return visitProjectionAfter(result, context);
                }
            }
            return visitProjectionAfter(optExpression, context);
        }

        private LogicalProperty rewriteLogicProperty(LogicalProperty logicalProperty,
                                                     Map<Integer, Integer> stringColumnIdToDictColumnIds) {
            ColumnRefSet outputColumns = logicalProperty.getOutputColumns();
            int[] columnIds = outputColumns.getColumnIds();
            outputColumns.clear();
            // For string column rewrite to dictionary column, other columns remain unchanged
            Arrays.stream(columnIds).map(cid -> stringColumnIdToDictColumnIds.getOrDefault(cid, cid))
                    .forEach(outputColumns::union);
            return logicalProperty;
        }

        private Projection rewriteProjectOperator(Projection projectOperator,
                                                  DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            context.stringColumnIdToDictColumnIds.putAll(newStringToDicts);

            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(projectOperator.getColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
                rewriteOneScalarOperatorForProjection(kv.getKey(), kv.getValue(), context,
                        newProjectMap, newStringToDicts);
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasEncoded = false;
            }
            return new Projection(newProjectMap, projectOperator.getCommonSubOperatorMap());
        }

        private PhysicalTopNOperator rewriteTopNOperator(PhysicalTopNOperator operator, DecodeContext context) {

            List<Ordering> orderingList = Lists.newArrayList();
            for (Ordering orderDesc : operator.getOrderSpec().getOrderDescs()) {
                final ColumnRefOperator columnRef = orderDesc.getColumnRef();
                if (context.stringColumnIdToDictColumnIds.containsKey(columnRef.getId())) {
                    Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnRef.getId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);
                    orderingList.add(new Ordering(dictColumn, orderDesc.isAscending(), orderDesc.isNullsFirst()));
                } else {
                    orderingList.add(orderDesc);
                }
            }
            OrderSpec newOrderSpec = new OrderSpec(orderingList);

            ScalarOperator predicate = operator.getPredicate();

            // now we have not support predicate in sort
            if (predicate != null) {
                ColumnRefSet columns = predicate.getUsedColumns();
                for (Integer stringId : context.stringColumnIdToDictColumnIds.keySet()) {
                    Preconditions.checkState(!columns.contains(stringId));
                }
            }

            return new PhysicalTopNOperator(newOrderSpec, operator.getLimit(),
                    operator.getOffset(),
                    null,
                    operator.getSortPhase(),
                    operator.isSplit(),
                    operator.isEnforced(),
                    predicate,
                    operator.getProjection()
            );
        }

        private void rewriteOneScalarOperatorForProjection(ColumnRefOperator keyColumn,
                                                           ScalarOperator valueOperator,
                                                           DecodeContext context,
                                                           Map<ColumnRefOperator, ScalarOperator> newProjectMap,
                                                           Map<Integer, Integer> newStringToDicts) {
            if (valueOperator instanceof ColumnRefOperator) {
                ColumnRefOperator stringColumn = (ColumnRefOperator) valueOperator;
                if (context.stringColumnIdToDictColumnIds.containsKey(stringColumn.getId())) {
                    Integer columnId = context.stringColumnIdToDictColumnIds.get(stringColumn.getId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                    newProjectMap.put(dictColumn, dictColumn);
                    newProjectMap.remove(keyColumn);

                    newStringToDicts.put(keyColumn.getId(), dictColumn.getId());
                }
                return;
            }

            if (!Projection.couldApplyDictOptimize(valueOperator)) {
                return;
            }

            int stringColumnId = valueOperator.getUsedColumns().getFirstId();
            if (context.stringColumnIdToDictColumnIds.containsKey(stringColumnId)) {
                // if output was TYPE_VARCHAR rewrite as DictColumn
                // if output was other type, only rewrite input column
                if (valueOperator.getType().isVarchar()) {
                    Integer columnId =
                            context.stringColumnIdToDictColumnIds.get(valueOperator.getUsedColumns().getFirstId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                    ColumnRefOperator newDictColumn = context.columnRefFactory.create(
                            keyColumn.getName(), ID_TYPE, keyColumn.isNullable());

                    final DictMappingOperator newCallOperator =
                            new DictMappingOperator(dictColumn, valueOperator.clone(), ID_TYPE);

                    newProjectMap.put(newDictColumn, newCallOperator);
                    newProjectMap.remove(keyColumn);

                    context.stringFunctions.put(newDictColumn, newCallOperator);

                    newStringToDicts.put(keyColumn.getId(), newDictColumn.getId());
                } else {
                    Integer columnId =
                            context.stringColumnIdToDictColumnIds.get(valueOperator.getUsedColumns().getFirstId());
                    ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);
                    final DictMappingOperator newCallOperator =
                            new DictMappingOperator(dictColumn, valueOperator.clone(), valueOperator.getType());
                    newProjectMap.put(keyColumn, newCallOperator);
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
                boolean canApplyDictDecodeOpt = (kv.getValue().getUsedColumns().cardinality() > 0) &&
                        (PhysicalHashAggregateOperator.couldApplyLowCardAggregateFunction.contains(
                                kv.getValue().getFnName()));
                if (canApplyDictDecodeOpt) {
                    CallOperator oldCall = kv.getValue();
                    int columnId = kv.getValue().getUsedColumns().getFirstId();
                    if (context.needRewriteMultiCountDistinctColumns.contains(columnId)) {
                        // we only need rewrite TFunction
                        Type[] newTypes = new Type[] {ID_TYPE};
                        AggregateFunction newFunction =
                                (AggregateFunction) Expr.getBuiltinFunction(kv.getValue().getFnName(), newTypes,
                                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);
                        CallOperator newCall = new CallOperator(oldCall.getFnName(), newFunction.getReturnType(),
                                Collections.singletonList(dictColumn), newFunction,
                                oldCall.isDistinct());
                        ColumnRefOperator outputColumn = kv.getKey();
                        newAggMap.put(outputColumn, newCall);
                    } else if (context.stringColumnIdToDictColumnIds.containsKey(columnId)) {
                        Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnId);
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);

                        List<ScalarOperator> newArguments = Collections.singletonList(dictColumn);
                        Type[] newTypes = newArguments.stream().map(ScalarOperator::getType).toArray(Type[]::new);
                        String fnName = kv.getValue().getFnName();
                        AggregateFunction newFunction =
                                (AggregateFunction) Expr.getBuiltinFunction(kv.getValue().getFnName(), newTypes,
                                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        Type newReturnType = null;

                        ColumnRefOperator outputColumn = kv.getKey();

                        // For the top aggregation node, the return value is the return type. For the rest of
                        // aggregation nodes, the return value is the intermediate result.
                        // if intermediate type was null, it may be using one-stage aggregation
                        // so return type was it real return type
                        if (aggOperator.getType().isGlobal()) {
                            newReturnType = newFunction.getReturnType();
                        } else {
                            newReturnType = newFunction.getIntermediateType() == null ?
                                    newFunction.getReturnType() : newFunction.getIntermediateType();
                        }

                        // Add decode node to aggregate function that returns a string
                        if (fnName.equals(FunctionSet.MAX) || fnName.equals(FunctionSet.MIN)) {
                            ColumnRefOperator outputStringColumn = kv.getKey();
                            final ColumnRefOperator newDictColumn = context.columnRefFactory.create(
                                    dictColumn.getName(), ID_TYPE, dictColumn.isNullable());
                            newAggMap.remove(outputStringColumn);
                            newStringToDicts.put(outputStringColumn.getId(), newDictColumn.getId());

                            for (Pair<Integer, ColumnDict> globalDict : context.globalDicts) {
                                if (globalDict.first.equals(dictColumnId)) {
                                    context.globalDicts.add(new Pair<>(newDictColumn.getId(), globalDict.second));
                                    break;
                                }
                            }

                            outputColumn = newDictColumn;
                        } else if (fnName.equals(FunctionSet.MULTI_DISTINCT_COUNT)) {
                            context.needRewriteMultiCountDistinctColumns.add(outputColumn.getId());
                        }

                        CallOperator newCall = new CallOperator(oldCall.getFnName(), newReturnType,
                                newArguments, newFunction,
                                oldCall.isDistinct());

                        newAggMap.put(outputColumn, newCall);
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
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalMergeJoin(OptExpression optExpression, DecodeContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        public OptExpression visitPhysicalJoin(OptExpression optExpression, DecodeContext context) {
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

            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) aggExpr.getOp();
            context.needEncode = aggOperator.couldApplyStringDict(context.allStringColumnIds);
            if (context.needEncode) {
                aggOperator.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns,
                        context.allStringColumnIds);
            }

            OptExpression childExpr = aggExpr.inputAt(0);
            context.hasEncoded = false;

            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            boolean needRewrite =
                    !context.needRewriteMultiCountDistinctColumns.isEmpty() &&
                            aggOperator.couldApplyStringDict(context.needRewriteMultiCountDistinctColumns);
            needRewrite = needRewrite || (!context.stringColumnIdToDictColumnIds.keySet().isEmpty() &&
                    aggOperator.couldApplyStringDict(context.stringColumnIdToDictColumnIds.keySet()));
            if (context.hasEncoded || needRewrite) {
                if (needRewrite) {
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

            if ((table.getKeysType().equals(KeysType.PRIMARY_KEYS))) {
                continue;
            }
            if (table.hasForbitGlobalDict()) {
                continue;
            }
            for (ColumnRefOperator column : scanOperator.getColRefToColumnMetaMap().keySet()) {
                // Condition 1:
                if (!column.getType().isVarchar()) {
                    continue;
                }

                ColumnStatistic columnStatistic = GlobalStateMgr.getCurrentStatisticStorage().
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
            dictToStrings.put(dictId, id);
        }
        PhysicalDecodeOperator decodeOperator = new PhysicalDecodeOperator(ImmutableMap.copyOf(dictToStrings),
                Maps.newHashMap(context.stringFunctions));
        decodeOperator.setLimit(childExpr.get(0).getOp().getLimit());
        OptExpression result = OptExpression.create(decodeOperator, childExpr);
        result.setStatistics(childExpr.get(0).getStatistics());
        result.setLogicalProperty(childExpr.get(0).getLogicalProperty());
        return result;
    }

    public static class CouldApplyDictOptimizeVisitor extends ScalarOperatorVisitor<Boolean, Void> {

        public CouldApplyDictOptimizeVisitor() {
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }

        @Override
        public Boolean visitCall(CallOperator call, Void context) {
            if (!call.getFunction().isCouldApplyDictOptimize()) {
                return false;
            }
            return call.getChildren().stream().allMatch(scalarOperator -> scalarOperator.accept(this, null));
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL) {
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
            return operator.getChild(0).accept(this, null);
        }

        @Override
        public Boolean visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            return operator.getChildren().stream().allMatch(scalarOperator -> scalarOperator.accept(this, null));
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

            if (!checkTypeCanPushDown(predicate)) {
                return false;
            }

            return predicate.getChild(0).isColumnRef();
        }

        @Override
        public Boolean visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!checkTypeCanPushDown(predicate)) {
                return false;
            }

            return predicate.getChild(0).isColumnRef() &&
                    predicate.allValuesMatch(ScalarOperator::isConstantRef);
        }

        @Override
        public Boolean visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            if (!checkTypeCanPushDown(predicate)) {
                return false;
            }

            return predicate.getChild(0).isColumnRef();
        }

        // These type predicates couldn't be pushed down to storage engine,
        // which are consistent with BE implementations.
        private boolean checkTypeCanPushDown(ScalarOperator scalarOperator) {
            Type leftType = scalarOperator.getChild(0).getType();
            return !leftType.isFloatingPointType() && !leftType.isJsonType() && !leftType.isTime();
        }
    }
}