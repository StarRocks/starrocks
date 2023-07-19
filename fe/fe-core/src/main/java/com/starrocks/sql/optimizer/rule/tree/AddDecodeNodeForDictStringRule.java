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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.EQ_FOR_NULL;

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
 * The concrete example could refer to LowCardinalityTest
 */
public class AddDecodeNodeForDictStringRule implements TreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(AddDecodeNodeForDictStringRule.class);

    private final Map<Long, Set<Integer>> tableIdToStringColumnIds = Maps.newHashMap();
    private final Map<Pair<Long, String>, ColumnDict> globalDictCache = Maps.newHashMap();

    public static final Type ID_TYPE = Type.INT;

    static class DecodeContext {
        // The parent operators whether it needs the child operators to encode
        boolean needEncode = false;
        // The child operators whether they have been encoded
        boolean hasEncoded = false;
        final ColumnRefFactory columnRefFactory;
        // Global DictCache
        // (TableID, ColumnName) -> ColumnDict
        final Map<Pair<Long, String>, ColumnDict> globalDictCache;
        final Map<Long, Set<Integer>> tableIdToStringColumnIds;
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

        public DecodeContext(Map<Pair<Long, String>, ColumnDict> globalDictCache,
                             Map<Long, Set<Integer>> tableIdToStringColumnIds, ColumnRefFactory columnRefFactory) {
            this(globalDictCache, tableIdToStringColumnIds, columnRefFactory, Lists.newArrayList());
        }

        public DecodeContext(Map<Pair<Long, String>, ColumnDict> globalDictCache,
                             Map<Long, Set<Integer>> tableIdToStringColumnIds, ColumnRefFactory columnRefFactory,
                             List<Pair<Integer, ColumnDict>> globalDicts) {
            this.globalDictCache = globalDictCache;
            this.tableIdToStringColumnIds = tableIdToStringColumnIds;
            this.columnRefFactory = columnRefFactory;
            stringColumnIdToDictColumnIds = Maps.newHashMap();
            stringFunctions = Maps.newHashMap();
            this.globalDicts = globalDicts;
            disableDictOptimizeColumns = new ColumnRefSet();
            needRewriteMultiCountDistinctColumns = Sets.newHashSet();
            allStringColumnIds = tableIdToStringColumnIds.values().stream()
                    .flatMap(x -> x.stream()).collect(Collectors.toSet());
        }

        // if column ref is an applied optimized string column, return the dictionary column.
        // else return column ref itself
        ColumnRefOperator getMappedOperator(ColumnRefOperator columnRef) {
            int id = columnRef.getId();
            Integer mapped = stringColumnIdToDictColumnIds.getOrDefault(id, id);
            return columnRefFactory.getColumnRef(mapped);
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
            Preconditions.checkState(globalDicts == other.globalDicts);
            return this;
        }

        // return a colSet of all string cols referenced by an operator which have been encoded
        // by its child operators. It means these cols should be replaced by its dict column
        // in this operator.
        public Set<Integer> getEncodedStringCols() {
            return stringColumnIdToDictColumnIds.keySet();
        }
    }

    public static class DecodeVisitor extends OptExpressionVisitor<OptExpression, DecodeContext> {

        public static boolean couldApplyDictOptimize(ScalarOperator operator, Set<Integer> dictEncodedColumnSlotIds) {
            final CouldApplyDictOptimizeContext couldApplyCtx = new CouldApplyDictOptimizeContext();
            couldApplyCtx.dictEncodedColumnSlotIds = dictEncodedColumnSlotIds;
            operator.accept(new CouldApplyDictOptimizeVisitor(), couldApplyCtx);
            return couldApplyCtx.canDictOptBeApplied;
        }

        public static boolean cannotApplyDictOptimize(ScalarOperator operator, Set<Integer> dictEncodedColumnSlotIds) {
            final CouldApplyDictOptimizeContext couldApplyCtx = new CouldApplyDictOptimizeContext();
            couldApplyCtx.dictEncodedColumnSlotIds = dictEncodedColumnSlotIds;
            operator.accept(new CouldApplyDictOptimizeVisitor(), couldApplyCtx);
            return !couldApplyCtx.canDictOptBeApplied && couldApplyCtx.stopOptPropagateUpward;
        }

        public static boolean isSimpleStrictPredicate(ScalarOperator operator) {
            return operator.accept(new IsSimpleStrictPredicateVisitor(), null);
        }

        private void visitProjectionBefore(OptExpression optExpression, DecodeContext context) {
            if (optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                context.needEncode = context.needEncode || projection.needApplyStringDict(context.allStringColumnIds);
                if (context.needEncode) {
                    projection.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns,
                            context.allStringColumnIds);
                }
            }
        }

        // exist any scalarOperator if used encoded string cols and cannot apply dict optimize
        // means we cannot optimize this projection and need add a decodeNode before this projection.
        // scalarOperators can be optimized are:
        // 1. if it's a pass-through entry like col1 -> col1, we don't care it.
        // 2. scalarOperator don't ref cols from encoded string cols, we don't care it.
        // 3. exists multiple columnRef to one dict col, we cannot rewrite this projection.
        // 4. scalarOperator ref cols from encoded string cols should meet these requirements:
        //    a. all these dict cols of these string cols exist in the global dict
        //    b. can gain benefit from the optimization
        private boolean couldApplyStringDict(DecodeContext context, Projection projection) {
            final Set<Integer> globalDictIds =
                    context.globalDicts.stream().map(a -> a.first).collect(Collectors.toSet());
            Set<Integer> encodedStringCols = context.getEncodedStringCols();
            Map<ColumnRefOperator, ColumnRefOperator> memo = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (entry.getValue().isColumnRef()) {
                    ColumnRefOperator key = entry.getKey();
                    ColumnRefOperator value = (ColumnRefOperator) entry.getValue();
                    if (globalDictIds.contains(context.stringColumnIdToDictColumnIds.get(value.getId()))) {
                        if (memo.containsKey(value)) {
                            return false;
                        } else {
                            memo.put(value, key);
                        }
                    }
                }
            }

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (!entry.getValue().equals(entry.getKey())) {
                    ScalarOperator operator = entry.getValue();
                    Set<Integer> usedCols = operator.getUsedColumns().getStream().collect(Collectors.toSet());
                    usedCols.retainAll(encodedStringCols);
                    if (!usedCols.isEmpty()) {
                        Set<Integer> dictCols = usedCols.stream().map(e -> context.stringColumnIdToDictColumnIds.get(e))
                                .collect(Collectors.toSet());
                        if (!(globalDictIds.containsAll(dictCols) &&
                                couldApplyDictOptimize(operator, encodedStringCols))) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        // create a new dictionary column and assign the same property except for the type and column id
        // the input column maybe a dictionary column or a string column
        private ColumnRefOperator createNewDictColumn(DecodeContext context, ColumnRefOperator inputColumn) {
            return context.columnRefFactory.create(inputColumn.getName(), ID_TYPE, inputColumn.isNullable());
        }

        public OptExpression visitProjectionAfter(OptExpression optExpression, DecodeContext context) {
            if (context.hasEncoded && optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                ColumnRefSet encodedStringCols = ColumnRefSet.createByIds(context.getEncodedStringCols());

                if (!projection.getUsedColumns().isIntersect(encodedStringCols)) {
                    context.clear();
                } else if (couldApplyStringDict(context, projection)) {
                    Projection newProjection = rewriteProjectOperator(projection, context);
                    optExpression.getOp().setProjection(newProjection);
                    optExpression.setLogicalProperty(rewriteLogicProperty(optExpression.getLogicalProperty(),
                            new ColumnRefSet(newProjection.getOutputColumns())));
                    return optExpression;
                } else {
                    OptExpression decodeExp = generateDecodeOExpr(context, Collections.singletonList(optExpression));
                    decodeExp.getOp().setProjection(optExpression.getOp().getProjection());
                    optExpression.getOp().setProjection(null);
                    return decodeExp;
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
            if (context.hasEncoded) {
                optExpression.setLogicalProperty(rewriteLogicProperty(optExpression.getLogicalProperty(),
                        context.stringColumnIdToDictColumnIds));
            }
            return visitProjectionAfter(optExpression, context);
        }

        public OptExpression visitPhysicalTopN(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);
            // top N node
            PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpression.getOp();
            context.needEncode = topN.couldApplyStringDict(context.allStringColumnIds);
            if (context.needEncode) {
                topN.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns, context.allStringColumnIds);
            }

            context.hasEncoded = false;
            OptExpression childExpr = optExpression.inputAt(0);
            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);

            Set<Integer> stringColumns = context.getEncodedStringCols();
            boolean needRewrite = !stringColumns.isEmpty() && topN.couldApplyStringDict(stringColumns);

            if (context.hasEncoded || needRewrite) {
                if (needRewrite) {
                    PhysicalTopNOperator newTopN = rewriteTopNOperator(topN, context);
                    OptExpression result = OptExpression.create(newTopN, newChildExpr);
                    result.setStatistics(optExpression.getStatistics());
                    result.setLogicalProperty(rewriteLogicProperty(optExpression.getLogicalProperty(),
                            context.stringColumnIdToDictColumnIds));
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
                List<ColumnRefOperator> newOutputColumns = Lists.newArrayList(scanOperator.getOutputColumns());

                List<Pair<Integer, ColumnDict>> globalDicts = context.globalDicts;
                ScalarOperator newPredicate;
                List<ScalarOperator> predicates = Utils.extractConjuncts(scanOperator.getPredicate());

                // check column could apply dict optimize and replace string column to dict column
                for (Integer columnId : context.tableIdToStringColumnIds.get(tableId)) {
                    ColumnRefOperator stringColumn = context.columnRefFactory.getColumnRef(columnId);
                    if (!scanOperator.getColRefToColumnMetaMap().containsKey(stringColumn)) {
                        continue;
                    }

                    BooleanSupplier checkColumnCouldApply = () -> {
                        if (context.disableDictOptimizeColumns.contains(columnId)) {
                            return false;
                        }

                        if (scanOperator.getPredicate() != null &&
                                scanOperator.getPredicate().getUsedColumns().contains(columnId)) {
                            // If there is an unsupported expression in any of the low cardinality columns,
                            // we disable low cardinality optimization.
                            return predicates.stream().allMatch(
                                    predicate -> !predicate.getUsedColumns().contains(columnId) ||
                                            couldApplyDictOptimize(predicate, context.allStringColumnIds));
                        }
                        return true;
                    };

                    if (!checkColumnCouldApply.getAsBoolean()) {
                        continue;
                    }

                    ColumnRefOperator newDictColumn = createNewDictColumn(context, stringColumn);

                    if (newOutputColumns.contains(stringColumn)) {
                        newOutputColumns.remove(stringColumn);
                        newOutputColumns.add(newDictColumn);
                    }

                    Column oldColumn = scanOperator.getColRefToColumnMetaMap().get(stringColumn);
                    Column newColumn = new Column(oldColumn);
                    newColumn.setType(ID_TYPE);

                    newColRefToColumnMetaMap.remove(stringColumn);
                    newColRefToColumnMetaMap.put(newDictColumn, newColumn);

                    // get dict from cache
                    ColumnDict columnDict = context.globalDictCache.get(new Pair<>(tableId, stringColumn.getName()));
                    Preconditions.checkState(columnDict != null);
                    globalDicts.add(new Pair<>(newDictColumn.getId(), columnDict));

                    context.stringColumnIdToDictColumnIds.put(columnId, newDictColumn.getId());
                    context.hasEncoded = true;
                }

                // rewrite predicate
                // get all string columns for this table
                Set<Integer> stringColumns = context.tableIdToStringColumnIds.get(tableId);
                // get all could apply this optimization string columns
                ColumnRefSet applyOptCols = new ColumnRefSet();
                stringColumns.stream().filter(cid -> context.stringColumnIdToDictColumnIds.containsKey(cid))
                        .forEach(applyOptCols::union);

                // if predicate used any apply to optimize column, it should be rewritten
                if (scanOperator.getPredicate() != null) {
                    for (int i = 0; i < predicates.size(); i++) {
                        ScalarOperator predicate = predicates.get(i);
                        if (predicate.getUsedColumns().isIntersect(applyOptCols)) {
                            final DictMappingRewriter rewriter = new DictMappingRewriter(context);
                            final ScalarOperator newCallOperator = rewriter.rewrite(predicate.clone());
                            predicates.set(i, newCallOperator);
                        }
                    }
                }

                newPredicate = Utils.compoundAnd(predicates);
                if (context.hasEncoded) {
                    // TODO: maybe have to implement a clone method to create a physical node.
                    PhysicalOlapScanOperator newOlapScan =
                            new PhysicalOlapScanOperator(scanOperator.getTable(), newColRefToColumnMetaMap,
                                    scanOperator.getDistributionSpec(), scanOperator.getLimit(), newPredicate,
                                    scanOperator.getSelectedIndexId(), scanOperator.getSelectedPartitionId(),
                                    scanOperator.getSelectedTabletId(), scanOperator.getPrunedPartitionPredicates(),
                                    scanOperator.getProjection(), scanOperator.isUsePkIndex());
                    newOlapScan.setCanUseAnyColumn(scanOperator.getCanUseAnyColumn());
                    newOlapScan.setCanUseMinMaxCountOpt(scanOperator.getCanUseMinMaxCountOpt());
                    newOlapScan.setPreAggregation(scanOperator.isPreAggregation());
                    newOlapScan.setGlobalDicts(globalDicts);
                    // set output columns because of the projection is not encoded but the colRefToColumnMetaMap has encoded.
                    // There need to set right output columns
                    newOlapScan.setOutputColumns(newOutputColumns);
                    newOlapScan.setNeedSortedByKeyPerTablet(scanOperator.needSortedByKeyPerTablet());

                    OptExpression result = new OptExpression(newOlapScan);
                    result.setLogicalProperty(rewriteLogicProperty(optExpression.getLogicalProperty(),
                            context.stringColumnIdToDictColumnIds));
                    result.setStatistics(optExpression.getStatistics());
                    return visitProjectionAfter(result, context);
                }
            }
            return visitProjectionAfter(optExpression, context);
        }

        private static LogicalProperty rewriteLogicProperty(LogicalProperty logicalProperty,
                                                            Map<Integer, Integer> stringColumnIdToDictColumnIds) {
            ColumnRefSet outputColumns = logicalProperty.getOutputColumns();
            final ColumnRefSet rewritesOutputColumns = new ColumnRefSet();
            int[] columnIds = outputColumns.getColumnIds();
            // For string column rewrite to dictionary column, other columns remain unchanged
            Arrays.stream(columnIds).map(cid -> stringColumnIdToDictColumnIds.getOrDefault(cid, cid))
                    .forEach(rewritesOutputColumns::union);
            LogicalProperty newProperty = new LogicalProperty(logicalProperty);
            newProperty.setOutputColumns(rewritesOutputColumns);
            return newProperty;
        }

        private static LogicalProperty rewriteLogicProperty(LogicalProperty logicalProperty,
                                                            ColumnRefSet outputColumns) {
            LogicalProperty newProperty = new LogicalProperty(logicalProperty);
            newProperty.setOutputColumns(outputColumns);
            return newProperty;
        }

        private Projection rewriteProjectOperator(Projection projectOperator, DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(projectOperator.getColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projectOperator.getColumnRefMap().entrySet()) {
                rewriteOneScalarOperatorForProjection(kv.getKey(), kv.getValue(), context, newProjectMap,
                        newStringToDicts);
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasEncoded = false;
            }
            return new Projection(newProjectMap);
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

            List<ColumnRefOperator> partitionByColumns = null;
            if (operator.getPartitionByColumns() != null) {
                partitionByColumns = operator.getPartitionByColumns().stream().map(context::getMappedOperator)
                        .collect(Collectors.toList());
            }

            OrderSpec newOrderSpec = new OrderSpec(orderingList);

            ScalarOperator predicate = operator.getPredicate();

            // now we have not supported predicate in sort
            if (predicate != null) {
                ColumnRefSet columns = predicate.getUsedColumns();
                for (Integer stringId : context.getEncodedStringCols()) {
                    Preconditions.checkState(!columns.contains(stringId));
                }
            }

            return new PhysicalTopNOperator(newOrderSpec, operator.getLimit(), operator.getOffset(), partitionByColumns,
                    operator.getPartitionLimit(), operator.getSortPhase(), operator.getTopNType(), operator.isSplit(),
                    operator.isEnforced(), predicate, operator.getProjection());
        }

        private void rewriteOneScalarOperatorForProjection(ColumnRefOperator keyColumn, ScalarOperator valueOperator,
                                                           DecodeContext context,
                                                           Map<ColumnRefOperator, ScalarOperator> newProjectMap,
                                                           Map<Integer, Integer> newStringToDicts) {
            ColumnRefSet encodedCols = ColumnRefSet.createByIds(context.getEncodedStringCols());
            if (!valueOperator.getUsedColumns().isIntersect(encodedCols)) {
                return;
            }

            if (valueOperator instanceof ColumnRefOperator) {
                ColumnRefOperator stringColumn = (ColumnRefOperator) valueOperator;
                Integer columnId = context.stringColumnIdToDictColumnIds.get(stringColumn.getId());
                ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(columnId);

                newProjectMap.put(dictColumn, dictColumn);
                newProjectMap.remove(keyColumn);

                newStringToDicts.put(keyColumn.getId(), dictColumn.getId());
                return;
            }

            // rewrite value operator
            final DictMappingRewriter rewriter = new DictMappingRewriter(context);
            final ScalarOperator newCallOperator = rewriter.rewrite(valueOperator.clone());
            // rewrite result:
            // 1. If the expression uses all low-cardinality optimizations,
            // then it can be rewritten as DictExpr
            // eg:
            // TYPE_STRING upper(TYPE_STRING) -> ID_TYPE DictExpr(ID_TYPE)
            // TYPE_INT cast(TYPE_STRING as TYPE_INT) -> TYPE_INT DictExpr(ID_TYPE)
            //
            // 2. Expressions can only be partially rewritten
            // eg:
            // TYPE_INT IF(TYPE_STRING > "100", rand(), 1) -> TYPE_INT -> IF(DictExpr(ID_TYPE), rand(), 1)
            if (!valueOperator.getType().equals(newCallOperator.getType())) {
                Preconditions.checkState(valueOperator.getType().isVarchar());
                Preconditions.checkState(newCallOperator.getType().equals(ID_TYPE));

                ColumnRefOperator newDictColumn = createNewDictColumn(context, keyColumn);
                newProjectMap.remove(keyColumn);
                newProjectMap.put(newDictColumn, newCallOperator);

                context.stringFunctions.put(newDictColumn, newCallOperator);
                newStringToDicts.put(keyColumn.getId(), newDictColumn.getId());
            } else {
                newProjectMap.put(keyColumn, newCallOperator);
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
            exchangeOperator.setDistributionSpec(new HashDistributionSpec(new HashDistributionDesc(shuffledColumns,
                    hashDistributionSpec.getHashDistributionDesc().getSourceType())));
            exchangeOperator.setGlobalDicts(context.globalDicts);
            return exchangeOperator;
        }

        private PhysicalHashAggregateOperator rewriteAggOperator(PhysicalHashAggregateOperator aggOperator,
                                                                 DecodeContext context) {
            Map<Integer, Integer> newStringToDicts = Maps.newHashMap();

            final List<Map.Entry<ColumnRefOperator, CallOperator>> newAggMapEntry = Lists.newArrayList();

            for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggOperator.getAggregations().entrySet()) {
                boolean canApplyDictDecodeOpt = (kv.getValue().getUsedColumns().cardinality() > 0) &&
                        (PhysicalHashAggregateOperator.COULD_APPLY_LOW_CARD_AGGREGATE_FUNCTION.contains(
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
                                Collections.singletonList(dictColumn), newFunction, oldCall.isDistinct());
                        ColumnRefOperator outputColumn = kv.getKey();
                        newAggMapEntry.add(Maps.immutableEntry(outputColumn, newCall));
                    } else if (context.stringColumnIdToDictColumnIds.containsKey(columnId)) {
                        Integer dictColumnId = context.stringColumnIdToDictColumnIds.get(columnId);
                        ColumnRefOperator dictColumn = context.columnRefFactory.getColumnRef(dictColumnId);

                        List<ScalarOperator> newArguments = Collections.singletonList(dictColumn);
                        Type[] newTypes = newArguments.stream().map(ScalarOperator::getType).toArray(Type[]::new);
                        String fnName = kv.getValue().getFnName();
                        AggregateFunction newFunction =
                                (AggregateFunction) Expr.getBuiltinFunction(kv.getValue().getFnName(), newTypes,
                                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        Type newReturnType;

                        ColumnRefOperator outputColumn = kv.getKey();

                        // For the top aggregation node, the return value is the return type. For the rest of
                        // aggregation nodes, the return value is the intermediate result.
                        // if intermediate type was null, it may be using one-stage aggregation
                        // so return type was it real return type
                        if (aggOperator.getType().isGlobal()) {
                            newReturnType = newFunction.getReturnType();
                        } else {
                            newReturnType = newFunction.getIntermediateType() == null ? newFunction.getReturnType() :
                                    newFunction.getIntermediateType();
                        }

                        // Add decode node to aggregate function that returns a string
                        if (fnName.equals(FunctionSet.MAX) || fnName.equals(FunctionSet.MIN)) {
                            ColumnRefOperator outputStringColumn = kv.getKey();
                            final ColumnRefOperator newDictColumn = createNewDictColumn(context, dictColumn);
                            if (context.stringFunctions.containsKey(dictColumn)) {
                                context.stringFunctions.put(newDictColumn, context.stringFunctions.get(dictColumn));
                            }
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

                        CallOperator newCall =
                                new CallOperator(oldCall.getFnName(), newReturnType, newArguments, newFunction,
                                        oldCall.isDistinct());

                        newAggMapEntry.add(Maps.immutableEntry(outputColumn, newCall));
                    } else {
                        newAggMapEntry.add(kv);
                    }
                } else {
                    newAggMapEntry.add(kv);
                }
            }
            Map<ColumnRefOperator, CallOperator> newAggMap = ImmutableMap.copyOf(newAggMapEntry);

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
                for (Integer stringId : context.getEncodedStringCols()) {
                    Preconditions.checkState(!columns.contains(stringId));
                }
            }

            context.stringColumnIdToDictColumnIds = newStringToDicts;
            if (newStringToDicts.isEmpty()) {
                context.hasEncoded = false;
            }
            final PhysicalHashAggregateOperator newHashAggregator =
                    new PhysicalHashAggregateOperator(aggOperator.getType(), newGroupBys, newPartitionsBy, newAggMap,
                            aggOperator.getSingleDistinctFunctionPos(), aggOperator.isSplit(), aggOperator.getLimit(),
                            aggOperator.getPredicate(), aggOperator.getProjection());
            newHashAggregator.setUseSortAgg(aggOperator.isUseSortAgg());
            return newHashAggregator;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression optExpression, DecodeContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalMergeJoin(OptExpression optExpression, DecodeContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalNestLoopJoin(OptExpression optExpression, DecodeContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        public OptExpression visitPhysicalJoin(OptExpression optExpression, DecodeContext context) {
            visitProjectionBefore(optExpression, context);
            context.needEncode = true;

            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) optExpression.getOp();
            joinOperator.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns);

            DecodeContext mergeContext = new DecodeContext(context.globalDictCache, context.tableIdToStringColumnIds,
                    context.columnRefFactory, context.globalDicts);
            for (int i = 0; i < optExpression.arity(); ++i) {
                context.clear();
                OptExpression childExpr = optExpression.inputAt(i);
                OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
                optExpression.setChild(i, newChildExpr);
                if (context.hasEncoded) {
                    if (joinOperator.couldApplyStringDict(context.getEncodedStringCols())) {
                        mergeContext.merge(context);
                    } else {
                        insertDecodeExpr(optExpression, Collections.singletonList(newChildExpr), i, context);
                    }
                }
            }

            context.clear();
            context.merge(mergeContext);
            optExpression.setLogicalProperty(rewriteLogicProperty(optExpression.getLogicalProperty(),
                    context.stringColumnIdToDictColumnIds));
            return visitProjectionAfter(optExpression, context);
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression aggExpr, DecodeContext context) {
            visitProjectionBefore(aggExpr, context);

            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) aggExpr.getOp();
            // Fix issue: https://github.com/StarRocks/starrocks/issues/19901
            // TODO(by satanson): forbid dict optimization if the Agg is multi-stage Agg and has having-clause.
            //  it is quite conservative, but actually, if the Agg's having-clause references aggregations,
            //  then the optimization can not propagate upwards. In the future, this Rule should be refined as
            //  follows:
            //   1. Assign each expr a property that describes axiom exprs on whom the expr depends. the axiom
            //      exprs means ColumnRefOperators represent tablet columns. so from this, we can break through
            //      project operators and obtain the truth whether a expr depends on dict-encoding column or not.
            //   2. Rewrite this Rule to make each visitXXX method can discards rewritten child OptExpression
            //      and resorts to the orignal child OptExpression according to the current OptExpression's state.
            if (!aggOperator.isOnePhaseAgg() && aggOperator.getPredicate() != null) {
                return aggExpr;
            }
            context.needEncode = aggOperator.couldApplyStringDict(context.allStringColumnIds);
            if (context.needEncode) {
                aggOperator.fillDisableDictOptimizeColumns(context.disableDictOptimizeColumns,
                        context.allStringColumnIds);
            }

            OptExpression childExpr = aggExpr.inputAt(0);
            context.hasEncoded = false;

            OptExpression newChildExpr = childExpr.getOp().accept(this, childExpr, context);
            boolean needRewrite = !context.needRewriteMultiCountDistinctColumns.isEmpty() &&
                    aggOperator.couldApplyStringDict(context.needRewriteMultiCountDistinctColumns);
            needRewrite = needRewrite || (!context.getEncodedStringCols().isEmpty() &&
                    aggOperator.couldApplyStringDict(context.getEncodedStringCols()));

            ColumnRefSet unsupportedAggs = new ColumnRefSet();
            aggOperator.fillDisableDictOptimizeColumns(unsupportedAggs, context.getEncodedStringCols());

            if (context.hasEncoded || needRewrite) {
                if (needRewrite && unsupportedAggs.isEmpty()) {
                    PhysicalHashAggregateOperator newAggOper = rewriteAggOperator(aggOperator, context);
                    OptExpression result = OptExpression.create(newAggOper, newChildExpr);
                    result.setStatistics(aggExpr.getStatistics());
                    result.setLogicalProperty(
                            rewriteLogicProperty(aggExpr.getLogicalProperty(), context.stringColumnIdToDictColumnIds));
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
                if (exchangeOperator.couldApplyStringDict(context.getEncodedStringCols())) {
                    PhysicalDistributionOperator newExchangeOper = rewriteDistribution(exchangeOperator, context);

                    OptExpression result = OptExpression.create(newExchangeOper, newChildExpr);
                    result.setStatistics(exchangeExpr.getStatistics());
                    result.setLogicalProperty(rewriteLogicProperty(exchangeExpr.getLogicalProperty(),
                            context.stringColumnIdToDictColumnIds));
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

        List<PhysicalOlapScanOperator> scanOperators = Utils.extractPhysicalOlapScanOperator(root);

        for (PhysicalOlapScanOperator scanOperator : scanOperators) {
            OlapTable table = (OlapTable) scanOperator.getTable();
            long version = table.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo)
                    .orElse(0L);

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

                ColumnStatistic columnStatistic =
                        GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistic(table, column.getName());
                // Condition 2: the varchar column is low cardinality string column
                if (!FeConstants.USE_MOCK_DICT_MANAGER && (columnStatistic.isUnknown() ||
                        columnStatistic.getDistinctValuesCount() > CacheDictManager.LOW_CARDINALITY_THRESHOLD)) {
                    LOG.debug("{} isn't low cardinality string column", column.getName());
                    continue;
                }

                // Condition 3: the varchar column has collected global dict
                if (IDictManager.getInstance().hasGlobalDict(table.getId(), column.getName(), version)) {
                    Optional<ColumnDict> dict =
                            IDictManager.getInstance().getGlobalDict(table.getId(), column.getName());
                    // cache reaches capacity limit, randomly eliminate some keys
                    // then we will get an empty dictionary.
                    if (!dict.isPresent()) {
                        continue;
                    }
                    globalDictCache.put(new Pair<>(table.getId(), column.getName()), dict.get());
                    if (!tableIdToStringColumnIds.containsKey(table.getId())) {
                        Set<Integer> integers = Sets.newHashSet();
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

        DecodeContext context = new DecodeContext(globalDictCache, tableIdToStringColumnIds,
                taskContext.getOptimizerContext().getColumnRefFactory());

        OptExpression rewriteExpr = root.getOp().accept(new DecodeVisitor(), root, context);
        if (context.hasEncoded) {
            return generateDecodeOExpr(context, Collections.singletonList(rewriteExpr));
        }
        return rewriteExpr;
    }

    public static void insertDecodeExpr(OptExpression parentExpr, List<OptExpression> childExpr, int index,
                                        DecodeContext context) {
        OptExpression decodeExp = generateDecodeOExpr(context, childExpr);
        parentExpr.setChild(index, decodeExp);
    }

    private static OptExpression generateDecodeOExpr(DecodeContext context, List<OptExpression> childExpr) {
        Map<ColumnRefOperator, ColumnRefOperator> dictToStrings = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : context.stringColumnIdToDictColumnIds.entrySet()) {
            dictToStrings.put(context.columnRefFactory.getColumnRef(entry.getValue()),
                    context.columnRefFactory.getColumnRef(entry.getKey()));
        }
        PhysicalDecodeOperator decodeOperator = new PhysicalDecodeOperator(ImmutableMap.copyOf(dictToStrings),
                Maps.newHashMap(context.stringFunctions));
        OptExpression result = OptExpression.create(decodeOperator, childExpr);
        result.setStatistics(childExpr.get(0).getStatistics());

        LogicalProperty decodeProperty = new LogicalProperty(childExpr.get(0).getLogicalProperty());
        result.setLogicalProperty(
                DecodeVisitor.rewriteLogicProperty(decodeProperty, decodeOperator.getDictToStrings()));
        context.clear();
        return result;
    }

    // Check if an expression can be optimized using a dictionary
    // 1. If the input column is only a dictionary column and there are no unsupported expressions in this expression,
    // then it must be able to use dictionary optimization
    // 2. If the input column is multi-column, and if there are expressions in the path of the dictionary column
    // that can use dictionary optimization, then it is also able to use dictionary optimization
    // eg:
    // select if (x = 1, dict, y) from table; couldn't use dictionary optimize. If rewritten as a dictionary
    // optimization is meaningless
    //
    // select if (dict = 1, x, y) from table; could use dictionary optimize.
    // Because we can save the overhead of string filtering

    private static class CouldApplyDictOptimizeContext {
        // can use cardinality optimized dictionary columns.
        // try to apply a low-cardinality dictionary optimization to these columns
        private Set<Integer> dictEncodedColumnSlotIds;
        // whether is worth using dictionary optimization
        private boolean worthApplied = false;
        //
        private boolean canDictOptBeApplied = false;
        // indicates the existence of expressions that do not support optimization using dictionaries
        private boolean stopOptPropagateUpward = false;

        void reset() {
            canDictOptBeApplied = false;
            stopOptPropagateUpward = false;
        }
    }

    public static class CouldApplyDictOptimizeVisitor
            extends ScalarOperatorVisitor<Void, CouldApplyDictOptimizeContext> {

        public CouldApplyDictOptimizeVisitor() {
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, CouldApplyDictOptimizeContext context) {
            context.stopOptPropagateUpward = true;
            return null;
        }

        private void couldApply(ScalarOperator operator, CouldApplyDictOptimizeContext context) {
            boolean stopOptPropagateUpward = false;
            boolean canDictOptBeApplied = false;
            // For any expression, if his child supports low cardinality optimization.
            // Then it must support low cardinality optimization.
            // Because we can let child do a low cardinality optimization,
            // the expression itself does not do any optimization
            // eg:
            // Expression(child1, child2)
            // if only child1 support, but child2 has unsupported function such as rand().
            // we can rewrite to
            // Expression(DictExpr(child1), child2)
            for (ScalarOperator child : operator.getChildren()) {
                context.reset();
                child.accept(this, context);
                stopOptPropagateUpward = stopOptPropagateUpward || context.stopOptPropagateUpward;
                canDictOptBeApplied = canDictOptBeApplied || context.canDictOptBeApplied;
            }

            // DictExpr only support one input columnRefs
            // concat(dict, dict) -> DictExpr(dict)
            // concat(dict1, dict2) -> nothing to do
            stopOptPropagateUpward |= operator.getUsedColumns().cardinality() > 1;

            // If there exist expressions that cannot be optimized using low cardinality.
            // We need to avoid unused optimizations
            // eg:
            // if (a=1, dict, c) -> nothing to do
            // if (a=1, upper(dict), c) -> if (a = 1, DictExpr(dict), c)
            if (stopOptPropagateUpward) {
                context.canDictOptBeApplied = context.worthApplied && canDictOptBeApplied;
            } else {
                context.canDictOptBeApplied = canDictOptBeApplied;
            }
            context.stopOptPropagateUpward = stopOptPropagateUpward;

        }

        @Override
        public Void visitCall(CallOperator call, CouldApplyDictOptimizeContext context) {
            if (call.getFunction() == null || !call.getFunction().isCouldApplyDictOptimize()) {
                context.stopOptPropagateUpward = true;
                return null;
            }

            couldApply(call, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitBinaryPredicate(BinaryPredicateOperator predicate, CouldApplyDictOptimizeContext context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL || !predicate.getChild(1).isConstant() ||
                    !predicate.getChild(0).isColumnRef()) {
                context.canDictOptBeApplied = false;
                context.stopOptPropagateUpward = true;
                return null;
            }

            predicate.getChild(0).accept(this, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicateOperator predicate, CouldApplyDictOptimizeContext context) {
            if (!predicate.allValuesMatch(ScalarOperator::isConstantRef) || !predicate.getChild(0).isColumnRef()) {
                context.canDictOptBeApplied = false;
                context.stopOptPropagateUpward = true;
                return null;
            }

            predicate.getChild(0).accept(this, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitIsNullPredicate(IsNullPredicateOperator predicate, CouldApplyDictOptimizeContext context) {
            if (!predicate.getChild(0).isColumnRef()) {
                context.canDictOptBeApplied = false;
                context.stopOptPropagateUpward = true;
                return null;
            }

            predicate.getChild(0).accept(this, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitCastOperator(CastOperator operator, CouldApplyDictOptimizeContext context) {
            operator.getChild(0).accept(this, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitCaseWhenOperator(CaseWhenOperator operator, CouldApplyDictOptimizeContext context) {
            couldApply(operator, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitVariableReference(ColumnRefOperator variable, CouldApplyDictOptimizeContext context) {
            context.canDictOptBeApplied = context.dictEncodedColumnSlotIds.contains(variable.getId());
            context.stopOptPropagateUpward = false;
            return null;
        }

        @Override
        public Void visitConstant(ConstantOperator literal, CouldApplyDictOptimizeContext context) {
            context.canDictOptBeApplied = false;
            context.stopOptPropagateUpward = false;
            return null;
        }

        @Override
        public Void visitLikePredicateOperator(LikePredicateOperator predicate, CouldApplyDictOptimizeContext context) {
            predicate.getChild(0).accept(this, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicateOperator predicate, CouldApplyDictOptimizeContext context) {
            if (predicate.isNot()) {
                context.canDictOptBeApplied = false;
                context.stopOptPropagateUpward = true;
                return null;
            }
            couldApply(predicate, context);
            context.worthApplied |= context.canDictOptBeApplied;
            return null;
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

            return predicate.getChild(0).isColumnRef() && predicate.allValuesMatch(ScalarOperator::isConstantRef);
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
            return !leftType.isFloatingPointType() && !leftType.isComplexType() && !leftType.isJsonType() &&
                    !leftType.isTime();
        }
    }
}