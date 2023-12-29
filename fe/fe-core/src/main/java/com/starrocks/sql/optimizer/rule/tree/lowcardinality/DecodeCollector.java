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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.analysis.BinaryType.EQ_FOR_NULL;

/*
 * For compute all string columns that can benefit from low-cardinality optimization by bottom-up
 * 1. collect & check all string columns
 * 2. collect expressions & aggregations related to string column
 * 3. compute new string column which one is generated from dict-expression.
 * 4. compute string column optimize benefit
 */
public class DecodeCollector extends OptExpressionVisitor<DecodeInfo, DecodeInfo> {
    private static final Logger LOG = LogManager.getLogger(DecodeCollector.class);

    public static final Set<String> LOW_CARD_AGGREGATE_FUNCTIONS = Sets.newHashSet(FunctionSet.COUNT,
            FunctionSet.MULTI_DISTINCT_COUNT, FunctionSet.MAX, FunctionSet.MIN, FunctionSet.APPROX_COUNT_DISTINCT);

    public static final Set<String> LOW_CARD_STRING_FUNCTIONS =
            ImmutableSet.of(FunctionSet.APPEND_TRAILING_CHAR_IF_ABSENT, FunctionSet.CONCAT, FunctionSet.CONCAT_WS,
                    FunctionSet.HEX, FunctionSet.LEFT, FunctionSet.LIKE, FunctionSet.LOWER, FunctionSet.LPAD,
                    FunctionSet.LTRIM, FunctionSet.REGEXP_EXTRACT, FunctionSet.REGEXP_REPLACE, FunctionSet.REPEAT,
                    FunctionSet.REPLACE, FunctionSet.REVERSE, FunctionSet.RIGHT, FunctionSet.RPAD, FunctionSet.RTRIM,
                    FunctionSet.SPLIT_PART, FunctionSet.SUBSTR, FunctionSet.SUBSTRING, FunctionSet.SUBSTRING_INDEX,
                    FunctionSet.TRIM, FunctionSet.UPPER, FunctionSet.IF);

    // array<string> support:
    //  array<string> -> array<string>: array function
    //  array<string> -> string       : array element
    public static final Set<String> LOW_CARD_ARRAY_FUNCTIONS = ImmutableSet.of(
            FunctionSet.ARRAY_MIN,  // ARRAY -> STRING
            FunctionSet.ARRAY_MAX, FunctionSet.ARRAY_DISTINCT, // ARRAY -> ARRAY
            FunctionSet.ARRAY_SORT, FunctionSet.REVERSE, FunctionSet.ARRAY_SLICE, FunctionSet.ARRAY_FILTER,
            FunctionSet.ARRAY_LENGTH, // ARRAY -> bigint, return direct
            FunctionSet.CARDINALITY);

    private final SessionVariable sessionVariable;

    // These fields are the same as the fields in the DecodeContext,
    // the difference: these fields store all string information, the
    // DecodeContext only stores the ones that need to be optimized.
    private final Map<Operator, DecodeInfo> allOperatorDecodeInfo = Maps.newIdentityHashMap();

    private final Map<Integer, ColumnDict> globalDicts = Maps.newHashMap();

    private final Map<Integer, List<ScalarOperator>> stringExpressions = Maps.newHashMap();

    private final Map<Integer, List<CallOperator>> stringAggregateExpressions = Maps.newHashMap();

    private final Map<Integer, ScalarOperator> stringRefToDefineExprMap = Maps.newHashMap();

    // string column use counter, 0 meanings decoded immediately after it was generated.
    // for compute global dict define expressions
    private final Map<Integer, Integer> expressionStringRefCounter = Maps.newHashMap();

    private final List<Integer> scanStringColumns = Lists.newArrayList();

    public DecodeCollector(SessionVariable session) {
        this.sessionVariable = session;
    }

    public void collect(OptExpression root, DecodeContext context) {
        collectImpl(root, null);
        initContext(context);
    }

    private void initContext(DecodeContext context) {
        // choose the profitable string columns
        for (Integer cid : scanStringColumns) {
            if (expressionStringRefCounter.getOrDefault(cid, 0) > 1) {
                context.allStringColumns.add(cid);
                continue;
            }
            List<ScalarOperator> dictExprList = stringExpressions.getOrDefault(cid, Collections.emptyList());
            long allExprNum = dictExprList.size();
            // only query original string-column
            long worthless = dictExprList.stream().filter(ScalarOperator::isColumnRef).count();
            // we believe that the more complex expressions using the dict-column, and the preformance will be better
            if (worthless == 0 && allExprNum != 0) {
                context.allStringColumns.add(cid);
            } else if (allExprNum > worthless && allExprNum >= worthless * 2) {
                context.allStringColumns.add(cid);
            }
        }
        // resolve depend-on relation:
        // like: b = upper(a), c = lower(b), if we forbidden a, should forbidden b & c too
        for (Integer cid : stringRefToDefineExprMap.keySet()) {
            if (context.allStringColumns.contains(cid)) {
                continue;
            }
            if (!checkDependOnExpr(cid, context.allStringColumns)) {
                continue;
            }
            if (globalDicts.containsKey(cid) || expressionStringRefCounter.getOrDefault(cid, 0) != 0) {
                context.allStringColumns.add(cid);
            }
        }

        // Save the information of profitable string columns to DecodeContext
        for (Integer cid : context.allStringColumns) {
            if (globalDicts.containsKey(cid)) {
                context.stringRefToDicts.put(cid, globalDicts.get(cid));
            }
            if (stringRefToDefineExprMap.containsKey(cid)) {
                context.stringRefToDefineExprMap.put(cid, stringRefToDefineExprMap.get(cid));
            }
            if (stringExpressions.containsKey(cid)) {
                context.stringExprsMap.put(cid, stringExpressions.get(cid));
            }
        }

        // add string column's all aggregate expression(1st & 2nd stage)
        for (Integer aggregateId : stringAggregateExpressions.keySet()) {
            List<CallOperator> aggregateExprs = stringAggregateExpressions.get(aggregateId);
            for (CallOperator agg : aggregateExprs) {
                if (agg.getColumnRefs().stream().map(ColumnRefOperator::getId)
                        .anyMatch(context.allStringColumns::contains)) {
                    context.stringAggregateExprs.addAll(aggregateExprs);
                    context.allStringColumns.add(aggregateId);
                    break;
                }
            }
        }

        ColumnRefSet alls = new ColumnRefSet();
        context.allStringColumns.forEach(alls::union);
        for (Operator operator : allOperatorDecodeInfo.keySet()) {
            DecodeInfo info = allOperatorDecodeInfo.get(operator);
            info.outputStringColumns.intersect(alls);
            info.decodeStringColumns.intersect(alls);
            info.inputStringColumns.intersect(alls);
            if (!info.isEmpty()) {
                context.operatorDecodeInfo.put(operator, info);
            }
        }
    }

    private boolean checkDependOnExpr(int cid, Collection<Integer> checkList) {
        if (checkList.contains(cid)) {
            return true;
        }
        if (!stringRefToDefineExprMap.containsKey(cid)) {
            return false;
        }
        ScalarOperator define = stringRefToDefineExprMap.get(cid);
        for (ColumnRefOperator ref : define.getColumnRefs()) {
            if (ref.getId() == cid) {
                return false;
            }
            if (!checkDependOnExpr(ref.getId(), checkList)) {
                return false;
            }
        }
        return true;
    }

    private DecodeInfo collectImpl(OptExpression optExpression, OptExpression parent) {
        DecodeInfo context;
        if (optExpression.arity() == 1) {
            OptExpression child = optExpression.inputAt(0);
            context = collectImpl(child, optExpression);
        } else {
            context = new DecodeInfo();
            for (int i = 0; i < optExpression.arity(); ++i) {
                OptExpression child = optExpression.inputAt(i);
                context.addChildInfo(collectImpl(child, optExpression));
            }
        }

        context.parent = parent;
        DecodeInfo info = optExpression.getOp().accept(this, optExpression, context);
        if (info.isEmpty()) {
            return info;
        }

        // update all stringRef usage counter
        info.decodeStringColumns.getStream().forEach(c -> {
            if (expressionStringRefCounter.getOrDefault(c, -1) == 0) {
                expressionStringRefCounter.remove(c);
            }
        });
        info.inputStringColumns.getStream().forEach(c -> {
            if (expressionStringRefCounter.containsKey(c)) {
                expressionStringRefCounter.put(c, expressionStringRefCounter.get(c) + 1);
            }
        });
        allOperatorDecodeInfo.put(optExpression.getOp(), info);
        collectPredicate(optExpression.getOp(), info);
        collectProjection(optExpression.getOp(), info);
        return info;
    }

    @Override
    public DecodeInfo visit(OptExpression optExpression, DecodeInfo context) {
        return context.createDecodeInfo();
    }

    @Override
    public DecodeInfo visitPhysicalLimit(OptExpression optExpression, DecodeInfo context) {
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalTopN(OptExpression optExpression, DecodeInfo context) {
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalJoin(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.EMPTY;
        }
        PhysicalJoinOperator join = optExpression.getOp().cast();
        DecodeInfo result = context.createOutputInfo();
        if (join.getOnPredicate() == null) {
            return result;
        }
        ColumnRefSet onColumns = join.getOnPredicate().getUsedColumns();
        if (!result.inputStringColumns.containsAny(onColumns)) {
            return result;
        }
        result.outputStringColumns.clear();
        result.inputStringColumns.getStream().forEach(c -> {
            if (onColumns.contains(c)) {
                result.decodeStringColumns.union(c);
            } else {
                result.outputStringColumns.union(c);
            }
        });
        result.inputStringColumns.except(result.decodeStringColumns);
        return result;
    }

    @Override
    public DecodeInfo visitPhysicalHashAggregate(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.EMPTY;
        }
        PhysicalHashAggregateOperator aggregate = optExpression.getOp().cast();
        DecodeInfo info = context.createOutputInfo();

        ColumnRefSet disableColumns = new ColumnRefSet();
        for (ColumnRefOperator key : aggregate.getAggregations().keySet()) {
            CallOperator agg = aggregate.getAggregations().get(key);
            if (!LOW_CARD_AGGREGATE_FUNCTIONS.contains(agg.getFnName())) {
                disableColumns.union(agg.getUsedColumns());
                disableColumns.union(key);
                continue;
            }
            if (agg.getChildren().size() != 1 || !agg.getChildren().get(0).isColumnRef()) {
                disableColumns.union(agg.getUsedColumns());
                disableColumns.union(key);
            }
        }

        if (!disableColumns.isEmpty()) {
            info.decodeStringColumns.union(info.inputStringColumns);
            info.decodeStringColumns.intersect(disableColumns);
            info.inputStringColumns.except(info.decodeStringColumns);
        }

        info.outputStringColumns.clear();
        for (ColumnRefOperator key : aggregate.getAggregations().keySet()) {
            if (disableColumns.contains(key)) {
                continue;
            }
            CallOperator value = aggregate.getAggregations().get(key);
            if (!info.inputStringColumns.containsAll(value.getUsedColumns())) {
                continue;
            }
            // aggregate ref -> aggregate expr
            stringAggregateExpressions.computeIfAbsent(key.getId(), x -> Lists.newArrayList()).add(value);
            // min/max should replace to dict column, count/count distinct don't need
            if (FunctionSet.MAX.equals(value.getFnName()) || FunctionSet.MIN.equals(value.getFnName())) {
                info.outputStringColumns.union(key.getId());
                stringRefToDefineExprMap.putIfAbsent(key.getId(), value);
                expressionStringRefCounter.put(key.getId(), 1);
            } else if (aggregate.getType().isLocal() || aggregate.getType().isDistinctLocal()) {
                // count/count distinct, need output dict-set in 1st stage
                info.outputStringColumns.union(key.getId());
            }
        }

        for (ColumnRefOperator groupBy : aggregate.getGroupBys()) {
            if (info.inputStringColumns.contains(groupBy) && !info.decodeStringColumns.contains(groupBy)) {
                info.outputStringColumns.union(groupBy);
            }
        }

        for (ColumnRefOperator partition : aggregate.getPartitionByColumns()) {
            if (info.inputStringColumns.contains(partition) && !info.decodeStringColumns.contains(partition)) {
                info.outputStringColumns.union(partition);
            }
        }

        return info;
    }

    @Override
    public DecodeInfo visitPhysicalDistribution(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.EMPTY;
        }
        DecodeInfo result = context.createOutputInfo();
        // 1. join on-predicate don't support low-cardinality optimization, must decode before shuffle
        // 2. if parent don't need dict column, `parent` will null, it's unlikely, but to check is better
        if (context.parent != null && context.parent.getOp() instanceof PhysicalJoinOperator) {
            return visitPhysicalJoin(context.parent, context);
        }
        return result;
    }

    @Override
    public DecodeInfo visitPhysicalOlapScan(OptExpression optExpression, DecodeInfo context) {
        PhysicalOlapScanOperator scan = optExpression.getOp().cast();
        OlapTable table = (OlapTable) scan.getTable();
        long version = table.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo)
                .orElse(0L);

        if ((table.getKeysType().equals(KeysType.PRIMARY_KEYS))) {
            return DecodeInfo.EMPTY;
        }
        if (table.hasForbitGlobalDict()) {
            return DecodeInfo.EMPTY;
        }

        // check dict column
        DecodeInfo info = new DecodeInfo();
        for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
            // Condition 1:
            if (!column.getType().isVarchar() && !(ArrayType.ARRAY_VARCHAR.matchesType(column.getType()) &&
                    sessionVariable.isEnableArrayLowCardinalityOptimize())) {
                continue;
            }


            ColumnStatistic columnStatistic = GlobalStateMgr.getCurrentStatisticStorage()
                    .getColumnStatistic(table, column.getName());
            // Condition 2: the varchar column is low cardinality string column
            if (!FeConstants.USE_MOCK_DICT_MANAGER && (columnStatistic.isUnknown() ||
                    columnStatistic.getDistinctValuesCount() > CacheDictManager.LOW_CARDINALITY_THRESHOLD)) {
                LOG.debug("{} isn't low cardinality string column", column.getName());
                continue;
            }

            // Condition 3: the varchar column has collected global dict
            if (!IDictManager.getInstance().hasGlobalDict(table.getId(), column.getName(), version)) {
                LOG.debug("{} doesn't have global dict", column.getName());
                continue;
            }

            Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(table.getId(), column.getName());
            // cache reaches capacity limit, randomly eliminate some keys
            // then we will get an empty dictionary.
            if (dict.isEmpty()) {
                continue;
            }

            info.outputStringColumns.union(column);
            info.inputStringColumns.union(column);
            stringRefToDefineExprMap.put(column.getId(), column);
            scanStringColumns.add(column.getId());
            expressionStringRefCounter.put(column.getId(), 0);
            globalDicts.put(column.getId(), dict.get());
        }

        if (info.outputStringColumns.isEmpty()) {
            return DecodeInfo.EMPTY;
        }

        return info;
    }

    private void collectPredicate(Operator operator, DecodeInfo info) {
        if (operator.getPredicate() == null) {
            return;
        }
        DictExpressionCollector dictExpressionCollector = new DictExpressionCollector(info.outputStringColumns);
        dictExpressionCollector.collect(operator.getPredicate());

        info.outputStringColumns.getStream().forEach(c -> {
            List<ScalarOperator> expressions = dictExpressionCollector.getDictExpressions(c);
            if (!expressions.isEmpty()) {
                // predicate only translate to string expression
                stringExpressions.computeIfAbsent(c, l -> Lists.newArrayList()).addAll(expressions);
            }
        });
    }

    private void collectProjection(Operator operator, DecodeInfo info) {
        if (operator.getProjection() == null) {
            return;
        }

        ColumnRefSet decodeInput = info.outputStringColumns;
        info.outputStringColumns = new ColumnRefSet();
        for (ColumnRefOperator key : operator.getProjection().getColumnRefMap().keySet()) {
            if (decodeInput.contains(key)) {
                info.outputStringColumns.union(key.getId());
                continue;
            }

            DictExpressionCollector dictExpressionCollector = new DictExpressionCollector(decodeInput);

            ScalarOperator value = operator.getProjection().getColumnRefMap().get(key);
            dictExpressionCollector.collect(value);

            decodeInput.getStream().forEach(c -> {
                // collect dict expression
                List<ScalarOperator> exprs = dictExpressionCollector.getDictExpressions(c);
                if (!exprs.isEmpty()) {
                    // maybe not new dict, just optimize the expression with dictionary
                    stringExpressions.computeIfAbsent(c, l -> Lists.newArrayList()).addAll(exprs);
                }

                // whole expression support dictionary, define new dict column
                // only support varchar/array<varchar> column
                if (exprs.contains(value) && supportLowCardinality(value.getType())) {
                    stringRefToDefineExprMap.put(key.getId(), value);
                    expressionStringRefCounter.putIfAbsent(key.getId(), 0);
                    info.outputStringColumns.union(key.getId());
                }
            });
        }
    }

    private static boolean supportLowCardinality(Type type) {
        return type.isVarchar() || (type.isArrayType() && ((ArrayType) type).getItemType().isVarchar());
    }

    // Check if an expression can be optimized using a dictionary
    // If the expression only contains a string column, the expression can be optimized using a dictionary
    private static class DictExpressionCollector extends ScalarOperatorVisitor<ScalarOperator, Void> {
        // if expression contains constant-ref, return CONSTANTS, it's can be optmized with other dict-column
        private static final ScalarOperator CONSTANTS = ConstantOperator.TRUE;
        // if expression contains multi columns, return VARIABLES, we should ignore the expression
        private static final ScalarOperator VARIABLES = ConstantOperator.FALSE;

        private final ColumnRefSet allDictColumnRefs;
        private final Map<Integer, List<ScalarOperator>> dictExpressions = Maps.newHashMap();

        public DictExpressionCollector(ColumnRefSet allDictColumnRefs) {
            this.allDictColumnRefs = allDictColumnRefs;
        }

        public void collect(ScalarOperator scalarOperator) {
            ScalarOperator dictColumn = scalarOperator.accept(this, null);
            saveDictExpr(dictColumn, scalarOperator);
        }

        private void saveDictExpr(ScalarOperator dictColumn, ScalarOperator dictExpr) {
            if (dictColumn.isColumnRef()) {
                dictExpressions.computeIfAbsent(((ColumnRefOperator) dictColumn).getId(),
                        x -> Lists.newArrayList()).add(dictExpr);
            } else if (!dictColumn.isConstant()) {
                // array[x], array_min(x)
                List<ColumnRefOperator> used = dictColumn.getColumnRefs();
                Preconditions.checkState(used.stream().distinct().count() == 1);
                this.dictExpressions.computeIfAbsent(used.get(0).getId(), x -> Lists.newArrayList()).add(dictExpr);
            }
        }

        public List<ScalarOperator> getDictExpressions(int columnId) {
            if (!dictExpressions.containsKey(columnId)) {
                return Collections.emptyList();
            }

            return dictExpressions.get(columnId);
        }

        public List<ScalarOperator> visitChildren(ScalarOperator operator, Void context) {
            List<ScalarOperator> children = Lists.newArrayList();
            for (ScalarOperator child : operator.getChildren()) {
                children.add(child.accept(this, context));
            }
            return children;
        }

        private ScalarOperator mergeWithArray(List<ScalarOperator> collectors, ScalarOperator scalarOperator) {
            // all constant
            if (collectors.stream().allMatch(CONSTANTS::equals)) {
                return CONSTANTS;
            }

            long variableExpr = collectors.stream().filter(VARIABLES::equals).count();
            long dictCount = collectors.stream().filter(s -> !s.isConstant()).distinct().count();
            // only one scalar operator, and it's a dict column
            if (dictCount == 1 && variableExpr == 0) {
                return collectors.stream().filter(s -> !s.isConstant()).findFirst().get();
            }

            for (int i = 0; i < collectors.size(); i++) {
                saveDictExpr(collectors.get(i), scalarOperator.getChild(i));
            }
            return VARIABLES;
        }

        private ScalarOperator forbidden(List<ScalarOperator> collectors, ScalarOperator scalarOperator) {
            // all constant
            if (collectors.stream().allMatch(CONSTANTS::equals)) {
                return CONSTANTS;
            }

            for (int i = 0; i < collectors.size(); i++) {
                saveDictExpr(collectors.get(i), scalarOperator.getChild(i));
            }
            return VARIABLES;
        }

        private ScalarOperator merge(List<ScalarOperator> collectors, ScalarOperator scalarOperator) {
            if (collectors.stream().anyMatch(s -> s.getType().isArrayType())) {
                return forbidden(collectors, scalarOperator);
            }
            return mergeWithArray(collectors, scalarOperator);
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            return forbidden(visitChildren(scalarOperator, context), scalarOperator);
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            // return actual dict-column
            if (allDictColumnRefs.contains(variable)) {
                return variable;
            }
            return VARIABLES;
        }

        @Override
        public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
            return CONSTANTS;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (FunctionSet.nonDeterministicFunctions.contains(call.getFnName())) {
                return VARIABLES;
            }

            if (FunctionSet.ARRAY_FILTER.equalsIgnoreCase(call.getFnName())) {
                List<ScalarOperator> result = visitChildren(call, context);
                return CONSTANTS.equals(result.get(1)) ? mergeWithArray(result, call) : forbidden(result, call);
            }

            if (FunctionSet.ARRAY_MIN.equalsIgnoreCase(call.getFnName()) ||
                    FunctionSet.ARRAY_MAX.equalsIgnoreCase(call.getFnName())) {
                // for support: `dictExpr(array_min(array) = 'a')`, not `dictExpr(array_min(array)) = 'a'`
                ScalarOperator result = mergeWithArray(visitChildren(call, context), call);
                return !result.isConstant() ? call : result;
            }

            if (LOW_CARD_STRING_FUNCTIONS.contains(call.getFnName()) ||
                    LOW_CARD_ARRAY_FUNCTIONS.contains(call.getFnName()) ||
                    LOW_CARD_AGGREGATE_FUNCTIONS.contains(call.getFnName())) {
                return mergeWithArray(visitChildren(call, context), call);
            }
            return forbidden(visitChildren(call, context), call);
        }

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            List<ScalarOperator> children = visitChildren(collectionElementOp, context);
            if (supportLowCardinality(collectionElementOp.getChild(0).getType())) {
                ScalarOperator result = mergeWithArray(children, collectionElementOp);
                // for support: `dictExpr(array[0] = 'a')`, not `dictExpr(array[0]) = 'a'`
                return !result.isConstant() ? collectionElementOp : result;

            }
            return forbidden(children, collectionElementOp);
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL) {
                return forbidden(visitChildren(predicate, context), predicate);
            }
            return merge(visitChildren(predicate, context), predicate);
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
            return merge(visitChildren(operator, context), operator);
        }

        @Override
        public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            return merge(visitChildren(predicate, context), predicate);
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            return merge(visitChildren(predicate, context), predicate);
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
            return merge(visitChildren(predicate, context), predicate);
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return merge(visitChildren(predicate, context), predicate);
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            return merge(visitChildren(operator, context), operator);
        }
    }
}
