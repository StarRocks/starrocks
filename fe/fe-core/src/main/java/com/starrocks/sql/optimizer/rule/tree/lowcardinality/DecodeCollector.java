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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
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
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.tree.JsonPathRewriteRule;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.CacheRelaxDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import com.starrocks.thrift.TAccessPathType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.EQ_FOR_NULL;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

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

    //TODO(by satanson): it seems that we can support more windows functions in future, at present, we only support
    // LAG/LEAD/FIRST_VALUE/LAST_VALUE and the aggregations functions which can adopt low cardinality optimization
    // and used as window function.
    public static final Set<String> LOW_CARD_WINDOW_FUNCTIONS = Sets.newHashSet(FunctionSet.LAG, FunctionSet.LEAD,
            FunctionSet.FIRST_VALUE, FunctionSet.LAST_VALUE);

    static {
        LOW_CARD_WINDOW_FUNCTIONS.addAll(LOW_CARD_AGGREGATE_FUNCTIONS);
    }

    public static final Set<String> LOW_CARD_LOCAL_AGG_FUNCTIONS = Sets.newHashSet(FunctionSet.COUNT,
            FunctionSet.MAX, FunctionSet.MIN);

    public static final Set<String> LOW_CARD_STRING_FUNCTIONS =
            ImmutableSet.of(FunctionSet.APPEND_TRAILING_CHAR_IF_ABSENT, FunctionSet.CONCAT, FunctionSet.CONCAT_WS,
                    FunctionSet.HEX, FunctionSet.LEFT, FunctionSet.LIKE, FunctionSet.LOWER, FunctionSet.LPAD,
                    FunctionSet.LTRIM, FunctionSet.REGEXP_EXTRACT, FunctionSet.REGEXP_REPLACE, FunctionSet.REPEAT,
                    FunctionSet.REPLACE, FunctionSet.REVERSE, FunctionSet.RIGHT, FunctionSet.RPAD, FunctionSet.RTRIM,
                    FunctionSet.SPLIT_PART, FunctionSet.SUBSTR, FunctionSet.SUBSTRING, FunctionSet.SUBSTRING_INDEX,
                    FunctionSet.TRIM, FunctionSet.UPPER, FunctionSet.IF, FunctionSet.LENGTH, FunctionSet.CHAR_LENGTH);

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
    private final boolean isQuery;

    // These fields are the same as the fields in the DecodeContext,
    // the difference: these fields store all string information, the
    // DecodeContext only stores the ones that need to be optimized.
    private final Map<Operator, DecodeInfo> allOperatorDecodeInfo = Maps.newIdentityHashMap();

    private final Map<Integer, ColumnDict> globalDicts = Maps.newHashMap();

    private final Map<Integer, List<ScalarOperator>> stringExpressions = Maps.newHashMap();

    private final Map<Integer, List<CallOperator>> stringAggregateExpressions = Maps.newHashMap();

    private final Map<Integer, Integer> tableFunctionDependencies = Maps.newHashMap();

    private final Map<Integer, ScalarOperator> stringRefToDefineExprMap = Maps.newHashMap();

    // string column use counter, 0 meanings decoded immediately after it was generated.
    // for compute global dict define expressions
    private final Map<Integer, Integer> expressionStringRefCounter = Maps.newHashMap();

    private final List<Integer> scanStringColumns = Lists.newArrayList();

    // For these columns we need to disable the associated rewrites.
    private final ColumnRefSet disableRewriteStringColumns = new ColumnRefSet();

    private final Set<Integer> joinEqColumnGroupIds = Sets.newHashSet();
    private final List<Pair<Integer, Integer>> joinEqColumnGroups = Lists.newArrayList();

    // operators which are the children of Match operator
    private final ColumnRefSet matchChildren = new ColumnRefSet();

    private final ColumnRefSet scanColumnRefSet = new ColumnRefSet();

    // check if there is a blocking node in plan
    private boolean canBlockingOutput = false;

    public DecodeCollector(SessionVariable session, boolean isQuery) {
        this.sessionVariable = session;
        this.isQuery = isQuery;
    }

    public void collect(OptExpression root, DecodeContext context) {
        canBlockingOutput = new CheckBlockingNode().check(root);
        collectImpl(root, null);
        initContext(context);
    }

    public boolean isValidMatchChildren() {
        if (matchChildren.isEmpty()) {
            return true;
        }

        return scanColumnRefSet.containsAll(matchChildren);
    }

    private void fillDisableStringColumns() {
        // build string dependency
        // a = upper(b) b = upper(c)
        // if disable b, disable a & c
        // build dependencies
        // a = upper(b) b = upper(c)
        // a -> set(b, c)

        Map<Integer, Set<Integer>> dependencyStringIds = Maps.newHashMap();
        // build dependencies from project exprs
        this.stringRefToDefineExprMap.forEach((k, v) -> {
            for (ColumnRefOperator columnRef : v.getColumnRefs()) {
                dependencyStringIds.computeIfAbsent(columnRef.getId(), x -> Sets.newHashSet());
                final int cid = columnRef.getId();
                if (!k.equals(cid)) {
                    dependencyStringIds.get(cid).add(k);
                }
            }
        });
        // build dependencies from aggregate exprs
        this.stringAggregateExpressions.forEach((k, v) -> {
            for (CallOperator callOperator : v) {
                for (ColumnRefOperator columnRef : callOperator.getColumnRefs()) {
                    dependencyStringIds.computeIfAbsent(columnRef.getId(), x -> Sets.newHashSet());
                    final int cid = columnRef.getId();
                    if (!k.equals(cid)) {
                        dependencyStringIds.get(cid).add(k);
                    }
                }
            }
        });

        this.tableFunctionDependencies.forEach((k, v) -> {
            dependencyStringIds.computeIfAbsent(v, x -> Sets.newHashSet()).add(k);
        });

        for (Pair<Integer, Integer> pair : joinEqColumnGroups) {
            dependencyStringIds.computeIfAbsent(pair.first, x -> Sets.newHashSet()).add(pair.second);
        }
        // build relation groups. The same closure is built into the same group
        // eg:
        // 1 -> (2, 3)
        // 2 -> (3)
        // 3 -> 3
        // 4 -> 4
        // will generate result:
        // 1 -> (1,2,3)
        // 2 -> (1,2,3)
        // 3 -> (1,2,3)
        // 4 -> (4)
        UnionFind<Integer> unionFind = new UnionFind<>();
        dependencyStringIds.forEach((k, v) -> {
            for (Integer dependency : v) {
                unionFind.union(k, dependency);
            }
        });

        for (int columnId : disableRewriteStringColumns.getColumnIds()) {
            unionFind.getEquivGroup(columnId).forEach(disableRewriteStringColumns::union);
        }
    }

    private void mergeJoinEqColumnDicts() {
        for (Pair<Integer, Integer> p : joinEqColumnGroups) {
            final int colId1 = p.first;
            final int colId2 = p.second;

            if (disableRewriteStringColumns.contains(colId1)) {
                disableRewriteStringColumns.union(colId2);
                continue;
            }
            if (disableRewriteStringColumns.contains(colId2)) {
                disableRewriteStringColumns.union(colId1);
                continue;
            }

            final ColumnDict d1 = globalDicts.get(colId1);
            final ColumnDict d2 = globalDicts.get(colId2);
            final Pair<ColumnDict, ColumnDict> mergedDict = ColumnDict.merge(d1, d2);
            if (mergedDict == null) {
                disableRewriteStringColumns.union(colId1);
                disableRewriteStringColumns.union(colId2);
                continue;
            }

            globalDicts.put(colId1, mergedDict.first);
            globalDicts.put(colId2, mergedDict.second);
        }
    }

    private void initContext(DecodeContext context) {
        mergeJoinEqColumnDicts();
        fillDisableStringColumns();

        // choose the profitable string columns
        for (Integer cid : scanStringColumns) {
            if (disableRewriteStringColumns.contains(cid)) {
                continue;
            }
            if (matchChildren.contains(cid)) {
                continue;
            }
            if (expressionStringRefCounter.getOrDefault(cid, 0) > 1) {
                context.allStringColumns.add(cid);
                continue;
            }
            List<ScalarOperator> dictExprList = stringExpressions.getOrDefault(cid, Collections.emptyList());
            long allExprNum = dictExprList.size();
            // only query original string-column
            long worthless = dictExprList.stream()
                    .filter(ScalarOperator::isColumnRef)
                    .filter(x -> !((ColumnRefOperator) x).getHints().contains(JsonPathRewriteRule.COLUMN_REF_HINT))
                    .count();
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
            if (matchChildren.contains(cid)) {
                continue;
            }
            if (context.allStringColumns.contains(cid)) {
                continue;
            }
            if (disableRewriteStringColumns.contains(cid)) {
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
            if (disableRewriteStringColumns.contains(aggregateId)) {
                continue;
            }
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
            context = DecodeInfo.create();
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
    public DecodeInfo visitPhysicalCTEAnchor(OptExpression optExpression, DecodeInfo context) {
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalNoCTE(OptExpression optExpression, DecodeInfo context) {
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalLimit(OptExpression optExpression, DecodeInfo context) {
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalTopN(OptExpression optExpression, DecodeInfo context) {
        final DecodeInfo info = context.createOutputInfo();

        PhysicalTopNOperator topN = optExpression.getOp().cast();

        ColumnRefSet disableColumns = new ColumnRefSet();
        final Map<ColumnRefOperator, CallOperator> preAggCall = topN.getPreAggCall();
        if (preAggCall != null) {
            for (ColumnRefOperator key : preAggCall.keySet()) {
                CallOperator agg = preAggCall.get(key);
                if (!LOW_CARD_LOCAL_AGG_FUNCTIONS.contains(agg.getFnName())) {
                    disableColumns.union(agg.getUsedColumns());
                    disableColumns.union(key);
                    continue;
                }
                if (agg.getChildren().size() != 1 || !agg.getChildren().get(0).isColumnRef()) {
                    disableColumns.union(agg.getUsedColumns());
                    disableColumns.union(key);
                }
            }
        }

        if (!disableColumns.isEmpty()) {
            info.decodeStringColumns.union(info.inputStringColumns);
            info.decodeStringColumns.intersect(disableColumns);
            info.inputStringColumns.except(info.decodeStringColumns);
        }

        info.outputStringColumns.clear();
        info.outputStringColumns.union(info.inputStringColumns);

        if (preAggCall != null) {
            for (ColumnRefOperator key : preAggCall.keySet()) {
                if (disableColumns.contains(key)) {
                    continue;
                }
                CallOperator value = preAggCall.get(key);
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
                }
            }
        }

        return info;
    }

    @Override
    public DecodeInfo visitPhysicalFilter(OptExpression optExpression, DecodeInfo context) {
        if (optExpression.getInputs().get(0).getOp() instanceof PhysicalOlapScanOperator) {
            // PhysicalFilter->PhysicalOlapScan is a special pattern, the Filter's predicate is extracted from OlapScan,
            // we should keep the DecodeInfo from it's input.
            return context.createOutputInfo();
        }
        return context.createDecodeInfo();
    }

    private boolean tryHandleJoinEqPredicate(DecodeInfo decodeInfo,
                                             ScalarOperator predicate) {

        if (predicate.getOpType() != OperatorType.BINARY) {
            return false;
        }

        BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
        if (!binary.getBinaryType().isEquivalence()) {
            return false;
        }

        ScalarOperator left = binary.getChild(0);
        ScalarOperator right = binary.getChild(1);
        if (!left.isColumnRef() || !right.isColumnRef()) {
            return false;
        }

        ColumnRefOperator leftRef = (ColumnRefOperator) left;
        ColumnRefOperator rightRef = (ColumnRefOperator) right;
        if (!decodeInfo.inputStringColumns.contains(leftRef) || !decodeInfo.inputStringColumns.contains(rightRef)) {
            return false;
        }

        int colId1 = leftRef.getId();
        int colId2 = rightRef.getId();
        // Each low-cardinality column can participate in only one equality condition.
        if (joinEqColumnGroupIds.contains(colId1) || joinEqColumnGroupIds.contains(colId2)) {
            return false;
        }
        // Low-cardinality columns cannot be DefineExpr, because we need merge these two global dicts.
        if (!globalDicts.containsKey(colId1) || !globalDicts.containsKey(colId2)) {
            return false;
        }

        joinEqColumnGroupIds.add(colId1);
        joinEqColumnGroupIds.add(colId2);
        joinEqColumnGroups.add(new Pair<>(colId2, colId1));
        return true;
    }

    /**
     * Retain low-cardinality columns used in join ON equality conditions and add them to joinEqColumnGroups.
     * All other columns used in join ON conditions are added to disableRewriteStringColumns.
     *
     * <p> Constraints:
     * <ul>
     * <li> Both left and right keys in the ON equality condition must be low-cardinality column refs.
     * <li> Each low-cardinality column can participate in only one equality condition.
     * <li> Low-cardinality columns cannot be DefineExpr.
     * <li> Currently only supports broadcast join.
     * </ul>
     */
    private void extractJoinEqGroups(DecodeInfo decodeInfo, ScalarOperator root) {
        if (OperatorType.COMPOUND.equals(root.getOpType())) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) root;
            if (compound.isAnd()) {
                for (ScalarOperator child : compound.getChildren()) {
                    extractJoinEqGroups(decodeInfo, child);
                }
                return;
            }
        }

        if (tryHandleJoinEqPredicate(decodeInfo, root)) {
            return;
        }

        root.getUsedColumns().getStream().forEach(disableRewriteStringColumns::union);
    }

    @Override
    public DecodeInfo visitPhysicalJoin(OptExpression optExpression, DecodeInfo context) {
        PhysicalJoinOperator join = optExpression.getOp().cast();
        DecodeInfo result = context.createOutputInfo();
        if (join.getOnPredicate() == null) {
            return result;
        }

        ColumnRefSet onColumns = join.getOnPredicate().getUsedColumns();
        if (context.outputStringColumns.isEmpty() || !result.inputStringColumns.containsAny(onColumns)) {
            onColumns.getStream().forEach(disableRewriteStringColumns::union);
            return result;
        }

        DistributionProperty leftDistribution = optExpression.getRequiredProperties().get(0).getDistributionProperty();
        DistributionProperty rightDistribution = optExpression.getRequiredProperties().get(1).getDistributionProperty();
        // Currently only supports broadcast join.
        if (!sessionVariable.isEnableLowCardinalityOptimizeForJoin() ||
                (leftDistribution.isShuffle() && rightDistribution.isShuffle())) {
            onColumns.getStream().forEach(disableRewriteStringColumns::union);
        } else {
            extractJoinEqGroups(result, join.getOnPredicate());
        }

        result.outputStringColumns.clear();
        result.inputStringColumns.getStream().forEach(c -> {
            if (!disableRewriteStringColumns.contains(c)) {
                result.outputStringColumns.union(c);
            }
        });
        result.decodeStringColumns.except(disableRewriteStringColumns);
        result.inputStringColumns.except(disableRewriteStringColumns);
        return result;
    }

    @Override
    public DecodeInfo visitPhysicalUnion(OptExpression optExpression, DecodeInfo context) {
        return visitPhysicalSetOperation(optExpression, context);
    }

    @Override
    public DecodeInfo visitPhysicalIntersect(OptExpression optExpression, DecodeInfo context) {
        return visitPhysicalSetOperation(optExpression, context);
    }

    @Override
    public DecodeInfo visitPhysicalExcept(OptExpression optExpression, DecodeInfo context) {
        return visitPhysicalSetOperation(optExpression, context);
    }

    private DecodeInfo visitPhysicalSetOperation(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }
        DistributionSpec dist = optExpression.getRequiredProperties().get(0).getDistributionProperty().getSpec();

        if (!(dist instanceof HashDistributionSpec)) {
            return visit(optExpression, context);
        }
        PhysicalSetOperation setOp = optExpression.getOp().cast();
        DecodeInfo result = context.createOutputInfo();
        result.decodeStringColumns.except(result.outputStringColumns);
        result.outputStringColumns.getStream().forEach(c -> disableRewriteStringColumns.union(c));
        result.outputStringColumns.clear();

        ColumnRefSet shuffleColumnIds = ColumnRefSet.of();
        for (int i = 0; i < optExpression.arity(); ++i) {
            OptExpression child = optExpression.inputAt(i);
            DistributionSpec childDistSpec = child.getOutputProperty().getDistributionProperty().getSpec();
            Preconditions.checkState(childDistSpec instanceof HashDistributionSpec);
            HashDistributionSpec childHashDistSpec = (HashDistributionSpec) childDistSpec;
            int childIdx = i;
            EquivalentDescriptor childEqvDesc = childHashDistSpec.getEquivDesc();
            childHashDistSpec.getShuffleColumns().forEach(shuffleCol -> setOp.getChildOutputColumns().get(childIdx)
                    .stream()
                    .filter(colRef -> childEqvDesc.isConnected(shuffleCol, new DistributionCol(colRef.getId(), true)))
                    .forEach(shuffleColumnIds::union));
        }

        if (!result.inputStringColumns.containsAny(shuffleColumnIds)) {
            return result;
        }
        shuffleColumnIds.getStream().forEach(c -> disableRewriteStringColumns.union(c));
        result.decodeStringColumns.except(disableRewriteStringColumns);
        result.inputStringColumns.except(disableRewriteStringColumns);
        return result;
    }

    @Override
    public DecodeInfo visitPhysicalAnalytic(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }
        PhysicalWindowOperator windowOp = optExpression.getOp().cast();
        DecodeInfo info = context.createOutputInfo();

        ColumnRefSet disableColumns = new ColumnRefSet();
        for (ColumnRefOperator key : windowOp.getAnalyticCall().keySet()) {
            CallOperator windowCallOp = windowOp.getAnalyticCall().get(key);
            String fnName = windowCallOp.getFnName();
            if (!LOW_CARD_WINDOW_FUNCTIONS.contains(fnName)) {
                disableColumns.union(windowCallOp.getUsedColumns());
                disableColumns.union(key);
                continue;
            }

            // LEAD/LAG with specified default value can not adopt low-cardinality optimization
            if ((fnName.equals(FunctionSet.LEAD) || fnName.equals(FunctionSet.LAG))) {
                ScalarOperator lastArg = windowCallOp.getArguments().get(windowCallOp.getArguments().size() - 1);
                if (!lastArg.isConstantNull()) {
                    disableColumns.union(windowCallOp.getUsedColumns());
                    disableColumns.union(key);
                    continue;
                }
            }

            Map<Boolean, List<ScalarOperator>> argGroups = windowCallOp.getChildren().stream()
                    .filter(Predicate.not(ScalarOperator::isConstant))
                    .collect(Collectors.partitioningBy(ScalarOperator::isColumnRef));

            List<ScalarOperator> columnRefArgs = argGroups.get(true);
            List<ScalarOperator> exprArgs = argGroups.get(false);

            // window function must have only one string-type column-ref argument.
            if (!exprArgs.isEmpty() || columnRefArgs.size() != 1) {
                disableColumns.union(windowCallOp.getUsedColumns());
                disableColumns.union(key);
            }
        }

        if (!disableColumns.isEmpty()) {
            info.decodeStringColumns.union(info.inputStringColumns);
            info.decodeStringColumns.intersect(disableColumns);
            info.inputStringColumns.except(info.decodeStringColumns);
        }

        info.outputStringColumns.clear();
        for (ColumnRefOperator key : windowOp.getAnalyticCall().keySet()) {
            if (disableColumns.contains(key)) {
                continue;
            }
            CallOperator value = windowOp.getAnalyticCall().get(key);
            if (!info.inputStringColumns.containsAll(value.getUsedColumns())) {
                continue;
            }

            stringAggregateExpressions.computeIfAbsent(key.getId(), x -> Lists.newArrayList()).add(value);
            // if the function return type is not string or array<string>, then its output column can not be
            // encoded, however the function evaluation can adopt encoded columns, for examples:
            // 1. select v1, count(t.a1) over(partition by v1) select t0, unnest(t0.a1) t(a1);
            // 2. select v1, count(distinct t.a1) over(partition by v1) select t0, unnest(t0.a1) t(a1);
            // t0.a1 is array<string> column and low-cardinality encoded.
            if (value.getType().isStringType() || value.getType().isStringArrayType()) {
                info.outputStringColumns.union(key.getId());
                stringRefToDefineExprMap.putIfAbsent(key.getId(), value);
                expressionStringRefCounter.put(key.getId(), 1);
            }
        }

        for (ScalarOperator partitionBy : windowOp.getPartitionExpressions()) {
            Preconditions.checkArgument(partitionBy instanceof ColumnRefOperator);
            ColumnRefOperator partitionByColumnRef = (ColumnRefOperator) partitionBy;
            if (info.inputStringColumns.contains(partitionByColumnRef) &&
                    !info.decodeStringColumns.contains(partitionByColumnRef)) {
                info.outputStringColumns.union(partitionByColumnRef);
            }
        }

        for (Ordering orderBy : windowOp.getOrderByElements()) {
            ColumnRefOperator orderByColumnRef = orderBy.getColumnRef();
            if (info.inputStringColumns.contains(orderByColumnRef) &&
                    !info.decodeStringColumns.contains(orderByColumnRef)) {
                info.outputStringColumns.union(orderByColumnRef);
            }
        }

        // the columns which are not arguments of window functions can also use encoded column;
        // for an example:
        // select t.a1, t.a2, lead(t.a1) over(partition by t.a2) select t0, unnest(t0.a1, t0.a2) t(a1,a2);
        // both t0.a1 and t0.a2 are array<string> and low-cardinality encoded, the output columns: t.a1, t.a2,
        // lead(t.a1) and t.a2 in partition-by all can adopt encoded columns.
        ColumnRefSet outerColumnSet = new ColumnRefSet();
        outerColumnSet.union(context.outputStringColumns);
        outerColumnSet.intersect(info.inputStringColumns);
        outerColumnSet.except(info.decodeStringColumns);
        outerColumnSet.except(disableColumns);
        info.outputStringColumns.union(outerColumnSet);
        return info;
    }

    @Override
    public DecodeInfo visitPhysicalHashAggregate(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
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
            } else if (aggregate.getType().isLocal() || aggregate.getType().isDistinct()) {
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
    public DecodeInfo visitPhysicalTableFunction(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }
        DecodeInfo info = context.createOutputInfo();
        PhysicalTableFunctionOperator tableFunc = optExpression.getOp().cast();

        if (!FunctionSet.UNNEST.equalsIgnoreCase(tableFunc.getFn().getFunctionName().getFunction())) {
            info.decodeStringColumns.union(info.inputStringColumns);
            info.decodeStringColumns.intersect(tableFunc.getFnParamColumnRefs());
            info.inputStringColumns.except(info.decodeStringColumns);
        }

        info.outputStringColumns.clear();
        for (ColumnRefOperator outerColRef : tableFunc.getOuterColRefs()) {
            if (info.inputStringColumns.contains(outerColRef)) {
                info.outputStringColumns.union(outerColRef);
            }
        }

        if (!FunctionSet.UNNEST.equalsIgnoreCase(tableFunc.getFn().getFunctionName().getFunction())) {
            return info;
        }

        Preconditions.checkState(tableFunc.getFnParamColumnRefs().size() == tableFunc.getFnResultColRefs().size());
        for (int i = 0; i < tableFunc.getFnParamColumnRefs().size(); i++) {
            ColumnRefOperator unnestOutput = tableFunc.getFnResultColRefs().get(i);
            ColumnRefOperator unnestInput = tableFunc.getFnParamColumnRefs().get(i);

            if (info.inputStringColumns.contains(unnestInput)) {
                stringRefToDefineExprMap.put(unnestOutput.getId(), unnestInput);
                expressionStringRefCounter.put(unnestOutput.getId(), 1);
                info.outputStringColumns.union(unnestOutput);
                tableFunctionDependencies.put(unnestInput.getId(), unnestOutput.getId());
            }
        }
        return info;
    }

    @Override
    public DecodeInfo visitPhysicalDistribution(OptExpression optExpression, DecodeInfo context) {
        if (context.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }
        return context.createOutputInfo();
    }

    @Override
    public DecodeInfo visitPhysicalOlapScan(OptExpression optExpression, DecodeInfo context) {
        PhysicalOlapScanOperator scan = optExpression.getOp().cast();
        OlapTable table = (OlapTable) scan.getTable();
        long version = table.getPartitions().stream().flatMap(p -> p.getSubPartitions().stream()).map(
                PhysicalPartition::getVisibleVersionTime).max(Long::compareTo).orElse(0L);

        if (table.hasForbiddenGlobalDict()) {
            return DecodeInfo.empty();
        }
        if (table.inputHasTempPartition(scan.getSelectedPartitionId())) {
            return DecodeInfo.empty();
        }

        // check dict column
        DecodeInfo info = DecodeInfo.create();
        for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
            // Condition 1:
            if (!supportAndEnabledLowCardinality(column.getType())) {
                continue;
            }

            if (!checkComplexTypeInvalid(scan, column)) {
                continue;
            }

            // If it's not an extended column, we have to check the cardinality of the column.
            // TODO(murphy) support collect cardinality of extended column
            if (!checkExtendedColumn(scan, column)) {
                ColumnStatistic columnStatistic = GlobalStateMgr.getCurrentState().getStatisticStorage()
                        .getColumnStatistic(table, column.getName());
                // Condition 2: the varchar column is low cardinality string column

                boolean alwaysCollectDict = sessionVariable.isAlwaysCollectDict();
                if (!alwaysCollectDict && !column.getType().isArrayType() && !FeConstants.USE_MOCK_DICT_MANAGER &&
                        (columnStatistic.isUnknown() ||
                                columnStatistic.getDistinctValuesCount() >
                                        CacheDictManager.LOW_CARDINALITY_THRESHOLD)) {
                    LOG.debug("{} isn't low cardinality string column", column.getName());
                    continue;
                }
            }

            // Condition 3: the varchar column has collected global dict
            Column columnObj = table.getColumn(column.getName());
            if (!IDictManager.getInstance().hasGlobalDict(table.getId(), columnObj.getColumnId(), version)) {
                LOG.debug("{} doesn't have global dict", column.getName());
                continue;
            }

            Optional<ColumnDict> dict =
                    IDictManager.getInstance().getGlobalDict(table.getId(), columnObj.getColumnId());
            // cache reaches capacity limit, randomly eliminate some keys
            // then we will get an empty dictionary.
            if (dict.isEmpty()) {
                continue;
            }

            markedAsGlobalDictOpt(info, column, dict.get());
        }

        if (info.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }

        return info;
    }

    private boolean banArrayColumnWithPredicate(PhysicalScanOperator scan, ColumnRefOperator column) {
        return column.getType().isArrayType() &&
                scan.getPredicate() != null && scan.getPredicate().getColumnRefs().contains(column);
    }

    private Pair<Boolean, Optional<ColumnDict>> checkConnectorGlobalDict(PhysicalScanOperator scan, Table table,
                                                                         ColumnRefOperator column) {
        // Condition 1:
        if (!supportAndEnabledLowCardinality(column.getType())) {
            return new Pair<>(false, Optional.empty());
        }

        // Condition 1.1:
        if (banArrayColumnWithPredicate(scan, column)) {
            return new Pair<>(false, Optional.empty());
        }

        // Condition 2: the varchar column is low cardinality string column
        ColumnStatistic columnStatistic = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getConnectorTableStatistics(table, List.of(column.getName())).get(0).getColumnStatistic();

        if (!columnStatistic.isUnknown() &&
                columnStatistic.getDistinctValuesCount() > CacheDictManager.LOW_CARDINALITY_THRESHOLD) {
            LOG.debug("{} isn't low cardinality string column", column.getName());
            return new Pair<>(false, Optional.empty());
        }

        boolean alwaysCollectDict = sessionVariable.isAlwaysCollectDictOnLake();
        if (!alwaysCollectDict && !FeConstants.USE_MOCK_DICT_MANAGER && columnStatistic.isUnknown()) {
            LOG.debug("{} isn't low cardinality string column", column.getName());
            return new Pair<>(false, Optional.empty());
        }

        // Condition 3: the varchar column has collected global dict
        if (!IRelaxDictManager.getInstance().hasGlobalDict(table.getUUID(), column.getName())) {
            LOG.debug("{} doesn't have global dict", column.getName());
            return new Pair<>(false, Optional.empty());
        }

        Optional<ColumnDict> dict = IRelaxDictManager.getInstance().getGlobalDict(table.getUUID(),
                column.getName());
        // cache reaches capacity limit, randomly eliminate some keys
        // then we will get an empty dictionary.
        if (dict.isEmpty() || dict.get().getVersion() > CacheRelaxDictManager.PERIOD_VERSION_THRESHOLD) {
            return new Pair<>(false, Optional.empty());
        }
        return new Pair<>(true, dict);
    }

    @Override
    public DecodeInfo visitPhysicalHiveScan(OptExpression optExpression, DecodeInfo context) {
        if (!canBlockingOutput || !sessionVariable.isUseLowCardinalityOptimizeOnLake() || !isQuery) {
            return DecodeInfo.empty();
        }
        PhysicalHiveScanOperator scan = optExpression.getOp().cast();
        HiveTable table = (HiveTable) scan.getTable();

        // only support parquet
        if (table.getStorageFormat() == null || !table.getStorageFormat().equals(HiveStorageFormat.PARQUET)) {
            return DecodeInfo.empty();
        }

        // check dict column
        DecodeInfo info = DecodeInfo.create();
        for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
            // don't collect partition columns
            if (table.getPartitionColumnNames().contains(column.getName())) {
                continue;
            }

            Pair<Boolean, Optional<ColumnDict>> res = checkConnectorGlobalDict(scan, table, column);
            if (!res.first) {
                continue;
            }

            markedAsGlobalDictOpt(info, column, res.second.get());
        }

        if (info.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }

        return info;
    }

    @Override
    public DecodeInfo visitPhysicalIcebergScan(OptExpression optExpression, DecodeInfo context) {
        if (!canBlockingOutput || !sessionVariable.isUseLowCardinalityOptimizeOnLake() || !isQuery) {
            return DecodeInfo.empty();
        }

        PhysicalIcebergScanOperator scan = optExpression.getOp().cast();
        IcebergTable table = (IcebergTable) scan.getTable();

        // only support parquet
        if (!table.getNativeTable().properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).
                equalsIgnoreCase("parquet")) {
            return DecodeInfo.empty();
        }

        // check dict column
        DecodeInfo info = DecodeInfo.create();
        for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
            if (table.getPartitionColumnNames().contains(column.getName())) {
                continue;
            }

            Pair<Boolean, Optional<ColumnDict>> res = checkConnectorGlobalDict(scan, table, column);
            if (!res.first) {
                continue;
            }

            markedAsGlobalDictOpt(info, column, res.second.get());
        }

        if (info.outputStringColumns.isEmpty()) {
            return DecodeInfo.empty();
        }

        return info;
    }

    private void markedAsGlobalDictOpt(DecodeInfo info, ColumnRefOperator column, ColumnDict dict) {
        info.outputStringColumns.union(column);
        info.inputStringColumns.union(column);
        stringRefToDefineExprMap.put(column.getId(), column);
        scanStringColumns.add(column.getId());
        expressionStringRefCounter.put(column.getId(), 0);
        globalDicts.put(column.getId(), dict);
        scanColumnRefSet.union(column.getId());
    }

    // complex type may be support prune subfield, doesn't read data
    private boolean checkComplexTypeInvalid(PhysicalOlapScanOperator scan, ColumnRefOperator column) {
        String colId = scan.getColRefToColumnMetaMap().get(column).getColumnId().getId();
        for (ColumnAccessPath path : scan.getColumnAccessPaths()) {
            if (!StringUtils.equalsIgnoreCase(colId, path.getPath()) || path.getType() != TAccessPathType.ROOT) {
                continue;
            }
            // check the read path
            if (path.getChildren().stream().allMatch(p -> p.getType() == TAccessPathType.OFFSET)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if the column is an extended string column, if so, it can be used for global dict optimization.
     */
    private boolean checkExtendedColumn(PhysicalOlapScanOperator scan, ColumnRefOperator column) {
        if (!sessionVariable.isEnableJSONV2DictOpt()) {
            return false;
        }
        String colId = scan.getColRefToColumnMetaMap().get(column).getColumnId().getId();
        for (ColumnAccessPath path : scan.getColumnAccessPaths()) {
            if (path.isExtended() &&
                    path.getLinearPath().equals(colId) &&
                    path.getType() == TAccessPathType.ROOT &&
                    path.getValueType().isStringType()) {
                return true;
            }
        }

        return false;
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

        matchChildren.union(dictExpressionCollector.matchChildren);
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
                info.usedStringColumns.union(c);
            });
            matchChildren.union(dictExpressionCollector.matchChildren);
        }
    }

    private static boolean supportLowCardinality(Type type) {
        return type.isVarchar() || (type.isArrayType() && ((ArrayType) type).getItemType().isVarchar());
    }

    private boolean supportAndEnabledLowCardinality(Type type) {
        if (!supportLowCardinality(type)) {
            return false;
        } else if (type.isArrayType()) {
            return sessionVariable.isEnableArrayLowCardinalityOptimize();
        } else {
            return true;
        }
    }

    @TestOnly
    public boolean canBlockingOutput() {
        return canBlockingOutput;
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

        private final ColumnRefSet matchChildren = new ColumnRefSet();

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
            List<ScalarOperator> dictColumns = collectors.stream().filter(s -> !s.isConstant()).distinct()
                    .collect(Collectors.toList());
            // only one scalar operator, and it's a dict column
            if (dictColumns.size() == 1 && variableExpr == 0) {
                return dictColumns.get(0);
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
            if (operator.getType().isArrayType()) {
                return forbidden(visitChildren(operator, context), operator);
            }
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

        @Override
        public ScalarOperator visitMatchExprOperator(MatchExprOperator operator, Void context) {
            matchChildren.union((ColumnRefOperator) operator.getChildren().get(0));
            return merge(visitChildren(operator, context), operator);
        }
    }

    public static class CheckBlockingNode extends OptExpressionVisitor<Boolean, Void> {
        private boolean visitChild(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().size() != 1) {
                return false;
            }
            OptExpression child = optExpression.getInputs().get(0);
            return child.getOp().accept(this, child, context);
        }

        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            return visitChild(optExpression, context);
        }

        @Override
        public Boolean visitPhysicalTopN(OptExpression optExpression, Void context) {
            return true;
        }

        @Override
        public Boolean visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            return true;
        }

        public boolean check(OptExpression optExpression) {
            return optExpression.getOp().accept(this, optExpression, null);
        }
    }
}
