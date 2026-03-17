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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE;

public class ScalarOperatorUtil {
    public static CallOperator buildMultiCountDistinct(CallOperator oldFunctionCall) {
        Function searchDesc = new Function(new FunctionName(FunctionSet.MULTI_DISTINCT_COUNT),
                oldFunctionCall.getFunction().getArgs(), InvalidType.INVALID, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return null;
        }

        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fn.getReturnType(), oldFunctionCall.getChildren(),
                        fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildFusedMultiDistinct(List<CallOperator> calls, ColumnRefOperator arg) {
        Map<String, List<CallOperator>> groups = calls.stream().collect(Collectors.groupingBy(CallOperator::getFnName));
        List<CallOperator> sumGroup = groups.getOrDefault(FunctionSet.SUM, List.of());
        List<CallOperator> avgGroup = groups.getOrDefault(FunctionSet.AVG, List.of());
        String fusedFunName;
        if (sumGroup.isEmpty() && avgGroup.isEmpty()) {
            fusedFunName = FunctionSet.FUSED_MULTI_DISTINCT_COUNT;
        } else if (avgGroup.isEmpty()) {
            fusedFunName = FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM;
        } else if (sumGroup.isEmpty()) {
            fusedFunName = FunctionSet.FUSED_MULTI_DISTINCT_COUNT_AVG;
        } else {
            fusedFunName = FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM_AVG;
        }

        Function searchDesc = new Function(new FunctionName(fusedFunName),
                List.of(arg.getType()), InvalidType.INVALID, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        fn = Objects.requireNonNull(fn);
        AggregateFunction newFn = (AggregateFunction) fn.copy();
        newFn.setArgsType(new Type[] {arg.getType()});
        StructType type = (StructType) newFn.getReturnType();
        List<StructField> fields = Lists.newArrayList();
        Type countType = type.getField(FunctionSet.COUNT).getType().clone();
        fields.add(new StructField(FunctionSet.COUNT, countType));
        if (!sumGroup.isEmpty()) {
            Type sumType = sumGroup.get(0).getType().clone();
            fields.add(new StructField(FunctionSet.SUM, sumType));
        }
        if (!avgGroup.isEmpty()) {
            Type avgType = avgGroup.get(0).getType().clone();
            fields.add(new StructField(FunctionSet.AVG, avgType));
        }
        newFn.setRetType(new StructType(fields, true));
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(fusedFunName, newFn.getReturnType(), Lists.newArrayList(arg), newFn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildSum(ColumnRefOperator arg) {
        Preconditions.checkArgument(arg.getType() == IntegerType.BIGINT);
        Function searchDesc = new Function(new FunctionName(FunctionSet.SUM),
                new Type[] {arg.getType()}, arg.getType(), false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.SUM, fn.getReturnType(), Lists.newArrayList(arg), fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static Function findArithmeticFunction(CallOperator call, String fnName) {
        return findArithmeticFunction(call.getFunction().getArgs(), fnName);
    }

    public static Function findArithmeticFunction(Type[] argsType, String fnName) {
        return ExprUtils.getBuiltinFunction(fnName, argsType, IS_IDENTICAL);
    }

    public static Function findSumFn(Type[] argTypes) {
        Function sumFn = findArithmeticFunction(argTypes, FunctionSet.SUM);
        Preconditions.checkState(sumFn != null);
        Function newFn = sumFn.copy();
        if (argTypes[0].isDecimalV3()) {
            newFn.setArgsType(argTypes);
            newFn.setRetType(TypeFactory.createDecimalV3NarrowestType(38,
                    ((ScalarType) argTypes[0]).getScalarScale()));
        }
        return newFn;
    }

    public static boolean isSimpleLike(ScalarOperator op) {
        return Utils.downcast(op, LikePredicateOperator.class)
                .map(likeOp -> !likeOp.isRegexp() &&
                        likeOp.getChild(0).isColumnRef() &&
                        likeOp.getChild(1).isConstantRef())
                .orElse(false);
    }

    public static boolean isSimpleNotLike(ScalarOperator op) {
        return Utils.downcast(op, CompoundPredicateOperator.class)
                .map(compOp -> compOp.isNot() && isSimpleLike(compOp.getChild(0)))
                .orElse(false);
    }

    public static Stream<ScalarOperator> getStream(ScalarOperator operator) {
        return Stream.concat(Stream.of(operator),
                operator.getChildren().stream().flatMap(ScalarOperatorUtil::getStream));
    }

    /**
     * Compacts a common sub-operator map by removing unnecessary entries and inlining trivial ones.
     *
     * <p>This method performs three transformations on {@code commonSubOperatorMap} in order:
     * <ol>
     *   <li><b>Cleanup:</b> removes self-referencing entries (key == value).</li>
     *   <li><b>Dead-column elimination:</b> computes the transitive closure of column refs
     *       reachable from {@code columnRefMap} via BFS and removes any entries from
     *       {@code commonSubOperatorMap} that are not reachable.</li>
     *   <li><b>Inlining:</b> entries whose values are constant or are referenced exactly once
     *       across both maps are substituted directly into the expressions that use them and
     *       then removed from {@code commonSubOperatorMap}.</li>
     * </ol>
     *
     * <p>Both maps are modified in place.
     *
     * @param columnRefMap         output column refs mapped to their defining scalar expressions
     * @param commonSubOperatorMap common sub-expression column refs mapped to their definitions;
     *                             entries may be removed or have their values rewritten
     */
    public static void compactCommonSubOperatorMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                   Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        if (commonSubOperatorMap.isEmpty()) {
            return;
        }

        Map<ColumnRefOperator, Integer> refCounts = new HashMap<>(commonSubOperatorMap.size());
        for (Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> it = commonSubOperatorMap.entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry<ColumnRefOperator, ScalarOperator> entry = it.next();
            if (entry.getKey().equals(entry.getValue())) {
                it.remove();
            } else {
                refCounts.put(entry.getKey(), 0);
            }
        }
        for (ScalarOperator expr : columnRefMap.values()) {
            countRefs(expr, refCounts, commonSubOperatorMap);
        }

        Map<ColumnRefOperator, ScalarOperator> inlineMap = new HashMap<>();
        for (Iterator<Map.Entry<ColumnRefOperator, ScalarOperator>> it = commonSubOperatorMap.entrySet().iterator();
                it.hasNext(); ) {
            Map.Entry<ColumnRefOperator, ScalarOperator> entry = it.next();
            int count = refCounts.getOrDefault(entry.getKey(), 0);
            if (count == 0) {
                it.remove();
            } else if (entry.getValue().isConstant() || count == 1) {
                inlineMap.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }

        if (!inlineMap.isEmpty()) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(inlineMap, true);
            ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();

            columnRefMap.replaceAll((k, v) -> {
                ScalarOperator result = rewriter.rewrite(v);
                if (result.isConstant()) {
                    result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                }
                return result;
            });

            commonSubOperatorMap.replaceAll((k, v) -> {
                ScalarOperator result = rewriter.rewrite(v);
                if (result.isConstant()) {
                    result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                }
                return result;
            });
        }
    }

    /**
     * Eliminates the common sub-expression map by inlining all of its definitions into
     * {@code columnRefMap} and then clearing the map. After this call, every value in
     * {@code columnRefMap} is self-contained (no references to common sub-expression column
     * refs remain) and {@code commonSubOperatorMap} is empty.
     *
     * <p>Constant-foldable results are simplified via {@link ScalarOperatorRewriter}.
     *
     * @param columnRefMap         map of output column refs to their scalar expressions;
     *                             updated in place with inlined definitions
     * @param commonSubOperatorMap map of common sub-expression column refs to their
     *                             definitions; cleared after all entries are inlined
     */
    public static void eliminateCommonSubOperatorMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                     Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        if (commonSubOperatorMap.isEmpty()) {
            return;
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(commonSubOperatorMap, true);
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        columnRefMap.replaceAll((k, v) -> {
            ScalarOperator result = rewriter.rewrite(v);
            if (result.isConstant()) {
                result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            }
            return result;
        });
        commonSubOperatorMap.clear();
    }

    /**
     * Returns the entries of a common sub-operator map in dependency order: entries that are
     * depended upon appear before the entries that reference them. When iterating the result,
     * every entry's dependencies are guaranteed to have been seen already.
     *
     * @param commonSubOperatorMap common sub-expression column refs mapped to their definitions
     * @return list of (columnRef, scalarOperator) entries in topological (dependency) order
     * @throws IllegalStateException if a cycle exists among the entries
     */
    public static List<Map.Entry<ColumnRefOperator, ScalarOperator>> topologicalSortCommonSubOperatorMap(
            Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        if (commonSubOperatorMap.isEmpty()) {
            return new ArrayList<>();
        }
        int size = commonSubOperatorMap.size();
        ColumnRefSet keyIds = new ColumnRefSet(commonSubOperatorMap.keySet());
        Map<Integer, ColumnRefOperator> idToKey = new HashMap<>(size);
        for (ColumnRefOperator key : commonSubOperatorMap.keySet()) {
            idToKey.put(key.getId(), key);
        }

        Map<ColumnRefOperator, Integer> inDegree = new HashMap<>(size);
        Map<ColumnRefOperator, List<ColumnRefOperator>> dependents = new HashMap<>();

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : commonSubOperatorMap.entrySet()) {
            ColumnRefOperator ref = entry.getKey();
            int refId = ref.getId();
            ColumnRefSet depIds = entry.getValue().getUsedColumns();
            int depCount = 0;
            for (int depId : depIds.getColumnIds()) {
                if (depId != refId && keyIds.contains(depId)) {
                    depCount++;
                    dependents.computeIfAbsent(idToKey.get(depId), k -> new ArrayList<>()).add(ref);
                }
            }
            inDegree.put(ref, depCount);
        }

        Deque<ColumnRefOperator> queue = new ArrayDeque<>();
        for (Map.Entry<ColumnRefOperator, Integer> e : inDegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }

        List<Map.Entry<ColumnRefOperator, ScalarOperator>> result = new ArrayList<>(size);
        while (!queue.isEmpty()) {
            ColumnRefOperator n = queue.remove();
            result.add(Maps.immutableEntry(n, commonSubOperatorMap.get(n)));
            for (ColumnRefOperator m : dependents.getOrDefault(n, Collections.emptyList())) {
                inDegree.merge(m, -1, Integer::sum);
                if (inDegree.get(m) == 0) {
                    queue.add(m);
                }
            }
        }

        if (result.size() != commonSubOperatorMap.size()) {
            throw new IllegalStateException(
                    "Cycle in common sub operator map: dependency graph has a cycle");
        }
        return result;
    }

    /**
     * Returns the set of column refs that are used as inputs by the given column and common
     * sub-expression maps, excluding refs that are defined locally in {@code commonSubOperatorMap}.
     *
     * <p>That is, this is the set of "external" columns the expressions depend on (e.g. from the
     * child operator), not the column refs that are defined by {@code commonSubOperatorMap} itself.
     *
     * @param columnRefMap         map of output column refs to their scalar expressions
     * @param commonSubOperatorMap map of common sub-expression column refs to their definitions
     *                             (keys are excluded from the result)
     * @return set of column ref IDs that are used as inputs
     */
    public static ColumnRefSet getUsedInputColumns(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                   Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        ColumnRefSet result = new ColumnRefSet();
        columnRefMap.values().forEach(e -> result.union(e.getUsedColumns()));
        commonSubOperatorMap.values().forEach(e -> result.union(e.getUsedColumns()));
        commonSubOperatorMap.keySet().forEach(e -> result.except(e.getUsedColumns()));
        return result;
    }

    /**
     * Returns the set of "external" input column refs that {@code scalarOperator} transitively depends on,
     * resolving references through the common sub-expression map via BFS.
     *
     * <p>Column refs that appear as keys in {@code commonSubOperatorById} are considered locally
     * defined and are expanded into their constituent columns. Column refs <em>not</em> in the map
     * are treated as true inputs (e.g. columns from a child operator) and included in the result.
     *
     * @param scalarOperator        the scalar expression to analyze
     * @param commonSubOperatorById map from column ref ID to its defining sub-expression;
     *                              keys are local definitions that will be expanded, not returned
     * @return a fresh {@link ColumnRefSet} containing only the externally-supplied column ref IDs
     */
    public static ColumnRefSet getUsedInputColumns(ScalarOperator scalarOperator,
                                                   Map<Integer, ScalarOperator> commonSubOperatorById) {
        if (commonSubOperatorById.isEmpty()) {
            return scalarOperator.getUsedColumns();
        }
        ColumnRefSet result = new ColumnRefSet();
        ColumnRefSet visited = new ColumnRefSet();
        Deque<Integer> queue = new ArrayDeque<>();
        for (int id : scalarOperator.getUsedColumns().getColumnIds()) {
            visited.union(id);
            queue.add(id);
        }

        while (!queue.isEmpty()) {
            int colId = queue.remove();
            ScalarOperator subExpr = commonSubOperatorById.get(colId);
            if (subExpr != null) {
                for (int depId : subExpr.getUsedColumns().getColumnIds()) {
                    if (!visited.contains(depId)) {
                        visited.union(depId);
                        queue.add(depId);
                    }
                }
            } else {
                result.union(colId);
            }
        }
        return result;
    }

    private static void countRefs(ScalarOperator op, Map<ColumnRefOperator, Integer> refCounts,
                                  Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        if (op.isColumnRef()) {
            ColumnRefOperator colRef = (ColumnRefOperator) op;
            Integer oldCount = refCounts.get(colRef);
            if (oldCount != null) {
                refCounts.put(colRef, oldCount + 1);
                if (oldCount == 0) {
                    countRefs(commonSubOperatorMap.get(colRef), refCounts, commonSubOperatorMap);
                }
            }
        } else {
            for (ScalarOperator child : op.getChildren()) {
                countRefs(child, refCounts, commonSubOperatorMap);
            }
        }
    }
}
