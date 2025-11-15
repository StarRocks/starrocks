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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/*
 * e.g.
 *    select a, b, count(c) as cc, sum(d)
 *    from t group by a, b
 *    order by cc limit 10
 *
 * -> select t.a, t.b, cc, sum(d)
 *    from t join (select a, b, count(c) as cc from t group by a, b order by cc limit 10) t1 on t.a = t1.a and t.b = t1.b
 *    group by t.a, t.b, cc
 *    order by cc limit 10
 */
public class SplitTopNAggregateRule extends TransformationRule {
    public SplitTopNAggregateRule() {
        super(RuleType.TF_SPLIT_TOPN_AGGREGATE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableSplitTopNAgg()) {
            return false;
        }

        LogicalTopNOperator topN = input.getOp().cast();
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        LogicalOlapScanOperator scan = input.inputAt(0).inputAt(0).getOp().cast();

        if (topN.getLimit() == Operator.DEFAULT_LIMIT
                || topN.getLimit() > context.getSessionVariable().getSplitTopNAggLimit()) {
            return false;
        }
        if (scan.getProjection() != null) {
            if (scan.getProjection().getColumnRefMap().entrySet()
                    .stream().anyMatch(e -> !e.getKey().equals(e.getValue()))) {
                return false;
            }
        }

        if (agg.getProjection() != null) {
            if (agg.getProjection().getColumnRefMap().entrySet()
                    .stream().anyMatch(e -> !e.getKey().equals(e.getValue()))) {
                return false;
            }
        }

        Statistics ss = input.inputAt(0).getStatistics();
        if (!FeConstants.runningUnitTest && ss != null && ss.getOutputRowCount() < (topN.getLimit() * 10)
                && ss.getColumnStatistics().values().stream().noneMatch(ColumnStatistic::isUnknown)) {
            return false;
        }

        // Get scan statistics for checking column average row size
        Statistics scanStatistics = input.inputAt(0).inputAt(0).getStatistics();

        // Calculate columns that will be read twice (once in each scan)
        List<ColumnRefOperator> orderByRefs = topN.getOrderByElements().stream().map(Ordering::getColumnRef).toList();
        List<ColumnRefOperator> splitAggregations = splitTopNAggregations(agg, orderByRefs);
        if (splitAggregations.isEmpty()) {
            return false;
        }

        // First scan (join right side) needs: groupingKeys + splitAggregations used columns + predicate columns
        ColumnRefSet firstScanColumns = new ColumnRefSet();
        firstScanColumns.union(agg.getGroupingKeys());
        splitAggregations.forEach(c -> firstScanColumns.union(agg.getAggregations().get(c).getUsedColumns()));
        if (scan.getPredicate() != null) {
            firstScanColumns.union(scan.getPredicate().getUsedColumns());
        }

        // Check predicate and column type constraints only for duplicated columns
        if (!checkPredicateAndColumnConstraints(scan, scanStatistics, firstScanColumns)) {
            return false;
        }

        return true;
    }

    private boolean checkPredicateAndColumnConstraints(LogicalOlapScanOperator scan, Statistics scanStatistics,
                                                       ColumnRefSet duplicatedColumns) {
        // if read too much repeated columns, scan's extra costs may bigger then agg's
        if (duplicatedColumns.size() > 3) {
            return false;
        }

        if (hasLongStringOrComplexType(scan, duplicatedColumns, scanStatistics)) {
            return false;
        }

        ScalarOperator predicate = scan.getPredicate();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> disconjuncts = Utils.extractDisjunctive(predicate);

        // if evaluate too much predicates, scan's extra costs may bigger then agg's
        if (conjuncts.size() > 2 || disconjuncts.size() > 2) {
            return false;
        }

        return true;
    }

    /**
     * Check if the duplicated columns have long string or complex type columns.
     * Long string is determined by averageRowSize > 5 or no statistics.
     */
    private boolean hasLongStringOrComplexType(LogicalOlapScanOperator scan, ColumnRefSet duplicatedColumns,
                                               Statistics scanStatistics) {
        for (ColumnRefOperator colRef : scan.getColRefToColumnMetaMap().keySet()) {
            if (!duplicatedColumns.contains(colRef)) {
                continue;
            }

            Type colType = colRef.getType();

            if (colType.isStringType()) {
                if (isLongString(colRef, scanStatistics)) {
                    return true;
                }
            }

            if (isComplexType(colType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if a string column is a long string based on statistics.
     * Returns true if:
     * - No statistics available, OR
     * - averageRowSize >= 5
     */
    private boolean isLongString(ColumnRefOperator colRef, Statistics scanStatistics) {
        if (scanStatistics == null) {
            // No statistics: treat as long string to be safe
            return true;
        }

        ColumnStatistic columnStatistic = scanStatistics.getColumnStatistics().get(colRef);
        if (columnStatistic == null || columnStatistic.isUnknown()) {
            // No statistics for this column: treat as long string to be safe
            return true;
        }

        double averageRowSize = columnStatistic.getAverageRowSize();
        return averageRowSize >= 5;
    }

    /**
     * Check if a type is complex type (ARRAY, MAP, STRUCT, JSON, VARIANT).
     */
    private boolean isComplexType(Type type) {
        return type.isComplexType() || type.isJsonType() || type.isVariantType();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        LogicalOlapScanOperator scan = input.inputAt(0).inputAt(0).getOp().cast();
        ColumnRefFactory factory = context.getColumnRefFactory();

        List<ColumnRefOperator> orderByRefs = topN.getOrderByElements().stream().map(Ordering::getColumnRef).toList();

        List<ColumnRefOperator> splitAggregations = splitTopNAggregations(agg, orderByRefs);
        if (splitAggregations.isEmpty()) {
            return List.of();
        }

        // build join right
        ColumnRefSet newAggUseRefs = new ColumnRefSet();
        newAggUseRefs.union(agg.getGroupingKeys());
        splitAggregations.forEach(c -> newAggUseRefs.union(agg.getAggregations().get(c).getUsedColumns()));

        if (scan.getPredicate() != null) {
            newAggUseRefs.union(scan.getPredicate().getUsedColumns());
        }
        Map<ColumnRefOperator, ColumnRefOperator> refToNew = Maps.newHashMap();

        newAggUseRefs.getStream().forEach(k -> {
            ColumnRefOperator v = factory.getColumnRef(k);
            ColumnRefOperator n = factory.create(v.getName(), v.getType(), v.isNullable());
            refToNew.put(v, n);
        });

        OptExpression right = buildTopNAggregatePlan(input, refToNew, splitAggregations);

        // build join left
        ColumnRefSet newScanUseRefs = new ColumnRefSet();
        newScanUseRefs.union(agg.getGroupingKeys());
        agg.getAggregations().forEach((k, v) -> {
            if (!splitAggregations.contains(k)) {
                newScanUseRefs.union(v.getUsedColumns());
            }
        });

        Map<ColumnRefOperator, ScalarOperator> scanProjections = Maps.newHashMap();
        newScanUseRefs.getStream().forEach(k -> scanProjections.put(factory.getColumnRef(k), factory.getColumnRef(k)));

        LogicalOlapScanOperator newScan = LogicalOlapScanOperator.builder()
                .withOperator(scan)
                .setProjection(new Projection(scanProjections))
                .build();

        // build join
        List<ScalarOperator> eqPredicate = Lists.newArrayList();
        for (ColumnRefOperator groupingKey : agg.getGroupingKeys()) {
            eqPredicate.add(new BinaryPredicateOperator(BinaryType.EQ, groupingKey, refToNew.get(groupingKey)));
        }

        LogicalJoinOperator join = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(eqPredicate))
                .build();

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        agg.getAggregations().forEach((k, v) -> {
            if (!splitAggregations.contains(k)) {
                newAggregations.put(k, v);
            }
        });

        List<ColumnRefOperator> newGroupBy = Lists.newArrayList(agg.getGroupingKeys());
        newGroupBy.addAll(splitAggregations);
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setAggregations(newAggregations)
                .setGroupingKeys(newGroupBy)
                .setPartitionByColumns(newGroupBy)
                .build();

        return List.of(OptExpression.create(topN,
                OptExpression.create(newAgg, OptExpression.create(join, OptExpression.create(newScan), right))));
    }

    private OptExpression buildTopNAggregatePlan(OptExpression input, Map<ColumnRefOperator, ColumnRefOperator> refMapping,
                                                 List<ColumnRefOperator> topNAggregations) {
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        LogicalOlapScanOperator scan = input.inputAt(0).inputAt(0).getOp().cast();

        Map<ColumnRefOperator, Column> refToMeta = Maps.newHashMap();
        Map<Column, ColumnRefOperator> metaToRef = Maps.newHashMap();
        Projection scanProjection = null;

        scan.getColumnMetaToColRefMap().forEach((k, v) -> {
            if (refMapping.containsKey(v)) {
                metaToRef.put(k, refMapping.get(v));
                refToMeta.put(refMapping.get(v), k);
            }
        });

        if (scan.getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> map = Maps.newHashMap();
            for (ColumnRefOperator k : scan.getProjection().getColumnRefMap().keySet()) {
                if (refMapping.containsKey(k)) {
                    Preconditions.checkState(k.equals(scan.getProjection().getColumnRefMap().get(k)));
                    map.put(refMapping.get(k), refMapping.get(k));
                }
            }
            scanProjection = new Projection(map);
        }

        ReplaceColumnRefRewriter refRewriter = new ReplaceColumnRefRewriter(refMapping);
        LogicalOlapScanOperator newScan = LogicalOlapScanOperator.builder()
                .withOperator(scan)
                .setProjection(scanProjection)
                .setColRefToColumnMetaMap(refToMeta)
                .setColumnMetaToColRefMap(metaToRef)
                .setPredicate(refRewriter.rewrite(scan.getPredicate()))
                .build();

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        for (ColumnRefOperator aggRef : topNAggregations) {
            newAggregations.put(aggRef, (CallOperator) refRewriter.rewrite(agg.getAggregations().get(aggRef)));
        }

        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setPartitionByColumns(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setAggregations(newAggregations)
                .setPredicate(refRewriter.rewrite(agg.getPredicate()))
                .setProjection(null)
                .build();

        List<Ordering> newOrdering = Lists.newArrayList();
        for (Ordering o : topN.getOrderByElements()) {
            if (refMapping.containsKey(o.getColumnRef())) {
                newOrdering.add(new Ordering(refMapping.get(o.getColumnRef()), o.isAscending(), o.isNullsFirst()));
            } else {
                newOrdering.add(o);
            }
        }

        LogicalTopNOperator newTopN = LogicalTopNOperator.builder()
                .withOperator(topN)
                .setOrderByElements(newOrdering)
                .setProjection(null)
                .build();

        return OptExpression.create(newTopN, OptExpression.create(newAgg, OptExpression.create(newScan)));
    }

    private List<ColumnRefOperator> splitTopNAggregations(LogicalAggregationOperator agg,
                                                          List<ColumnRefOperator> orderByRefs) {
        ColumnRefSet usedColumnRefs = new ColumnRefSet(orderByRefs);
        if (agg.getPredicate() != null) {
            usedColumnRefs.union(agg.getPredicate().getUsedColumns());
        }

        List<ColumnRefOperator> hitAggregations = Lists.newArrayList();
        agg.getAggregations().forEach((k, v) -> {
            if (usedColumnRefs.contains(k)) {
                hitAggregations.add(k);
            }
        });
        if (hitAggregations.isEmpty() || hitAggregations.size() == agg.getAggregations().size()) {
            return List.of();
        }
        return hitAggregations;
    }
}
