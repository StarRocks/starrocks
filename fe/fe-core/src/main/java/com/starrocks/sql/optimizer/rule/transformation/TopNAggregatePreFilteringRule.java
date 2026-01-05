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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Before:
 *    select a, b, count(c) as cc, sum(d)
 *    from (subquery)
 *    group by a, b
 *    order by a, b limit 10
 * After:
 * if subquery is a table scan:
 * select t0.a, t0.b, count(c) as cc, sum(d)
 *    from t0 join (select a, b from t0 group by a, b  order by a, b limit 10) t1 on t0.a = t1.a and t0.b = t1.b
 *    group by t.a, t.b
 *    order by t.a, t.b limit 10;
 * Otherwise:
 * with cte as (select * from subquery)
 * select t0.a, t0.b, count(c) as cc, sum(d)
 *    from cte as t0 join (select a, b from subquery group by a, b order by a, b limit 10) t1 on t0.a = t1.a and t0.b = t1.b
 *    group by t.a, t.b
 *    order by t.a, t.b limit 10;
 */
public class TopNAggregatePreFilteringRule extends TransformationRule {
    public TopNAggregatePreFilteringRule() {
        super(RuleType.TF_SPLIT_TOPN_AGGREGATE_TO_JOIN_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        int topNAggPreFilteringMode = context.getSessionVariable().getTopNAggPreFilteringMode();
        // -1: disable, 0: auto, 1: enable
        if (topNAggPreFilteringMode < 0) {
            return false;
        }

        LogicalTopNOperator topN = input.getOp().cast();
        if (topN.isTopNPushDownAgg()) {
            return false;
        }
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        OptExpression child = input.inputAt(0).inputAt(0);

        // NOTE: those conditions must be met, so we can do pre-filtering
        if (topN.getLimit() == Operator.DEFAULT_LIMIT || !isIdentityProjection(agg.getProjection())
                || !orderByMatchesGroupingKeys(topN, agg) || agg.getGroupingKeys().isEmpty()
                || agg.getAggregations().isEmpty() || agg.getPredicate() != null) {
            return false;
        }
        if (child.getOp() instanceof LogicalOlapScanOperator scan) {
            if (!isIdentityProjection(scan.getProjection())) {
                return false;
            }
        }
        // if user force enable this rule
        if (topNAggPreFilteringMode > 0) {
            return true;
        }

        // Only apply this rule when:
        // - Not running unit test (to avoid affecting test coverage)
        // - Statistics is available
        Statistics ss = input.inputAt(0).getStatistics();
        if (!isEnableTopNAggregatePreFiltering(ss, topN, agg, child)) {
            return false;
        }
        return true;
    }

    private boolean isEnableTopNAggregatePreFiltering(Statistics ss,
                                                      LogicalTopNOperator topN,
                                                      LogicalAggregationOperator agg,
                                                      OptExpression childOpt) {
        if (ss == null) {
            return false;
        }
        // if any column statistics is unknown, we don't use pre-filtering since it may generate more overhead
        if (!FeConstants.runningUnitTest && ss.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown)) {
            return false;
        }
        // if output rows less than 10 million or its output rows are less limit * 100, we don't use pre-filtering since it may
        // generate more overhead
        if (!FeConstants.runningUnitTest &&
                (ss.getOutputRowCount() < 10_000_000 || ss.getOutputRowCount() < (topN.getLimit() * 100))) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalAggregationOperator agg = input.inputAt(0).getOp().cast();
        OptExpression child = input.inputAt(0).inputAt(0);
        ColumnRefFactory factory = context.getColumnRefFactory();
        // mark topN has been push down agg
        topN.setTopNPushDownAgg();
        // for input is a table scan, we can do more precise column pruning
        if (child.getOp() instanceof LogicalOlapScanOperator scan) {
            return rewriteWithScan(topN, agg, scan, factory);
        }
        return rewriteWithCTE(input, topN, agg, context);
    }

    private boolean orderByMatchesGroupingKeys(LogicalTopNOperator topN, LogicalAggregationOperator agg) {
        List<Ordering> orderByElements = topN.getOrderByElements();
        if (orderByElements.size() != agg.getGroupingKeys().size()) {
            return false;
        }
        for (int i = 0; i < orderByElements.size(); i++) {
            if (!orderByElements.get(i).getColumnRef().equals(agg.getGroupingKeys().get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean isIdentityProjection(Projection projection) {
        if (projection == null) {
            return true;
        }
        return projection.getColumnRefMap().entrySet()
                .stream()
                .allMatch(e -> e.getKey().equals(e.getValue()));
    }

    private List<OptExpression> rewriteWithScan(LogicalTopNOperator topN, LogicalAggregationOperator agg,
                                                LogicalOlapScanOperator scan, ColumnRefFactory factory) {
        ColumnRefSet rightRequired = new ColumnRefSet();
        rightRequired.union(agg.getGroupingKeys());
        if (scan.getPredicate() != null) {
            rightRequired.union(scan.getPredicate().getUsedColumns());
        }

        Map<ColumnRefOperator, ColumnRefOperator> refToNew = buildRefMapping(factory, rightRequired);
        OptExpression right = buildTopNAggregatePlan(topN, agg, scan, refToNew);

        ColumnRefSet leftRequired = new ColumnRefSet();
        leftRequired.union(agg.getGroupingKeys());
        agg.getAggregations().values().forEach(v -> leftRequired.union(v.getUsedColumns()));
        if (scan.getPredicate() != null) {
            leftRequired.union(scan.getPredicate().getUsedColumns());
        }

        LogicalOlapScanOperator newScan = LogicalOlapScanOperator.builder()
                .withOperator(scan)
                .setProjection(new Projection(buildIdentityScalarMap(factory, leftRequired)))
                .build();

        List<ScalarOperator> eqPredicate = Lists.newArrayList();
        for (ColumnRefOperator groupingKey : agg.getGroupingKeys()) {
            eqPredicate.add(new BinaryPredicateOperator(BinaryType.EQ, groupingKey, refToNew.get(groupingKey)));
        }

        LogicalJoinOperator join = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(eqPredicate))
                .build();

        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setAggregations(agg.getAggregations())
                .setGroupingKeys(agg.getGroupingKeys())
                .setPartitionByColumns(getPartitionColumns(agg))
                .build();

        return List.of(OptExpression.create(topN,
                OptExpression.create(newAgg, OptExpression.create(join, OptExpression.create(newScan), right))));
    }

    /**
     * If the child is not a scan, we use CTE to avoid duplicate computing.
     */
    private List<OptExpression> rewriteWithCTE(OptExpression input, LogicalTopNOperator topN,
                                               LogicalAggregationOperator agg, OptimizerContext context) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        int cteId = context.getCteContext().getNextCteId();
        context.getCteContext().addForceCTE(cteId);

        OptExpression child = input.inputAt(0).inputAt(0);
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), child);

        // Build right child that only needs grouping keys
        ColumnRefSet rightRequired = new ColumnRefSet();
        rightRequired.union(agg.getGroupingKeys());
        Map<ColumnRefOperator, ColumnRefOperator> rightConsumeOutput = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> refMapping = Maps.newHashMap();
        rightRequired.getStream().forEach(id -> {
            ColumnRefOperator origin = factory.getColumnRef(id);
            ColumnRefOperator newRef = factory.create(origin.getName(), origin.getType(), origin.isNullable());
            rightConsumeOutput.put(newRef, origin);
            refMapping.put(origin, newRef);
        });
        LogicalCTEConsumeOperator rightConsume = new LogicalCTEConsumeOperator(cteId, rightConsumeOutput);
        rightConsume.setProjection(new Projection(buildIdentityScalarMap(rightConsumeOutput.keySet())));
        OptExpression right = buildTopNOnConsume(topN, agg, OptExpression.create(rightConsume), refMapping);

        // Build left child for final aggregation
        ColumnRefSet leftRequired = new ColumnRefSet();
        leftRequired.union(agg.getGroupingKeys());
        agg.getAggregations().values().forEach(v -> leftRequired.union(v.getUsedColumns()));

        Map<ColumnRefOperator, ColumnRefOperator> leftConsumeOutput = Maps.newHashMap();
        leftRequired.getStream().forEach(id -> {
            ColumnRefOperator origin = factory.getColumnRef(id);
            leftConsumeOutput.put(origin, origin);
        });
        LogicalCTEConsumeOperator leftConsume = new LogicalCTEConsumeOperator(cteId, leftConsumeOutput);
        leftConsume.setProjection(new Projection(buildIdentityScalarMap(leftConsumeOutput.keySet())));

        List<ScalarOperator> eqPredicate = Lists.newArrayList();
        for (ColumnRefOperator groupingKey : agg.getGroupingKeys()) {
            eqPredicate.add(new BinaryPredicateOperator(BinaryType.EQ, groupingKey, refMapping.get(groupingKey)));
        }

        LogicalJoinOperator join = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(eqPredicate))
                .build();
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setAggregations(agg.getAggregations())
                .setGroupingKeys(agg.getGroupingKeys())
                .setPartitionByColumns(getPartitionColumns(agg))
                .build();

        OptExpression mainPlan = OptExpression.create(topN,
                OptExpression.create(newAgg, OptExpression.create(join, OptExpression.create(leftConsume), right)));
        OptExpression anchor = OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, mainPlan);
        return List.of(anchor);
    }

    private Map<ColumnRefOperator, ColumnRefOperator> buildRefMapping(ColumnRefFactory factory,
                                                                      ColumnRefSet required) {
        Map<ColumnRefOperator, ColumnRefOperator> refToNew = Maps.newHashMap();
        required.getStream().forEach(k -> {
            ColumnRefOperator v = factory.getColumnRef(k);
            ColumnRefOperator n = factory.create(v.getName(), v.getType(), v.isNullable());
            refToNew.put(v, n);
        });
        return refToNew;
    }

    private Map<ColumnRefOperator, ScalarOperator> buildIdentityScalarMap(ColumnRefFactory factory,
                                                                          ColumnRefSet required) {
        return required.getStream()
                .map(k -> {
                    ColumnRefOperator col = factory.getColumnRef(k);
                    return Map.entry(col, col);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<ColumnRefOperator, ScalarOperator> buildIdentityScalarMap(Collection<ColumnRefOperator> columnRefs) {
        return columnRefs.stream()
                .map(col -> Map.entry(col, col))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<ColumnRefOperator> getPartitionColumns(LogicalAggregationOperator agg) {
        if (agg.getPartitionByColumns() == null || agg.getPartitionByColumns().isEmpty()) {
            return agg.getGroupingKeys();
        }
        return agg.getPartitionByColumns();
    }

    private OptExpression buildTopNAggregatePlan(LogicalTopNOperator topN, LogicalAggregationOperator agg,
                                                 LogicalOlapScanOperator scan,
                                                 Map<ColumnRefOperator, ColumnRefOperator> refMapping) {
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

        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setPartitionByColumns(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setAggregations(Map.of())
                .setPredicate(refRewriter.rewrite(agg.getPredicate()))
                .setProjection(null)
                .build();

        List<Ordering> newOrdering = Lists.newArrayList();
        for (Ordering o : topN.getOrderByElements()) {
            newOrdering.add(new Ordering(refMapping.get(o.getColumnRef()), o.isAscending(), o.isNullsFirst()));
        }

        LogicalTopNOperator newTopN = LogicalTopNOperator.builder()
                .withOperator(topN)
                .setOrderByElements(newOrdering)
                .setProjection(null)
                .setTopNPushDownAgg()
                .build();

        return OptExpression.create(newTopN, OptExpression.create(newAgg, OptExpression.create(newScan)));
    }

    private OptExpression buildTopNOnConsume(LogicalTopNOperator topN, LogicalAggregationOperator agg,
                                             OptExpression consume,
                                             Map<ColumnRefOperator, ColumnRefOperator> refMapping) {
        ReplaceColumnRefRewriter refRewriter = new ReplaceColumnRefRewriter(refMapping);
        // new Agg
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setPartitionByColumns(agg.getGroupingKeys().stream().map(refMapping::get).toList())
                .setAggregations(Map.of())
                .setPredicate(refRewriter.rewrite(agg.getPredicate()))
                .setProjection(null)
                .build();
        // new topN
        List<Ordering> newOrdering = Lists.newArrayList();
        for (Ordering o : topN.getOrderByElements()) {
            newOrdering.add(new Ordering(refMapping.get(o.getColumnRef()), o.isAscending(), o.isNullsFirst()));
        }
        LogicalTopNOperator newTopN = LogicalTopNOperator.builder()
                .withOperator(topN)
                .setOrderByElements(newOrdering)
                .setProjection(null)
                .setTopNPushDownAgg()
                .build();
        return OptExpression.create(newTopN, OptExpression.create(newAgg, consume));
    }
}
