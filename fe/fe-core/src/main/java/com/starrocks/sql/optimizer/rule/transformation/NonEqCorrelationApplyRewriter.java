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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;

/*
 * Decorrelates an Apply whose correlation predicate is not a conjunction of equalities, e.g.
 *
 *   SELECT t0.v1, (SELECT count(*) FROM t1 WHERE t1.v4 = t0.v1 AND t1.v5 < t0.v2) FROM t0
 *
 * The inner rows cannot be pre-aggregated by a join key, because the non-equality predicate also depends on the
 * outer row. Instead, the subquery is evaluated once per distinct value of the outer columns it references (the
 * "domain" of the correlation), and the result is joined back to the outer rows:
 *
 *   with cte as (select * from t0)
 *   select cte.*, ifnull(r.cnt, 0)
 *   from cte left outer join (
 *        select d.v1, d.v2, count(*) cnt
 *        from (select distinct v1, v2 from cte) d inner join t1 on t1.v4 = d.v1 and t1.v5 < d.v2
 *        group by d.v1, d.v2) r
 *     on cte.v1 <=> r.v1 and cte.v2 <=> r.v2
 *
 *                            CTEAnchor
 *                           /         \
 *                    CTEProduce      Project(outer columns, apply output)
 *                        |                 \
 *                      outer         Left-Outer-Join(outer refs <=> domain refs)
 *                                      /          \
 *                                CTEConsume     Aggregate(group by domain refs, aggregations)
 *                                                    \
 *                                                  Inner-Join(correlation predicate on domain refs)
 *                                                  /        \
 *                                   Aggregate(distinct)    Filter(un-correlation)[optional]
 *                                            |               \
 *                                       CTEConsume          inner
 *
 * The rewrite is exact: the correlation predicate reads only the domain columns of the outer row, so evaluating it
 * on the distinct domain row gives the same result as evaluating it on the outer row, and the null-safe join back
 * matches every outer row with exactly one domain row (or none, if no inner row qualified).
 */
public class NonEqCorrelationApplyRewriter {
    private final OptimizerContext context;
    private final ColumnRefFactory factory;
    private final OptExpression input;
    private final LogicalApplyOperator apply;

    public NonEqCorrelationApplyRewriter(OptExpression input, OptimizerContext context) {
        this.input = input;
        this.context = context;
        this.factory = context.getColumnRefFactory();
        this.apply = input.getOp().cast();
    }

    /**
     * @param inner                  the subquery input, below the correlation filter
     * @param correlationPredicate   predicates that reference outer columns
     * @param unCorrelationPredicate predicates on the subquery input only, may be null
     * @param aggregations           aggregations computed per domain row, over the qualifying inner rows. They must
     *                               only reference columns of {@code inner}.
     * @param applyOutput            the value of the apply output, over the aggregation outputs. It is evaluated after
     *                               the outer join, so it sees NULL aggregations when no inner row qualified.
     * @param extraOutputs           additional projections over the aggregation outputs, e.g. a row-count assertion
     */
    public OptExpression rewrite(OptExpression inner, ScalarOperator correlationPredicate,
                                 ScalarOperator unCorrelationPredicate,
                                 Map<ColumnRefOperator, CallOperator> aggregations,
                                 ScalarOperator applyOutput,
                                 Map<ColumnRefOperator, ScalarOperator> extraOutputs) {
        if (!context.getSessionVariable().isEnablePipelineEngine()) {
            throw new SemanticException("Correlated subquery with Non-EQ predicate depends on pipeline engine, " +
                    "enable by: set enable_pipeline_engine=true;");
        }

        OptExpression outer = input.inputAt(0);
        ColumnRefSet outerColumns = outer.getOutputColumns();

        // the outer columns referenced by the correlation predicate
        List<ColumnRefOperator> domainRefs = Lists.newArrayList();
        ColumnRefSet usedColumns = correlationPredicate.getUsedColumns();
        for (ColumnRefOperator ref : apply.getCorrelationColumnRefs()) {
            if (usedColumns.contains(ref) && !domainRefs.contains(ref)) {
                domainRefs.add(ref);
            }
        }
        if (domainRefs.isEmpty() || !domainRefs.stream().allMatch(outerColumns::contains)) {
            throw new SemanticException("Not support correlated subquery whose correlation columns are not " +
                    "produced by the outer query");
        }

        int cteId = context.getCteContext().getNextCteId();
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), outer);

        // outer side: consume every outer column
        Map<ColumnRefOperator, ColumnRefOperator> outerConsumeMap = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> outerToConsume = Maps.newHashMap();
        for (int id : outerColumns.getColumnIds()) {
            ColumnRefOperator ref = factory.getColumnRef(id);
            ColumnRefOperator consumeRef = factory.create(ref, ref.getType(), ref.isNullable());
            outerConsumeMap.put(consumeRef, ref);
            outerToConsume.put(ref, consumeRef);
        }
        OptExpression outerConsume = OptExpression.create(new LogicalCTEConsumeOperator(cteId, outerConsumeMap));

        // domain side: distinct correlation columns
        Map<ColumnRefOperator, ColumnRefOperator> domainConsumeMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> outerToDomain = Maps.newHashMap();
        List<ColumnRefOperator> domainKeys = Lists.newArrayList();
        for (ColumnRefOperator ref : domainRefs) {
            ColumnRefOperator domainRef = factory.create(ref, ref.getType(), ref.isNullable());
            domainConsumeMap.put(domainRef, ref);
            outerToDomain.put(ref, domainRef);
            domainKeys.add(domainRef);
        }
        OptExpression domain = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(domainKeys), Maps.newHashMap()),
                OptExpression.create(new LogicalCTEConsumeOperator(cteId, domainConsumeMap)));

        // evaluate the subquery once per domain row
        OptExpression innerChild = inner;
        if (unCorrelationPredicate != null) {
            innerChild = OptExpression.create(new LogicalFilterOperator(unCorrelationPredicate), inner);
        }
        ScalarOperator domainPredicate = new ReplaceColumnRefRewriter(outerToDomain).rewrite(correlationPredicate);
        OptExpression domainJoin = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, domainPredicate), domain, innerChild);
        OptExpression domainAggregate = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(domainKeys), aggregations),
                domainJoin);

        // join the results back to the outer rows, every outer row matches at most one domain row
        List<ScalarOperator> joinBackPredicates = Lists.newArrayList();
        for (ColumnRefOperator ref : domainRefs) {
            joinBackPredicates.add(BinaryPredicateOperator.null_safe_eq(outerToConsume.get(ref),
                    outerToDomain.get(ref)));
        }
        OptExpression joinBack = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, Utils.compoundAnd(joinBackPredicates)),
                outerConsume, domainAggregate);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        outerToConsume.forEach(projectMap::put);
        projectMap.put(apply.getOutput(), applyOutput);
        projectMap.putAll(extraOutputs);
        OptExpression project = OptExpression.create(new LogicalProjectOperator(projectMap), joinBack);

        // the outer input is read twice, reuse it so both reads see the same rows
        context.getCteContext().addForceCTE(cteId);
        return OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, project);
    }
}
