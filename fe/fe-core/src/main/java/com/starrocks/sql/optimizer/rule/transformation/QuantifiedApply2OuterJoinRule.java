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
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CorrelatedPredicateRewriter;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QuantifiedApply2OuterJoinRule extends TransformationRule {
    public QuantifiedApply2OuterJoinRule() {
        super(RuleType.TF_QUANTIFIED_APPLY_TO_OUTER_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return !apply.isUseSemiAnti() && apply.isQuantified()
                && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    /*
     * @todo: support constant in sub-query
     * e.g.
     *   select v0, 1 in (select v0 from t0) from t1
     *
     * transform to:
     *   select t1.v0,
     *       case
     *           when countRows is null then false;
     *           when v1.t0 is null then null,
     *           when d.v0 is null then null
     *           when d.v0 is not null then true
     *       else false
     *       end
     *   from t1 cross join (
     *       select distinct v0, count(*) as countRows
     *       from t0
     *       where v0 = 1 or v0 is null
     *       order by v0 desc limit 1) d;
     */
    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        List<ScalarOperator> joinOnPredicate = Utils.extractConjuncts(apply.getCorrelationConjuncts());

        if (apply.getSubqueryOperator() instanceof MultiInPredicateOperator) {
            throw new SemanticException("Multi-column IN subquery not supported anywhere except conjuncts of the" +
                    " WHERE clause");
        }

        // IN/NOT IN
        InPredicateOperator inPredicate = (InPredicateOperator) apply.getSubqueryOperator();
        if (inPredicate.getChild(0).getUsedColumns().isEmpty()) {
            throw new SemanticException(SubqueryUtils.CONST_QUANTIFIED_COMPARISON);
        }

        joinOnPredicate.add(
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, inPredicate.getChildren()));

        List<ColumnRefOperator> correlationColumnRefs = Utils.extractColumnRef(inPredicate.getChild(0));
        correlationColumnRefs.addAll(apply.getCorrelationColumnRefs());

        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(joinOnPredicate)) {
            // @Todo:
            // 1. require least a EQ predicate
            // 2. for other binary predicate rewrite rule
            //  a. outer-key < inner key -> outer-key < aggregate MAX(key)
            //  b. outer-key > inner key -> outer-key > aggregate MIN(key)
            throw new SemanticException(SubqueryUtils.EXIST_NON_EQ_PREDICATE);
        }

        CorrelationOuterJoinTransformer transformer = new CorrelationOuterJoinTransformer(input, context);
        return Lists.newArrayList(transformer.transform());
    }

    private static class CorrelationOuterJoinTransformer {
        private final boolean isNotIn;
        private final OptimizerContext context;
        private final LogicalApplyOperator apply;
        private final ColumnRefFactory factory;
        private final OptExpression input;
        private final int cteId;

        // e.g: select t0.v1, t0.v2 in (select t1.v2 from t1 where t0.v3 = t1.v3) from t0;
        //
        // origin in sub-query in predicate
        // e.g: t0.v2 in (t1.v2) -> t0.v2 = t1.v2
        private BinaryPredicateOperator inPredicate;

        // origin correlation predicate
        // e.g: t0.v3 = t1.v3
        private ScalarOperator correlationPredicate;

        private ColumnRefSet inPredicateUsedRefs;

        private ColumnRefSet correlationPredicateInnerRefs;

        private ScalarOperator countJoinOnPredicate;

        private ScalarOperator distinctJoinOnPredicate;

        private final List<ColumnRefOperator> distinctAggregateOutputs;

        private ColumnRefOperator countRows;

        private ColumnRefOperator countNulls;

        public CorrelationOuterJoinTransformer(OptExpression input, OptimizerContext context) {
            this.input = input;
            this.apply = (LogicalApplyOperator) input.getOp();
            InPredicateOperator in = (InPredicateOperator) apply.getSubqueryOperator();

            this.isNotIn = in.isNotIn();
            this.context = context;
            this.factory = context.getColumnRefFactory();

            this.inPredicate = BinaryPredicateOperator.eq(in.getChild(0), in.getChild(1));
            this.correlationPredicate = apply.getCorrelationConjuncts();
            this.cteId = factory.getNextRelationId();

            this.distinctAggregateOutputs = Lists.newArrayList();
        }

        /*
         * In:
         * before: select t0.v1, t0.v2 in (select t1.v2 from t1 where t0.v3 = t1.v3) from t0;
         * after: with xx as (select t1.v2, t1.v3 from t1)
         *        select t0.v1,
         *             case
         *                 // t1 empty table and without where clause `t0.v3 = t1.v3`, then t1Rows may be 0
         *                 // t1 empty table, if t1Rows is null, means the result of join correlation predicate must be false
         *                 // `any_or_null in (empty) -> false`
         *                 when t1Rows = 0 or t1Rows is null then false
         *                 // `null in (any_or_null...) -> null`
         *                 when t0.v2 is null then null
         *                 // `a in (a, [any_or_null...]) -> true`
         *                 when t1d.v2 is not null then true
         *                 // `a in (null, [not_a_or_null...]) -> null`
         *                 when v2NotNulls < t1Rows then null
         *                 // `a in (not_a...) -> false`, not_a cannot be null
         *                 else false
         *             end
         *         from t0
         *              left outer join (select xx.v2, xx.v3 from xx group by xx.v2, xx.v3) as t1d
         *                              on t0.v2= t1d.v2 and t0.v3 = t1d.v3
         *              left outer join (select v3, count(*) as t1Rows, count(xx.v2) as v2NotNulls from xx group by xx.v3) as t1c
         *                              on t1c.v3 = t0.v3
         *
         *                                           CTEAnchor
         *                                          /        \
         *                                    CTEProduce     Project
         *    Apply(t0.v2 in t1.v2)              /               \
         *    /          \          ===>     Project         left/cross join
         *   t0          t1                   /              /          \
         *                                Filter         Left join       Agg(count)
         *                                 /             /       \           \
         *                               t1            t0     Agg(distinct)  CTEConsume
         *                                                         \
         *                                                        CTEConsume
         * */
        public OptExpression transform() {
            check();

            OptExpression cteProduce = buildCTEProduce();

            OptExpression distinctAggregate = buildDistinctAndConsume();
            OptExpression countAggregate = buildCountAndConsume();

            OptExpression leftJoin = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN,
                    distinctJoinOnPredicate), input.getInputs().get(0), distinctAggregate);

            OptExpression crossJoin = buildCrossOrLeftJoin(leftJoin, countAggregate);

            OptExpression caseWhen = buildCaseWhen(crossJoin);

            OptExpression anchor = OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, caseWhen);

            context.getCteContext().addForceCTE(cteId);
            return anchor;
        }

        private void check() {
            if (!context.getSessionVariable().isEnablePipelineEngine()) {
                throw new SemanticException("In sub-query depend on pipeline which one not in where-clause, " +
                        "enable by: set enable_pipeline_engine=true;");
            }
        }

        /*
         *
         *   CTEProduce
         *      |
         *   project(optional)
         *      |
         *    filter(optional)
         *      |
         *    child
         *
         */
        private OptExpression buildCTEProduce() {
            // CTE Produce
            OptExpression cteProduceChild = input.getInputs().get(1);

            CorrelatedPredicateRewriter corPredRewriter = new CorrelatedPredicateRewriter(
                    apply.getCorrelationColumnRefs(), context);

            CorrelatedPredicateRewriter inPredRewriter = new CorrelatedPredicateRewriter(
                    Utils.extractColumnRef(inPredicate.getChild(0)), context);

            correlationPredicate = SubqueryUtils.rewritePredicateAndExtractColumnRefs(
                    correlationPredicate, corPredRewriter);

            inPredicate = (BinaryPredicateOperator) SubqueryUtils.rewritePredicateAndExtractColumnRefs(
                    inPredicate, inPredRewriter);

            // update used columns
            Preconditions.checkState(inPredRewriter.getColumnRefToExprMap().keySet().size() == 1);
            inPredicateUsedRefs = new ColumnRefSet(inPredRewriter.getColumnRefToExprMap().keySet());
            correlationPredicateInnerRefs = new ColumnRefSet(corPredRewriter
                    .getColumnRefToExprMap().keySet());

            // CTE produce filter
            if (null != apply.getPredicate()) {
                LogicalProperty oldProperty = cteProduceChild.getLogicalProperty();
                cteProduceChild = OptExpression.create(
                        new LogicalFilterOperator(apply.getPredicate()), cteProduceChild);
                // todo: need check the OptExpression.create() method to avoid logicalProperty hasn't been set
                cteProduceChild.setLogicalProperty(oldProperty);
            }

            // CTE produce project
            if (SubqueryUtils.existNonColumnRef(corPredRewriter.getColumnRefToExprMap().values()) ||
                    SubqueryUtils.existNonColumnRef(inPredRewriter.getColumnRefToExprMap().values())) {
                // has function, need project node
                Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
                projectMap.putAll(corPredRewriter.getColumnRefToExprMap());
                projectMap.putAll(inPredRewriter.getColumnRefToExprMap());
                projectMap = SubqueryUtils.generateChildOutColumns(cteProduceChild, projectMap, context);

                cteProduceChild = OptExpression.create(new LogicalProjectOperator(projectMap), cteProduceChild);
            }

            return OptExpression.create(new LogicalCTEProduceOperator(cteId), cteProduceChild);
        }

        /*
         *  Aggregate(group by correlation columns + in column)
         *              |
         *          CTEConsume
         */
        private OptExpression buildDistinctAndConsume() {
            OptExpression cteProduceChild = input.getInputs().get(1);
            // CTE distinct consume
            Map<ColumnRefOperator, ColumnRefOperator> distinctConsumeOutputMaps = Maps.newHashMap();

            for (int columnId : inPredicateUsedRefs.getColumnIds()) {
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                distinctConsumeOutputMaps.put(factory.create(ref, ref.getType(), ref.isNullable()), ref);
            }

            for (int columnId : correlationPredicateInnerRefs.getColumnIds()) {
                if (inPredicateUsedRefs.contains(columnId)) {
                    // if inPredicateUsedRefs contains the col, we can skip this col to reduce number of group by cols.
                    continue;
                }
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                ColumnRefOperator dr = factory.create(ref, ref.getType(), ref.isNullable());
                distinctConsumeOutputMaps.put(dr, ref);
            }

            ColumnRefSet correlationCols = new ColumnRefSet();
            if (correlationPredicate != null) {
                correlationPredicate.getUsedColumns().getStream().
                        filter(c -> cteProduceChild.getOutputColumns().contains(c)).forEach(correlationCols::union);
            }
            for (int columnId : correlationCols.getColumnIds()) {
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                if (!distinctConsumeOutputMaps.containsValue(ref)) {
                    ColumnRefOperator dr = factory.create(ref, ref.getType(), ref.isNullable());
                    distinctConsumeOutputMaps.put(dr, ref);
                }
            }

            OptExpression distinctConsume =
                    OptExpression.create(new LogicalCTEConsumeOperator(cteId, distinctConsumeOutputMaps));

            // rewrite predicate by cte consume output
            Map<ColumnRefOperator, ScalarOperator> distinctRewriteMap = Maps.newHashMap();
            distinctConsumeOutputMaps.forEach((k, v) -> distinctRewriteMap.put(v, k));
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(distinctRewriteMap);

            inPredicate = (BinaryPredicateOperator) rewriter.rewrite(inPredicate);
            distinctJoinOnPredicate = Utils.compoundAnd(inPredicate, rewriter.rewrite(correlationPredicate));

            distinctAggregateOutputs.addAll(distinctConsumeOutputMaps.keySet());
            // distinct aggregate
            return OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL,
                    Lists.newArrayList(distinctAggregateOutputs), Maps.newHashMap()), distinctConsume);
        }

        /*
         * Aggregate(count(1), count(in column), group by correlation columns)
         *          |
         *      CTEConsume
         */
        private OptExpression buildCountAndConsume() {
            OptExpression cteProduceChild = input.getInputs().get(1);
            // CTE count consume
            Map<ColumnRefOperator, ColumnRefOperator> countConsumeOutputMaps = Maps.newHashMap();

            // rewrite cross/left join on-clause by the output cols of one child(the count agg OptExpression) of join
            Map<ColumnRefOperator, ScalarOperator> countRewriteMap = Maps.newHashMap();

            List<ColumnRefOperator> countAggregateGroupBys = Lists.newArrayList();
            List<ColumnRefOperator> countAggregateCounts = Lists.newArrayList();
            Set<ColumnRefOperator> countAggregateGroupBysRefSet = Sets.newHashSet();

            for (int columnId : inPredicateUsedRefs.getColumnIds()) {
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                ColumnRefOperator cr = factory.create(ref, ref.getType(), ref.isNullable());
                countConsumeOutputMaps.put(cr, ref);
                countAggregateCounts.add(cr);
            }

            for (int columnId : correlationPredicateInnerRefs.getColumnIds()) {
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                ColumnRefOperator cr = factory.create(ref, ref.getType(), ref.isNullable());
                countConsumeOutputMaps.put(cr, ref);
                countRewriteMap.put(ref, cr);
                countAggregateGroupBys.add(cr);
                countAggregateGroupBysRefSet.add(ref);
            }

            ColumnRefSet correlationCols = new ColumnRefSet();
            if (correlationPredicate != null) {
                correlationPredicate.getUsedColumns().getStream().
                        filter(c -> cteProduceChild.getOutputColumns().contains(c)).forEach(correlationCols::union);
            }
            for (int columnId : correlationCols.getColumnIds()) {
                ColumnRefOperator ref = factory.getColumnRef(columnId);
                if (!countAggregateGroupBysRefSet.contains(ref)) {
                    ColumnRefOperator dr = factory.create(ref, ref.getType(), ref.isNullable());
                    countConsumeOutputMaps.put(dr, ref);
                    countRewriteMap.put(ref, dr);
                    countAggregateGroupBys.add(dr);
                    countAggregateGroupBysRefSet.add(ref);
                }
            }

            OptExpression countConsume =
                    OptExpression.create(new LogicalCTEConsumeOperator(cteId, countConsumeOutputMaps));

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(countRewriteMap);
            countJoinOnPredicate = rewriter.rewrite(correlationPredicate);

            // count aggregate
            Map<ColumnRefOperator, CallOperator> aggregates = Maps.newHashMap();
            CallOperator countRowsFn = SubqueryUtils.createCountRowsOperator();
            countRows = factory.create("countRows", countRowsFn.getType(), countRowsFn.isNullable());
            aggregates.put(countRows, countRowsFn);

            Preconditions.checkState(countAggregateCounts.size() == 1);

            CallOperator countNullFn = SubqueryUtils.createCountRowsOperator(countAggregateCounts.get(0));
            countNulls = factory.create("countNotNulls", countNullFn.getType(), countNullFn.isNullable());
            aggregates.put(countNulls, countNullFn);

            return OptExpression.create(
                    new LogicalAggregationOperator(AggType.GLOBAL, countAggregateGroupBys, aggregates), countConsume);
        }

        /*
         * Correlation sub-query only need to consider the data related to the correlation key.
         * 1. build cross join if un-correlation sub-query
         * 2. build left join if correlation sub-query
         */
        private OptExpression buildCrossOrLeftJoin(OptExpression leftJoin, OptExpression countAggregate) {
            if (countJoinOnPredicate != null) {
                return OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, countJoinOnPredicate),
                        leftJoin, countAggregate);
            }

            return OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null),
                    leftJoin, countAggregate);
        }

        /*
         * Project:
         * case
         *     when t1Rows = 0 or t1Rows is null then false
         *     when t0.v2 is null then null
         *     when t1d.v2 is not null then true
         *     when v2NotNulls < t1Rows then null
         *     else false
         * end
         */
        private OptExpression buildCaseWhen(OptExpression left) {
            // case when
            Map<ColumnRefOperator, ScalarOperator> caseWhenMap = Maps.newHashMap();
            Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds())
                    .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> caseWhenMap.put(d, d));
            distinctAggregateOutputs.forEach(k -> caseWhenMap.put(k, k));

            List<ScalarOperator> whenThen = Lists.newArrayList();
            whenThen.add(CompoundPredicateOperator.or(new IsNullPredicateOperator(countRows),
                    BinaryPredicateOperator.eq(countRows, ConstantOperator.createBigint(0))));
            whenThen.add(ConstantOperator.createBoolean(false));

            whenThen.add(new IsNullPredicateOperator(inPredicate.getChild(0)));
            whenThen.add(ConstantOperator.createNull(Type.BOOLEAN));

            whenThen.add(new IsNullPredicateOperator(true, inPredicate.getChild(1)));
            whenThen.add(ConstantOperator.createBoolean(true));

            whenThen.add(BinaryPredicateOperator.lt(countNulls, countRows));
            whenThen.add(ConstantOperator.createNull(Type.BOOLEAN));

            ScalarOperator caseWhen =
                    new CaseWhenOperator(Type.BOOLEAN, null, ConstantOperator.createBoolean(false), whenThen);

            if (isNotIn) {
                caseWhenMap.put(apply.getOutput(),
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, caseWhen));
            } else {
                caseWhenMap.put(apply.getOutput(), caseWhen);
            }

            return OptExpression.create(new LogicalProjectOperator(caseWhenMap), left);
        }
    }
}
