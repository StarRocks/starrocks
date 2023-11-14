// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CorrelatedPredicateRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ScalarApply2JoinRule extends TransformationRule {
    public ScalarApply2JoinRule() {
        super(RuleType.TF_SCALAR_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        // Or-Scope is same with And-Scope
        return apply.isScalar() && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // Un-correlation subquery
        if (null == apply.getCorrelationConjuncts()) {
            return transformUnCorrelateCheckOneRows(input, apply, context);
        } else {
            return transformCorrelate(input, apply, context);
        }
    }

    private List<OptExpression> transformCorrelate(OptExpression input, LogicalApplyOperator apply,
                                                   OptimizerContext context) {
        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(Utils.extractConjuncts(apply.getCorrelationConjuncts()))) {
            throw new SemanticException(SubqueryUtils.EXIST_NON_EQ_PREDICATE);
        }

        if (apply.isNeedCheckMaxRows()) {
            return transformCorrelateWithCheckOneRows(input, apply, context);
        } else {
            return transformCorrelateWithoutCheckOneRows(input, apply, context);
        }
    }

    /*
     * e.g.
     *   SELECT * FROM t0
     *   WHERE t0.v2 > (
     *       SELECT t1.v2 FROM t1
     *       WHERE t0.v1 = t1.v1
     *   );
     *
     * Transfer to:
     *   SELECT t0.*, assert_true((t1a.countRows IS NULL) OR (t1a.countRows <= 1))
     *   FROM t0 LEFT OUTER JOIN
     *   (
     *       SELECT t1.v1, count(1) as countRows, any_value(t1.v2) as anyValue
     *       FROM t1
     *       GROUP BY t1.v1
     *   ) as t1a
     *   ON t0.v1 = t1a.v1
     *   WHERE t0.v2 > t1a.anyValue;
     */
    private List<OptExpression> transformCorrelateWithCheckOneRows(OptExpression input, LogicalApplyOperator apply,
                                                                   OptimizerContext context) {
        // t0.v1 = t1.v1
        ScalarOperator correlationPredicate = apply.getCorrelationConjuncts();

        CorrelatedPredicateRewriter rewriter = new CorrelatedPredicateRewriter(
                apply.getCorrelationColumnRefs(), context);

        ScalarOperator newPredicate = SubqueryUtils.rewritePredicateAndExtractColumnRefs(correlationPredicate, rewriter);

        Map<ColumnRefOperator, ScalarOperator> innerRefMap = rewriter.getColumnRefToExprMap();

        OptExpression rightChild = input.inputAt(1);

        if (SubqueryUtils.existNonColumnRef(innerRefMap.values())) {
            // exists expression, need put it in project node
            rightChild = OptExpression.create(new LogicalProjectOperator(
                    SubqueryUtils.generateChildOutColumns(rightChild, innerRefMap, context)), rightChild);
        }

        // Non-correlated predicates
        if (apply.getPredicate() != null) {
            rightChild = OptExpression.create(new LogicalFilterOperator(apply.getPredicate()), rightChild);
        }

        /*
         * Step1: build agg
         *      output: count(1) as countRows, any_value(t1.v2) as anyValue
         *      groupBy: t1.v1
         */
        ColumnRefFactory factory = context.getColumnRefFactory();

        // count aggregate
        Map<ColumnRefOperator, CallOperator> aggregates = Maps.newHashMap();
        CallOperator countRowsCallOp = SubqueryUtils.createCountRowsOperator();
        ColumnRefOperator countRows =
                factory.create("countRows", countRowsCallOp.getType(), countRowsCallOp.isNullable());
        aggregates.put(countRows, countRowsCallOp);

        // any_value aggregate
        ScalarOperator subqueryOperator = apply.getSubqueryOperator();
        CallOperator anyValueCallOp = SubqueryUtils.createAnyValueOperator(subqueryOperator);
        if (anyValueCallOp.getFunction() == null) {
            throw new SemanticException(String.format(
                    "NOT support scalar correlated sub-query of type %s",
                    subqueryOperator.getType().toSql()));
        }
        ColumnRefOperator anyValue = factory.create("anyValue", anyValueCallOp.getType(), anyValueCallOp.isNullable());
        aggregates.put(anyValue, anyValueCallOp);

        OptExpression newAggOpt = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(innerRefMap.keySet()), aggregates),
                rightChild);

        /*
         * Step2: build left outer join
         * t0 LEFT OUTER JOIN t1a
         * ON t0.v1 = t1a.v1
         */
        LogicalJoinOperator joinOp = new LogicalJoinOperator.Builder()
                .setJoinType(JoinOperator.LEFT_OUTER_JOIN)
                .setOnPredicate(newPredicate)
                .build();
        OptExpression newLeftOuterJoinOpt = OptExpression.create(joinOp, input.inputAt(0), newAggOpt);

        /*
         * Step3: build project
         */
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        // Other columns
        projectMap.put(countRows, countRows);
        projectMap.put(anyValue, anyValue);
        Arrays.stream(input.inputAt(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(i -> projectMap.put(i, i));
        // Mapping subquery's output to anyValue
        projectMap.put(apply.getOutput(), anyValue);
        // Add assertion column
        IsNullPredicateOperator countRowsIsNullPredicate = new IsNullPredicateOperator(countRows);
        BinaryPredicateOperator countRowsLEOneRowPredicate =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, countRows,
                        ConstantOperator.createBigint(1));
        PredicateOperator countRowsPredicate =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, countRowsIsNullPredicate,
                        countRowsLEOneRowPredicate);
        Function assertTrueFn = Expr.getBuiltinFunction(FunctionSet.ASSERT_TRUE, new Type[] {Type.BOOLEAN},
                Function.CompareMode.IS_IDENTICAL);
        CallOperator assertTrueCallOp =
                new CallOperator(FunctionSet.ASSERT_TRUE, Type.BOOLEAN,
                        Collections.singletonList(countRowsPredicate),
                        assertTrueFn);
        ColumnRefOperator assertion =
                factory.create("subquery_assertion", assertTrueCallOp.getType(), assertTrueCallOp.isNullable());
        projectMap.put(assertion, assertTrueCallOp);
        OptExpression newProjectOpt =
                OptExpression.create(new LogicalProjectOperator(projectMap), newLeftOuterJoinOpt);

        return Collections.singletonList(newProjectOpt);
    }

    private List<OptExpression> transformCorrelateWithoutCheckOneRows(OptExpression input, LogicalApplyOperator apply,
                                                                      OptimizerContext context) {
        OptExpression joinOptExpression;

        joinOptExpression = new OptExpression(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN,
                        Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate())));

        joinOptExpression.getInputs().addAll(input.getInputs());

        Map<ColumnRefOperator, ScalarOperator> output = Maps.newHashMap();
        output.put(apply.getOutput(), apply.getSubqueryOperator());

        // add all left column
        ColumnRefFactory factory = context.getColumnRefFactory();
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(d -> output.put(d, d));

        OptExpression projectExpression = new OptExpression(new LogicalProjectOperator(output));
        projectExpression.getInputs().add(joinOptExpression);

        return Lists.newArrayList(projectExpression);
    }

    // UnCorrelation Scalar Subquery:
    //  Project(output: subqueryOperator)
    //              |
    //          Cross-Join
    //          /        \
    //       LEFT     AssertOneRows
    private List<OptExpression> transformUnCorrelateCheckOneRows(OptExpression input, LogicalApplyOperator apply,
                                                                 OptimizerContext context) {
        // assert one rows will check rows, and fill null row if result is empty
        OptExpression assertOptExpression = new OptExpression(LogicalAssertOneRowOperator.createLessEqOne(""));
        assertOptExpression.getInputs().add(input.getInputs().get(1));

        // use hint, forbidden reorder un-correlate subquery
        OptExpression joinOptExpression = new OptExpression(LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.CROSS_JOIN)
                .setJoinHint(JoinOperator.HINT_BROADCAST).build());
        joinOptExpression.getInputs().add(input.getInputs().get(0));
        joinOptExpression.getInputs().add(assertOptExpression);

        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, ScalarOperator> allOutput = Maps.newHashMap();
        allOutput.put(apply.getOutput(), apply.getSubqueryOperator());

        // add all left outer column
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(d -> allOutput.put(d, d));

        OptExpression projectExpression = new OptExpression(new LogicalProjectOperator(allOutput));
        projectExpression.getInputs().add(joinOptExpression);

        return Lists.newArrayList(projectExpression);
    }
}
