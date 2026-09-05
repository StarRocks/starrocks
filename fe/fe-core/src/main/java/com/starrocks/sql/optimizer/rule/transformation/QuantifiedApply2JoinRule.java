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
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Rewrites an uncorrelated quantified subquery -- {@code IN} / {@code NOT IN} -- from a
 * {@link LogicalApplyOperator} (the dependent-join representation) into a plain hash join, so it
 * runs as a set operation instead of a per-row nested loop.
 *
 * <p>Transformation flow:
 * <ol>
 *   <li>Match a quantified, semi/anti Apply with no correlation (see {@link #check}). The subquery
 *       predicate is either an {@link InPredicateOperator} (single-column {@code x IN/NOT IN (sub)})
 *       or a {@link MultiInPredicateOperator} (row-value {@code (a, b) IN/NOT IN (sub)}).</li>
 *   <li>Build the equi keys: one {@code EQ(left_i, right_i)} per tuple column, ANDed together. They
 *       become the join's equality predicate and drive the hash join.</li>
 *   <li>For {@code NOT IN} over nullable columns, additionally build a per-column null-aware check
 *       {@code (l = r) OR l IS NULL OR r IS NULL}, ANDed across columns, carried as the join's OTHER
 *       conjunct. The executor's NULL_AWARE_LEFT_ANTI_JOIN applies it to the NULL-containing build
 *       rows so the result honours SQL three-valued logic.</li>
 *   <li>Pick the join type: {@code IN -> LEFT_SEMI_JOIN}, {@code NOT IN -> NULL_AWARE_LEFT_ANTI_JOIN};
 *       attach the correlation conjuncts and the apply predicate to the join ON clause.</li>
 *   <li>Wrap the join in a {@link LogicalProjectOperator} re-projecting the Apply's output columns.</li>
 * </ol>
 */
public class QuantifiedApply2JoinRule extends TransformationRule {
    public QuantifiedApply2JoinRule() {
        super(RuleType.TF_QUANTIFIED_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return apply.isUseSemiAnti() && apply.isQuantified()
                && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        boolean isNotIn = false;
        ScalarOperator simplifiedPredicate = null;
        // For a multi-column NOT IN, the equi keys (a=x AND b=y) only find exact (TRUE) matches; the
        // executor additionally pairs each probe row with the NULL-containing build rows for a
        // NULL_AWARE_LEFT_ANTI_JOIN. This per-column predicate -- (l=r) OR l IS NULL OR r IS NULL,
        // ANDed across columns -- is supplied as the join's OTHER conjunct so those null-build pairs
        // are kept only when the row comparison is TRUE or UNKNOWN (every column equal or NULL on
        // either side), matching SQL three-valued NOT IN semantics. The equi keys still drive the
        // hash join, so this keeps hash-join performance.
        ScalarOperator nullAwareConjunct = null;
        if (apply.getSubqueryOperator() instanceof MultiInPredicateOperator) {
            MultiInPredicateOperator multiIn = (MultiInPredicateOperator) apply.getSubqueryOperator();
            isNotIn = multiIn.isNotIn();
            List<ScalarOperator> conjuncts = Lists.newArrayList();
            List<ScalarOperator> nullAwareConjuncts = Lists.newArrayList();
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            for (int i = 0; i < multiIn.getTupleSize(); ++i) {
                ScalarOperator left = multiIn.getChild(i);
                ScalarOperator right = multiIn.getChild(multiIn.getTupleSize() + i);
                ScalarOperator normalizedConjunct =
                        rewriter.rewrite(new BinaryPredicateOperator(BinaryType.EQ,
                                left, right), ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                conjuncts.add(normalizedConjunct);
                if (isNotIn && (left.isNullable() || right.isNullable())) {
                    List<ScalarOperator> ors =
                            Lists.newArrayList(new BinaryPredicateOperator(BinaryType.EQ, left, right));
                    if (left.isNullable()) {
                        ors.add(new IsNullPredicateOperator(false, left));
                    }
                    if (right.isNullable()) {
                        ors.add(new IsNullPredicateOperator(false, right));
                    }
                    nullAwareConjuncts.add(rewriter.rewrite(Utils.compoundOr(ors),
                            ScalarOperatorRewriter.DEFAULT_REWRITE_RULES));
                }
            }
            simplifiedPredicate = Utils.compoundAnd(conjuncts);
            if (!nullAwareConjuncts.isEmpty()) {
                nullAwareConjunct = Utils.compoundAnd(nullAwareConjuncts);
            }
        } else {
            // IN/NOT IN
            InPredicateOperator ipo = (InPredicateOperator) apply.getSubqueryOperator();
            BinaryPredicateOperator bpo =
                    new BinaryPredicateOperator(BinaryType.EQ, ipo.getChildren());
            isNotIn = ipo.isNotIn();

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            simplifiedPredicate =
                    rewriter.rewrite(bpo, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);

        }
        simplifiedPredicate.setJoinDerived(true);

        // IN to SEMI-JOIN
        // NOT IN to ANTI-JOIN or NULL_AWARE_LEFT_ANTI_JOIN
        OptExpression joinExpression;
        if (isNotIn) {
            //@TODO: if will can filter null, use left-anti-join
            List<ScalarOperator> correlatedConjuncts = Utils.extractConjuncts(apply.getCorrelationConjuncts());
            correlatedConjuncts.forEach(conjunct -> conjunct.setCorrelated(true));
            ScalarOperator joinOnPredicate = Utils.compoundAnd(simplifiedPredicate,
                    Utils.compoundAnd(Utils.compoundAnd(correlatedConjuncts), apply.getPredicate()));
            if (nullAwareConjunct != null) {
                // Carry the per-column null-aware check as an OTHER join conjunct; the executor's
                // NAAJ filter path applies it to the null-build pairs to honour three-valued logic.
                nullAwareConjunct.setJoinDerived(true);
                joinOnPredicate = Utils.compoundAnd(joinOnPredicate, nullAwareConjunct);
            }
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN,
                    joinOnPredicate));
        } else {
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN,
                    Utils.compoundAnd(simplifiedPredicate,
                            Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate()))));
        }

        joinExpression.getInputs().addAll(input.getInputs());

        Map<ColumnRefOperator, ScalarOperator> outputColumns = input.getOutputColumns().getStream().map(
                id -> context.getColumnRefFactory().getColumnRef(id)
        ).collect(Collectors.toMap(Function.identity(), Function.identity()));
        return Lists.newArrayList(
                OptExpression.create(new LogicalProjectOperator(outputColumns), Lists.newArrayList(joinExpression)));
    }
}
