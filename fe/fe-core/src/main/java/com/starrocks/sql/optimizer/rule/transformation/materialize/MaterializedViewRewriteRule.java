// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialize;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.Collections;
import java.util.List;

import static com.starrocks.common.Pair.create;
import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ;

/*
 * SPJG materialized view rewrite rule, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 * Here is the rule for pattern Filter - Project - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 * Put multi table query rewrite in memo phase
 *
 */
public class MaterializedViewRewriteRule extends TransformationRule {
    public MaterializedViewRewriteRule() {
        super(RuleType.TF_MV_PROJECT_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static class RewrittenCandidate {
        public MaterializedView mv;
        public OptExpression rewrittenExpression;

        public RewrittenCandidate(MaterializedView mv, OptExpression expression) {
            this.mv = mv;
            this.rewrittenExpression = expression;
        }
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator childOperator = input.getInputs().get(0).getOp();
        if (!(childOperator instanceof LogicalScanOperator)) {
            return false;
        }
        // it must be SPJG tree
        if (context.getCandidateMvs().isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression filter, OptimizerContext context) {
        // should get all predicates within and below this OptExpression
        List<ScalarOperator> queryConjuncts = RewriteUtils.getAllPredicates(filter);
        ScalarOperator queryPredicate = Utils.compoundAnd(queryConjuncts);
        // column equality predicates and residual predicates
        Pair<ScalarOperator, ScalarOperator> splitedPredicatePair = RewriteUtils.splitPredicate(queryPredicate);
        EquivalenceClasses queryEc = new EquivalenceClasses();
        for (ScalarOperator equalPredicate : Utils.extractConjuncts(splitedPredicatePair.first)) {
            Preconditions.checkState(equalPredicate.isColumnRef());
            ColumnRefOperator left = (ColumnRefOperator) equalPredicate.getChild(0);
            ColumnRefOperator right = (ColumnRefOperator) equalPredicate.getChild(1);
            queryEc.addEquivalence(left, right);
        }

        // should get all query tables
        // add filter here
        List<Table> queryRefTables = RewriteUtils.getAllTables(filter);


        OptExpression rewrittenExpression = null;
        for (MaterializationContext mvContext : context.getCandidateMvs()) {
            List<Table> mvRefTables = null;
            // if the ref table set between query and mv do not intersect, skip
            if (Collections.disjoint(queryRefTables, mvRefTables)) {
                continue;
            }

            MaterializedViewRewriter rewriter = new MaterializedViewRewriter(splitedPredicatePair, queryEc,
                    null, filter, mvContext.getMv(), context);
            OptExpression rewritten = rewriter.rewriteQuery();
            if (rewritten != null) {
                // TODO(hkp): compare the cost and decide whether to use rewritten
                rewrittenExpression = rewritten;
            }
        }

        // sort candidates and return lowest cost candidate
        return Lists.newArrayList(rewrittenExpression);
    }


}
