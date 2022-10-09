// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Collections;
import java.util.List;

/*
 * SPJG materialized view rewrite rule, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 * Here is the rule for pattern Filter - Project - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 * Put multi table query rewrite in memo phase
 *
 */
public class BaseMaterializedViewRewriteRule extends TransformationRule {
    public BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // it must be SPJG tree
        if (context.getCandidateMvs().isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        // should get all predicates within and below this OptExpression
        List<ScalarOperator> queryConjuncts = RewriteUtils.getAllPredicates(queryExpression);
        ScalarOperator queryPredicate = Utils.compoundAnd(queryConjuncts);
        // column equality predicates and residual predicates
        // final Pair<ScalarOperator, ScalarOperator> splitedPredicatePair = RewriteUtils.splitPredicate(queryPredicate);
        final Triple<ScalarOperator, ScalarOperator, ScalarOperator> predicateTriple =
                RewriteUtils.splitPredicateToTriple(queryPredicate);
        EquivalenceClasses queryEc = new EquivalenceClasses();
        for (ScalarOperator equalPredicate : Utils.extractConjuncts(predicateTriple.getLeft())) {
            Preconditions.checkState(equalPredicate.getChild(0).isColumnRef());
            ColumnRefOperator left = (ColumnRefOperator) equalPredicate.getChild(0);
            Preconditions.checkState(equalPredicate.getChild(1).isColumnRef());
            ColumnRefOperator right = (ColumnRefOperator) equalPredicate.getChild(1);
            queryEc.addEquivalence(left, right);
        }

        // should get all query tables
        // add filter here
        List<Table> queryRefTables = RewriteUtils.getAllTables(queryExpression);


        List<OptExpression> results = Lists.newArrayList();
        for (MaterializationContext mvContext : context.getCandidateMvs()) {
            List<Table> mvRefTables = RewriteUtils.getAllTables(mvContext.getMvExpression());
            // if the ref table set between query and mv do not intersect, skip
            if (Collections.disjoint(queryRefTables, mvRefTables)) {
                continue;
            }

            LogicalProjectOperator queryProjector = null;
            if (queryExpression.getOp() instanceof LogicalProjectOperator) {
                queryProjector = (LogicalProjectOperator) queryExpression.getOp();
            }
            MaterializedViewRewriter rewriter = new MaterializedViewRewriter(predicateTriple, queryEc,
                    queryProjector, queryExpression, queryRefTables, mvRefTables, mvContext, context);
            List<OptExpression> rewritten = rewriter.rewriteQuery();
            if (rewritten != null) {
                // TODO(hkp): compare the cost and decide whether to use rewritten
                results.addAll(rewritten);
            }
        }

        // sort candidates and return lowest cost candidate
        return results;
    }


}
