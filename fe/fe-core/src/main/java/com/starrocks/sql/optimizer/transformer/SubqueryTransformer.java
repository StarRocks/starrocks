// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReplaceSubqueryRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReplaceSubqueryTypeRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SubqueryTransformer {

    private SubqueryTransformer() {
    }

    public static OptExprBuilder translate(ConnectContext session, CTETransformerContext cteContext,
                                           ColumnRefFactory columnRefFactory,
                                           OptExprBuilder subOpt, ScalarOperator scalarOperator, boolean useSemiAnti) {
        Visitor visitor = new Visitor(columnRefFactory, session);
        return scalarOperator.accept(visitor,
                new SubqueryContext(subOpt, useSemiAnti, cteContext));
    }

    public static ScalarOperator rewriteScalarOperator(ScalarOperator scalarOperator,
                                                       ExpressionMapping expressionMapping) {
        scalarOperator = eliminateInOrExistsPredicate(scalarOperator);
        return replaceSubquery(scalarOperator, expressionMapping);
    }

    private static ScalarOperator eliminateInOrExistsPredicate(ScalarOperator scalarOperator) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(scalarOperator);

        List<ScalarOperator> newConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());

        for (ScalarOperator conjunct : conjuncts) {
            if (conjunct instanceof InPredicateOperator) {
                if (conjunct.getChild(1) instanceof SubqueryOperator) {
                    SubqueryOperator subqueryOperator = conjunct.getChild(1).cast();
                    if (subqueryOperator.isUseSemiAnti()) {
                        continue;
                    }
                }
            } else if (conjunct instanceof ExistsPredicateOperator) {
                if (conjunct.getChild(0) instanceof SubqueryOperator) {
                    SubqueryOperator subqueryOperator = conjunct.getChild(0).cast();
                    if (subqueryOperator.isUseSemiAnti()) {
                        continue;
                    }
                }
            }

            newConjuncts.add(conjunct);
        }

        return Utils.compoundAnd(newConjuncts);
    }

    private static ScalarOperator replaceSubquery(ScalarOperator scalarOperator, ExpressionMapping expressionMapping) {
        if (scalarOperator == null) {
            return null;
        }
        List<SubqueryOperator> subqueries = Utils.collect(scalarOperator, SubqueryOperator.class);
        if (CollectionUtils.isEmpty(subqueries)) {
            return scalarOperator;
        }

        // The scalarOperator will be rewritten in the following steps:
        // 1. replace the SubqueryOperator with the ColumnRefOperator from expressionMapping
        // 2. re-calculate types of some functions or expression, such as if and case when
        // 3. perform implicit cast if necessary
        List<ScalarOperatorRewriteRule> rules = Lists.newArrayList();
        rules.add(new ReplaceSubqueryRewriteRule(expressionMapping));
        rules.add(new ReplaceSubqueryTypeRewriteRule());
        rules.add(new ImplicitCastRule());
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        return rewriter.rewrite(scalarOperator, rules);
    }

    private static final class Visitor extends ScalarOperatorVisitor<OptExprBuilder, SubqueryContext> {

        private final ColumnRefFactory columnRefFactory;
        private final ConnectContext session;

        public Visitor(ColumnRefFactory columnRefFactory, ConnectContext session) {
            this.columnRefFactory = columnRefFactory;
            this.session = session;
        }

        private LogicalPlan getLogicalPlan(QueryRelation relation, ConnectContext session, ExpressionMapping outer,
                                           CTETransformerContext cteContext) {
            if (!(relation instanceof SelectRelation)) {
                throw new SemanticException("Currently only subquery of the Select type are supported");
            }

            // For in subQuery, the order by is meaningless
            if (!relation.hasLimit()) {
                relation.getOrderBy().clear();
            }

            return new RelationTransformer(columnRefFactory, session, outer, cteContext).transform(relation);
        }

        @Override
        public OptExprBuilder visit(ScalarOperator operator, SubqueryContext context) {
            OptExprBuilder builder = context.builder;

            List<ScalarOperator> outerExprs = Collections.emptyList();
            if (operator.getChildren().stream().filter(SubqueryOperator.class::isInstance).count() == 1) {
                outerExprs = operator.getChildren().stream().filter(c -> !(c instanceof SubqueryOperator))
                        .collect(Collectors.toList());
            }
            for (ScalarOperator child : operator.getChildren()) {
                builder = child.accept(this,
                        new SubqueryContext(builder, false, context.cteContext, outerExprs));
            }

            return builder;
        }

        @Override
        public OptExprBuilder visitInPredicate(InPredicateOperator predicate, SubqueryContext context) {
            if (!(predicate.getChild(1) instanceof SubqueryOperator)) {
                return context.builder;
            }

            QueryStatement queryStatement = ((SubqueryOperator) predicate.getChild(1)).getQueryStatement();
            QueryRelation qb = queryStatement.getQueryRelation();
            LogicalPlan subqueryPlan = getLogicalPlan(qb, session, context.builder.getExpressionMapping(),
                    context.cteContext);
            if (qb instanceof SelectRelation &&
                    !subqueryPlan.getCorrelation().isEmpty() && ((SelectRelation) qb).hasAggregation()) {
                throw new SemanticException(
                        "Unsupported correlated in predicate subquery with grouping or aggregation");
            }

            ScalarOperator leftColRef = predicate.getChild(0);
            List<ColumnRefOperator> rightColRefs = subqueryPlan.getOutputColumn();
            if (rightColRefs.size() > 1) {
                throw new SemanticException("subquery must return a single column when used in InPredicate");
            }
            ColumnRefOperator rightColRef = rightColRefs.get(0);

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator inPredicateOperator =
                    rewriter.rewrite(new InPredicateOperator(predicate.isNotIn(), leftColRef, rightColRef),
                            ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(inPredicateOperator, inPredicateOperator.getType(),
                            inPredicateOperator.isNullable());
            ((SubqueryOperator) predicate.getChild(1)).setUseSemiAnti(context.useSemiAnti);
            context.builder.getExpressionMapping().put(predicate, outputPredicateRef);

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(inPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();
            context.builder =
                    new OptExprBuilder(applyOperator, Arrays.asList(context.builder, subqueryPlan.getRootBuilder()),
                            context.builder.getExpressionMapping());

            return context.builder;
        }

        @Override
        public OptExprBuilder visitExistsPredicate(ExistsPredicateOperator predicate, SubqueryContext context) {
            Preconditions.checkState(predicate.getChild(0) instanceof SubqueryOperator);

            QueryRelation qb = ((SubqueryOperator) predicate.getChild(0)).getQueryStatement().getQueryRelation();
            LogicalPlan subqueryPlan = getLogicalPlan(qb, session, context.builder.getExpressionMapping(),
                    context.cteContext);

            List<ColumnRefOperator> rightColRef = subqueryPlan.getOutputColumn();

            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(predicate, predicate.getType(), predicate.isNullable());
            ((SubqueryOperator) predicate.getChild(0)).setUseSemiAnti(context.useSemiAnti);
            context.builder.getExpressionMapping().put(predicate, outputPredicateRef);

            ExistsPredicateOperator existsPredicateOperator =
                    new ExistsPredicateOperator(predicate.isNotExists(), rightColRef.get(0));

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(existsPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();
            context.builder =
                    new OptExprBuilder(applyOperator, Arrays.asList(context.builder, subqueryPlan.getRootBuilder()),
                            context.builder.getExpressionMapping());

            return context.builder;
        }

        @Override
        public OptExprBuilder visitBetweenPredicate(BetweenPredicateOperator predicate, SubqueryContext context) {
            ScalarOperator compound;
            if (predicate.isNotBetween()) {
                ScalarOperator lower = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                        predicate.getChild(0), predicate.getChild(1));
                ScalarOperator upper = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT,
                        predicate.getChild(0), predicate.getChild(2));
                compound = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                        lower, upper);
            } else {
                ScalarOperator lower = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                        predicate.getChild(0), predicate.getChild(1));
                ScalarOperator upper = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE,
                        predicate.getChild(0), predicate.getChild(2));
                compound = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        lower, upper);
            }
            return compound.accept(this, context);
        }

        @Override
        public OptExprBuilder visitCompoundPredicate(CompoundPredicateOperator predicate, SubqueryContext context) {
            OptExprBuilder builder = context.builder;
            if (CompoundPredicateOperator.CompoundType.OR == predicate.getCompoundType()) {
                builder = predicate.getChild(0)
                        .accept(this, new SubqueryContext(builder, false, context.cteContext));
                builder = predicate.getChild(1)
                        .accept(this, new SubqueryContext(builder, false, context.cteContext));
            } else if (CompoundPredicateOperator.CompoundType.AND == predicate.getCompoundType()) {
                // And Scope extend from parents
                builder = predicate.getChild(0).accept(this,
                        new SubqueryContext(builder, context.useSemiAnti, context.cteContext));
                builder = predicate.getChild(1).accept(this,
                        new SubqueryContext(builder, context.useSemiAnti, context.cteContext));
            } else {
                builder = predicate.getChild(0)
                        .accept(this, new SubqueryContext(builder, false, context.cteContext));
            }

            return builder;
        }

        @Override
        public OptExprBuilder visitSubqueryOperator(SubqueryOperator operator, SubqueryContext context) {
            QueryStatement queryStatement = operator.getQueryStatement();
            QueryRelation queryRelation = queryStatement.getQueryRelation();

            LogicalPlan subqueryPlan = getLogicalPlan(queryRelation, session, context.builder.getExpressionMapping(),
                    context.cteContext);
            if (subqueryPlan.getOutputColumn().size() != 1) {
                throw new SemanticException("Scalar subquery should output one column");
            }

            ScalarOperator subqueryOutput = subqueryPlan.getOutputColumn().get(0);

            /*
             * The scalar aggregation in the subquery will be converted into a vector aggregation in scalar sub-query
             * but the scalar aggregation will return at least one row.
             * So we need to do special processing on count,
             * other aggregate functions do not need special processing because they return NULL
             */
            if (!subqueryPlan.getCorrelation().isEmpty() && queryRelation instanceof SelectRelation &&
                    ((SelectRelation) queryRelation).hasAggregation() &&
                    ((SelectRelation) queryRelation).getAggregate().get(0).getFnName().getFunction()
                            .equalsIgnoreCase(FunctionSet.COUNT)) {

                subqueryOutput = new CallOperator(FunctionSet.IFNULL, Type.BIGINT,
                        Lists.newArrayList(subqueryOutput, ConstantOperator.createBigint(0)),
                        Expr.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {Type.BIGINT, Type.BIGINT},
                                Function.CompareMode.IS_IDENTICAL));
            }

            // un-correlation scalar query, set outer columns
            ColumnRefSet outerUsedColumns = new ColumnRefSet();
            if (subqueryPlan.getCorrelation().isEmpty()) {
                for (ScalarOperator outer : context.outerExprs) {
                    outerUsedColumns.union(outer.getUsedColumns());
                }
            }

            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(subqueryOutput, subqueryOutput.getType(), subqueryOutput.isNullable());

            context.builder.getExpressionMapping().put(operator, outputPredicateRef);

            // The Apply's output column is the subquery's result
            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(subqueryOutput)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti)
                    .setUnCorrelationSubqueryPredicateColumns(outerUsedColumns)
                    .setNeedCheckMaxRows(true).build();
            context.builder =
                    new OptExprBuilder(applyOperator, Arrays.asList(context.builder, subqueryPlan.getRootBuilder()),
                            context.builder.getExpressionMapping());
            return context.builder;
        }
    }

    private static class SubqueryContext {
        public OptExprBuilder builder;
        public boolean useSemiAnti;
        public CTETransformerContext cteContext;
        public List<ScalarOperator> outerExprs;

        public SubqueryContext(OptExprBuilder builder, boolean useSemiAnti, CTETransformerContext cteContext,
                               List<ScalarOperator> outerExprs) {
            this.builder = builder;
            this.useSemiAnti = useSemiAnti;
            this.cteContext = cteContext;
            this.outerExprs = outerExprs;
        }

        public SubqueryContext(OptExprBuilder builder, boolean useSemiAnti, CTETransformerContext cteContext) {
            this(builder, useSemiAnti, cteContext, Collections.emptyList());
        }
    }
}
