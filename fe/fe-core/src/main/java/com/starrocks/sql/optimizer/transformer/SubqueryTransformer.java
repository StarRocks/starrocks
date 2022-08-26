// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SubqueryTransformer {
    private final ConnectContext session;

    public SubqueryTransformer(ConnectContext session) {
        this.session = session;
    }

    public OptExprBuilder handleSubqueries(ColumnRefFactory columnRefFactory, OptExprBuilder subOpt, Expr expression,
                                           CTETransformerContext cteContext) {
        if (subOpt.getExpressionMapping().hasExpression(expression)) {
            return subOpt;
        }

        FilterWithSubqueryHandler handler = new FilterWithSubqueryHandler(columnRefFactory, session);
        subOpt = expression.accept(handler, new SubqueryContext(subOpt, true, cteContext));

        return subOpt;
    }

    // Only support scalar-subquery in `SELECT` clause
    public OptExprBuilder handleScalarSubqueries(ColumnRefFactory columnRefFactory, OptExprBuilder subOpt,
                                                 Expr expression, CTETransformerContext cteContext) {
        if (subOpt.getExpressionMapping().hasExpression(expression)) {
            return subOpt;
        }

        FilterWithSubqueryHandler handler = new FilterWithSubqueryHandler(columnRefFactory, session);
        subOpt = expression.accept(handler, new SubqueryContext(subOpt, false, cteContext));

        return subOpt;
    }

    public ScalarOperator rewriteSubqueryScalarOperator(Expr predicate, OptExprBuilder subOpt,
                                                        List<ColumnRefOperator> correlation) {
        ScalarOperator scalarPredicate =
                SqlToScalarOperatorTranslator.translate(predicate, subOpt.getExpressionMapping(), correlation);

        List<InPredicate> inPredicates = Lists.newArrayList();
        predicate.collect(InPredicate.class, inPredicates);
        List<ExistsPredicate> existsSubquerys = Lists.newArrayList();
        predicate.collect(ExistsPredicate.class, existsSubquerys);

        List<ScalarOperator> s = Utils.extractConjuncts(scalarPredicate);
        for (InPredicate e : inPredicates) {
            if (!(e.getChild(1) instanceof Subquery)) {
                continue;
            }

            if (((Subquery) e.getChild(1)).isUseSemiAnti()) {
                ColumnRefOperator columnRefOperator = subOpt.getExpressionMapping().get(e);
                s.remove(columnRefOperator);
            }
        }

        for (ExistsPredicate e : existsSubquerys) {
            Preconditions.checkState(e.getChild(0) instanceof Subquery);
            if (((Subquery) e.getChild(0)).isUseSemiAnti()) {
                ColumnRefOperator columnRefOperator = subOpt.getExpressionMapping().get(e);
                s.remove(columnRefOperator);
            }
        }

        scalarPredicate = Utils.compoundAnd(s);

        return scalarPredicate;
    }

    public Expr rewriteJoinOnPredicate(Expr predicate) {
        if (predicate.getSubquery() == null) {
            return predicate;
        }

        List<Expr> conjuncts = AnalyzerUtils.extractConjuncts(predicate);
        List<Expr> newConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            List<InPredicate> inPredicates = Lists.newArrayList();
            conjunct.collect(InPredicate.class, inPredicates);
            List<ExistsPredicate> existsSubquerys = Lists.newArrayList();
            conjunct.collect(ExistsPredicate.class, existsSubquerys);

            boolean skip = false;
            for (InPredicate e : inPredicates) {
                if (!(e.getChild(1) instanceof Subquery)) {
                    continue;
                }

                if (((Subquery) e.getChild(1)).isUseSemiAnti()) {
                    skip = true;
                }
            }
            for (ExistsPredicate e : existsSubquerys) {
                Preconditions.checkState(e.getChild(0) instanceof Subquery);
                if (((Subquery) e.getChild(0)).isUseSemiAnti()) {
                    skip = true;
                }
            }
            if (!skip) {
                newConjuncts.add(conjunct);
            }
        }

        return AnalyzerUtils.compoundAnd(newConjuncts);
    }

    private static class SubqueryContext {
        public OptExprBuilder builder;
        public boolean useSemiAnti;
        public CTETransformerContext cteContext;
        public List<Expr> outerExprs;

        public SubqueryContext(OptExprBuilder builder, boolean useSemiAnti,
                               CTETransformerContext cteContext) {
            this.builder = builder;
            this.useSemiAnti = useSemiAnti;
            this.cteContext = cteContext;
            this.outerExprs = Collections.emptyList();
        }

        public SubqueryContext(OptExprBuilder builder, boolean useSemiAnti,
                               CTETransformerContext cteContext, List<Expr> outerExprs) {
            this.builder = builder;
            this.useSemiAnti = useSemiAnti;
            this.cteContext = cteContext;
            this.outerExprs = outerExprs;
        }
    }

    private static class FilterWithSubqueryHandler extends AstVisitor<OptExprBuilder, SubqueryContext> {
        private final ColumnRefFactory columnRefFactory;
        private final ConnectContext session;

        public FilterWithSubqueryHandler(ColumnRefFactory columnRefFactory, ConnectContext session) {
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
        public OptExprBuilder visitExpression(Expr node, SubqueryContext context) {
            OptExprBuilder builder = context.builder;
            if (builder.getExpressionMapping().hasExpression(node)) {
                return builder;
            }

            List<Expr> outerExprs = Collections.emptyList();
            if (node.getChildren().stream().filter(Subquery.class::isInstance).count() == 1) {
                outerExprs = node.getChildren().stream().filter(c -> !(c instanceof Subquery))
                        .collect(Collectors.toList());
            }
            for (Expr child : node.getChildren()) {
                builder = visit(child, new SubqueryContext(builder, false, context.cteContext, outerExprs));
            }

            return builder;
        }

        @Override
        public OptExprBuilder visitInPredicate(InPredicate inPredicate, SubqueryContext context) {
            if (!(inPredicate.getChild(1) instanceof Subquery)) {
                return context.builder;
            }

            QueryStatement queryStatement = ((Subquery) inPredicate.getChild(1)).getQueryStatement();
            QueryRelation qb = queryStatement.getQueryRelation();
            LogicalPlan subqueryPlan = getLogicalPlan(qb, session, context.builder.getExpressionMapping(),
                    context.cteContext);
            if (qb instanceof SelectRelation &&
                    !subqueryPlan.getCorrelation().isEmpty() && ((SelectRelation) qb).hasAggregation()) {
                throw new SemanticException(
                        "Unsupported correlated in predicate subquery with grouping or aggregation");
            }

            ScalarOperator leftColRef = SqlToScalarOperatorTranslator
                    .translate(inPredicate.getChild(0), context.builder.getExpressionMapping());
            List<ColumnRefOperator> rightColRef = subqueryPlan.getOutputColumn();
            if (rightColRef.size() > 1) {
                throw new SemanticException("subquery must return a single column when used in InPredicate");
            }

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator inPredicateOperator =
                    rewriter.rewrite(new InPredicateOperator(inPredicate.isNotIn(), leftColRef, rightColRef.get(0)),
                            ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(inPredicate, inPredicate.getType(), inPredicate.isNullable());
            context.builder.getExpressionMapping().put(inPredicate, outputPredicateRef);

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(inPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();
            context.builder =
                    new OptExprBuilder(applyOperator, Arrays.asList(context.builder, subqueryPlan.getRootBuilder()),
                            context.builder.getExpressionMapping());

            ((Subquery) inPredicate.getChild(1)).setUseSemiAnti(context.useSemiAnti);
            return context.builder;
        }

        @Override
        public OptExprBuilder visitExistsPredicate(ExistsPredicate existsPredicate, SubqueryContext context) {
            Preconditions.checkState(existsPredicate.getChild(0) instanceof Subquery);

            QueryRelation qb = ((Subquery) existsPredicate.getChild(0)).getQueryStatement().getQueryRelation();
            LogicalPlan subqueryPlan = getLogicalPlan(qb, session, context.builder.getExpressionMapping(),
                    context.cteContext);

            List<ColumnRefOperator> rightColRef = subqueryPlan.getOutputColumn();

            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(existsPredicate, existsPredicate.getType(), existsPredicate.isNullable());
            context.builder.getExpressionMapping().put(existsPredicate, outputPredicateRef);

            ExistsPredicateOperator existsPredicateOperator =
                    new ExistsPredicateOperator(existsPredicate.isNotExists(), rightColRef.get(0));

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(existsPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();
            context.builder =
                    new OptExprBuilder(applyOperator, Arrays.asList(context.builder, subqueryPlan.getRootBuilder()),
                            context.builder.getExpressionMapping());

            ((Subquery) existsPredicate.getChild(0)).setUseSemiAnti(context.useSemiAnti);
            return context.builder;
        }

        @Override
        public OptExprBuilder visitBetweenPredicate(BetweenPredicate node, SubqueryContext context) {
            if (node.isNotBetween()) {
                Expr lower = new BinaryPredicate(BinaryPredicate.Operator.LT, node.getChild(0), node.getChild(1));
                Expr upper = new BinaryPredicate(BinaryPredicate.Operator.GT, node.getChild(0), node.getChild(2));
                Expr compound = new CompoundPredicate(CompoundPredicate.Operator.OR, lower, upper);

                return compound.accept(this, context);
            } else {
                Expr lower = new BinaryPredicate(BinaryPredicate.Operator.GE, node.getChild(0), node.getChild(1));
                Expr upper = new BinaryPredicate(BinaryPredicate.Operator.LE, node.getChild(0), node.getChild(2));
                Expr compound = new CompoundPredicate(CompoundPredicate.Operator.AND, lower, upper);

                return compound.accept(this, context);
            }
        }

        @Override
        public OptExprBuilder visitCompoundPredicate(CompoundPredicate node, SubqueryContext context) {
            OptExprBuilder builder = context.builder;
            if (CompoundPredicate.Operator.OR == node.getOp()) {
                builder = node.getChild(0).accept(this, new SubqueryContext(builder, false, context.cteContext));
                builder = node.getChild(1).accept(this, new SubqueryContext(builder, false, context.cteContext));
            } else if (CompoundPredicate.Operator.AND == node.getOp()) {
                // And Scope extend from parents
                builder = node.getChild(0)
                        .accept(this, new SubqueryContext(builder, context.useSemiAnti, context.cteContext));
                builder = node.getChild(1)
                        .accept(this, new SubqueryContext(builder, context.useSemiAnti, context.cteContext));
            } else {
                builder = node.getChild(0).accept(this, new SubqueryContext(builder, false, context.cteContext));
            }

            return builder;
        }

        // scalar subquery
        @Override
        public OptExprBuilder visitSubquery(Subquery subquery, SubqueryContext context) {
            QueryStatement queryStatement = subquery.getQueryStatement();
            QueryRelation queryRelation = queryStatement.getQueryRelation();

            LogicalPlan subqueryPlan =
                    getLogicalPlan(queryRelation, session, context.builder.getExpressionMapping(),
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
                for (Expr outer : context.outerExprs) {
                    outerUsedColumns.union(SqlToScalarOperatorTranslator
                            .translate(outer, context.builder.getExpressionMapping())
                            .getUsedColumns());
                }
            }

            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(subquery, subquery.getType(), subquery.isNullable());
            context.builder.getExpressionMapping().put(subquery, outputPredicateRef);

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
}

