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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ConvertIntervalExpr;
import com.starrocks.analysis.DictQueryExpr;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictQueryOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static java.util.Objects.requireNonNull;

/**
 * Translator from Expr to ScalarOperator
 */
public final class SqlToScalarOperatorTranslator {
    private static final Logger LOG = LogManager.getLogger(SqlToScalarOperatorTranslator.class);

    private SqlToScalarOperatorTranslator() {
    }

    public static ColumnRefOperator findOrCreateColumnRefForExpr(Expr expression, ExpressionMapping expressionMapping,
                                                                 Map<ColumnRefOperator, ScalarOperator> projections,
                                                                 ColumnRefFactory columnRefFactory) {
        ColumnRefOperator columnRef;
        ScalarOperator scalarOperator =
                SqlToScalarOperatorTranslator.translate(expression, expressionMapping, columnRefFactory);
        if (scalarOperator.isColumnRef()) {
            columnRef = (ColumnRefOperator) scalarOperator;
        } else if (scalarOperator.isVariable() && projections.containsValue(scalarOperator)) {
            columnRef = projections.entrySet().stream().filter(e -> scalarOperator.equals(e.getValue())).findAny().map(
                    Map.Entry::getKey).orElse(null);
            Preconditions.checkNotNull(columnRef);
        } else {
            columnRef = columnRefFactory.create(expression, expression.getType(), scalarOperator.isNullable());
        }
        projections.put(columnRef, scalarOperator);
        return columnRef;
    }

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping,
                                           ColumnRefFactory columnRefFactory) {
        return translate(expression, expressionMapping, null, columnRefFactory);
    }

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping,
                                           List<ColumnRefOperator> correlation, ColumnRefFactory columnRefFactory) {
        return translate(expression, expressionMapping, correlation, columnRefFactory,
                null, null, null, null, false);
    }

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping,
                                           ColumnRefFactory columnRefFactory,
                                           ConnectContext session, CTETransformerContext cteContext,
                                           OptExprBuilder builder,
                                           Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders,
                                           boolean useSemiAnti) {
        List<ColumnRefOperator> correlation = Lists.newArrayList();
        ScalarOperator scalarOperator = translate(expression, expressionMapping, correlation, columnRefFactory,
                session, cteContext, builder, subqueryPlaceholders, useSemiAnti);
        if (!correlation.isEmpty()) {
            throw unsupportedException("Only support use correlated columns in the where clause of subqueries");
        }
        return scalarOperator;
    }

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping,
                                           List<ColumnRefOperator> correlation, ColumnRefFactory columnRefFactory,
                                           ConnectContext session, CTETransformerContext cteContext,
                                           OptExprBuilder builder,
                                           Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders,
                                           boolean useSemiAnti) {
        ColumnRefOperator columnRefOperator = expressionMapping.get(expression);
        if (columnRefOperator != null) {
            return columnRefOperator;
        }

        Visitor visitor = new Visitor(expressionMapping, columnRefFactory, correlation,
                session, cteContext, builder, subqueryPlaceholders);

        List<Subquery> subqueries = Lists.newArrayList();
        expression.collect(Subquery.class, subqueries);
        Context ctx = new Context(!subqueries.isEmpty(), useSemiAnti, Collections.emptyList());

        ScalarOperator result = visitor.visit(expression, ctx);

        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);

        requireNonNull(result, "translated expression is null");
        return result;
    }

    public static ScalarOperator translate(Expr expression) {
        IgnoreSlotVisitor visitor = new IgnoreSlotVisitor();
        return visitor.visit(expression, new Context());
    }

    private static final class Context {

        public final boolean hasSubquery;
        public final boolean useSemiAnti;

        // only for recording outer exprs to an un-correlate subquery in a predicate
        public final List<Expr> outerExprs;

        public Context() {
            this.hasSubquery = false;
            this.useSemiAnti = false;
            this.outerExprs = Collections.emptyList();
        }

        public Context(boolean hasSubquery, boolean useSemiAnti, List<Expr> outerExprs) {
            this.hasSubquery = hasSubquery;
            this.useSemiAnti = useSemiAnti;
            this.outerExprs = outerExprs;
        }

        public Context clone(Expr node) {
            if (!hasSubquery) {
                return this;
            }
            List<Expr> res = findOuterExprs(node);
            if (!this.useSemiAnti && outerExprs.isEmpty()) {
                return this;
            }
            return new Context(true, false, res);
        }

        private List<Expr> findOuterExprs(Expr node) {
            List<Expr> res = Lists.newArrayList();
            if (node instanceof Predicate && node.getChildren().stream().anyMatch(Subquery.class::isInstance)) {
                for (Expr child : node.getChildren()) {
                    if (collectSubqueryNum(child) == 0) {
                        res.add(child);
                    }
                }
            }
            return res;
        }

        private int collectSubqueryNum(Expr node) {
            int num = 0;
            for (Expr child : node.getChildren()) {
                if (num > 0) {
                    return num;
                }
                if (child instanceof Subquery) {
                    num++;
                } else {
                    num += collectSubqueryNum(child);
                }
            }
            return num;
        }
    }

    private static class Visitor extends AstVisitor<ScalarOperator, Context> {
        private ExpressionMapping expressionMapping;
        private final ColumnRefFactory columnRefFactory;
        private final List<ColumnRefOperator> correlation;
        private final ConnectContext session;
        private final CTETransformerContext cteContext;
        public final OptExprBuilder builder;
        public final Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders;

        public Visitor(ExpressionMapping expressionMapping, ColumnRefFactory columnRefFactory,
                       List<ColumnRefOperator> correlation, ConnectContext session,
                       CTETransformerContext cteContext, OptExprBuilder builder,
                       Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders) {
            this.expressionMapping = expressionMapping;
            this.columnRefFactory = columnRefFactory;
            if (correlation == null) {
                this.correlation = Lists.newArrayList();
            } else {
                this.correlation = correlation;
            }
            this.session = session;
            this.cteContext = cteContext;
            this.builder = builder;
            this.subqueryPlaceholders = subqueryPlaceholders;
        }

        @Override
        public ScalarOperator visit(ParseNode node, Context context) {
            Expr expr = (Expr) node;
            if (expressionMapping.get(expr) != null && !(expr.isConstant())) {
                return expressionMapping.get(expr);
            }

            return super.visit(node, context);

        }
        @Override
        public ScalarOperator visitParameterExpr(Parameter node, Context context) {
            if (node.getExpr() == null) {
                throw new SemanticException("Unknown parameter");
            }
            return visit(node.getExpr());
        }

        @Override
        public ScalarOperator visitSlot(SlotRef node, Context context) {
            ResolvedField resolvedField =
                    expressionMapping.getScope().resolveField(node, expressionMapping.getOuterScopeRelationId());
            ColumnRefOperator columnRefOperator =
                    expressionMapping.getColumnRefWithIndex(resolvedField.getRelationFieldIndex());

            if (!expressionMapping.getScope().isLambdaScope() &&
                    resolvedField.getScope().getRelationId().equals(expressionMapping.getOuterScopeRelationId())) {
                correlation.add(columnRefOperator);
            }

            // If origin type is struct type, means that node contains subfield access
            if (node.getTrueOriginType().isStructType()) {
                Preconditions.checkArgument(node.getUsedStructFieldPos() != null, "StructType SlotRef must have" +
                        "an non-empty usedStructFiledPos!");
                Preconditions.checkArgument(node.getUsedStructFieldPos().size() > 0);
                return SubfieldOperator.build(columnRefOperator, node.getOriginType(), node.getUsedStructFieldPos());
            } else {
                return columnRefOperator;
            }
        }

        @Override
        public ScalarOperator visitSubfieldExpr(SubfieldExpr node, Context context) {
            Preconditions.checkArgument(node.getChildren().size() == 1);
            ScalarOperator child = visit(node.getChild(0), context);
            return new SubfieldOperator(child, node.getType(), node.getFieldNames());
        }

        @Override
        public ScalarOperator visitFieldReference(FieldReference node, Context context) {
            ColumnRefOperator scalarOperator = expressionMapping.getColumnRefWithIndex(node.getFieldIndex());
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitArrayExpr(ArrayExpr node, Context context) {
            List<ScalarOperator> arrayElements = new ArrayList<>();
            for (Expr expr : node.getChildren()) {
                arrayElements.add(visit(expr, context.clone(node)));
            }

            return new ArrayOperator(node.getType(), node.isNullable(), arrayElements);
        }

        @Override
        public ScalarOperator visitMapExpr(MapExpr node, Context context) {
            List<ScalarOperator> mapElements = new ArrayList<>();
            for (Expr expr : node.getChildren()) {
                mapElements.add(visit(expr, context.clone(node)));
            }
            return new MapOperator(node.getType(), mapElements);
        }

        @Override
        public ScalarOperator visitCollectionElementExpr(CollectionElementExpr node, Context context) {
            Preconditions.checkState(node.getChildren().size() == 2);
            ScalarOperator collectionOperator = visit(node.getChild(0), context.clone(node));
            ScalarOperator subscriptOperator = visit(node.getChild(1), context.clone(node));
            return new CollectionElementOperator(node.getType(), collectionOperator, subscriptOperator);
        }

        @Override
        public ScalarOperator visitArraySliceExpr(ArraySliceExpr node, Context context) {
            ScalarOperator arrayOperator = visit(node.getChild(0), context.clone(node));
            ScalarOperator lowerBound = visit(node.getChild(1), context.clone(node));
            ScalarOperator upperBound = visit(node.getChild(2), context.clone(node));
            return new ArraySliceOperator(node.getType(), Lists.newArrayList(arrayOperator, lowerBound, upperBound));
        }

        @Override
        public ScalarOperator visitArrowExpr(ArrowExpr node, Context context) {
            Preconditions.checkArgument(node.getChildren().size() == 2);

            // TODO(mofei) make it more elegant
            Function func = GlobalStateMgr.getCurrentState().getFunction(FunctionSet.JSON_QUERY_FUNC,
                    Function.CompareMode.IS_IDENTICAL);
            Preconditions.checkNotNull(func, "json_query function not exists");

            List<ScalarOperator> arguments = node.getChildren()
                    .stream()
                    .map(child -> visit(child, context.clone(node)))
                    .collect(Collectors.toList());
            return new CallOperator(
                    FunctionSet.JSON_QUERY,
                    Type.JSON,
                    arguments,
                    func);
        }

        @Override
        public ScalarOperator visitLambdaFunctionExpr(LambdaFunctionExpr node,
                                                      Context context) {
            Preconditions.checkArgument(node.getChildren().size() >= 2);
            List<ColumnRefOperator> refs = Lists.newArrayList();
            List<LambdaArgument> args = Lists.newArrayList();
            for (int i = 1; i < node.getChildren().size(); ++i) {
                args.add((LambdaArgument) node.getChild(i));
                refs.add((ColumnRefOperator) visit(node.getChild(i), context.clone(node)));
            }
            Scope scope = new Scope(args, expressionMapping.getScope());
            ExpressionMapping old = expressionMapping;
            expressionMapping = new ExpressionMapping(scope, refs, expressionMapping);
            ScalarOperator lambda = visit(node.getChild(0), context.clone(node));
            expressionMapping = old; // recover it
            return new LambdaFunctionOperator(refs, lambda, Type.FUNCTION);
        }

        @Override
        public ScalarOperator visitLambdaArguments(LambdaArgument node, Context context) {
            // To avoid the ids of lambda arguments are different after each visit()
            if (node.getTransformed() == null) {
                node.setTransformed(columnRefFactory.create(node.getName(), node.getType(), node.isNullable(), true));
            }
            return node.getTransformed();
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicate node,
                                                     Context context) {
            switch (node.getOp()) {
                case AND:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                            visit(node.getChild(0), context), visit(node.getChild(1), context));
                case OR:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                            visit(node.getChild(0), context.clone(node)),
                            visit(node.getChild(1), context.clone(node)));
                case NOT:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                            visit(node.getChild(0), context.clone(node)));
                default:
                    throw new UnsupportedOperationException("nonsupport compound predicate type");
            }
        }

        @Override
        public ScalarOperator visitBetweenPredicate(BetweenPredicate node, Context context)
                throws SemanticException {
            return new BetweenPredicateOperator(node.isNotBetween(),
                    visit(node.getChild(0), context.clone(node)),
                    visit(node.getChild(1), context.clone(node)),
                    visit(node.getChild(2), context.clone(node)));
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicate node, Context context) {
            return new BinaryPredicateOperator(node.getOp(),
                    visit(node.getChild(0), context.clone(node)),
                    visit(node.getChild(1), context.clone(node)));
        }

        @Override
        public ScalarOperator visitExistsPredicate(ExistsPredicate node, Context context)
                throws SemanticException {
            if (!(node.getChild(0) instanceof Subquery)) {
                return new ExistsPredicateOperator(node.isNotExists(), node.getChildren()
                        .stream()
                        .map(child -> visit(child, context.clone(node)))
                        .toArray(ScalarOperator[]::new));
            }

            QueryStatement queryStatement = ((Subquery) node.getChild(0)).getQueryStatement();
            QueryRelation relation = queryStatement.getQueryRelation();
            LogicalPlan subqueryPlan = SubqueryUtils.getLogicalPlan(session, cteContext, columnRefFactory,
                    relation, builder.getExpressionMapping());

            List<ColumnRefOperator> rightColRefs = subqueryPlan.getOutputColumn();
            ColumnRefOperator rightColRef = rightColRefs.get(0);

            ExistsPredicateOperator existsPredicateOperator =
                    new ExistsPredicateOperator(node.isNotExists(), rightColRef);

            ColumnRefOperator outputPredicateRef = columnRefFactory.create(node, node.getType(), node.isNullable());
            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(existsPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();

            SubqueryOperator subqueryOperator = new SubqueryOperator(rightColRef.getType(), queryStatement,
                    applyOperator, subqueryPlan.getRootBuilder());

            subqueryPlaceholders.put(outputPredicateRef, subqueryOperator);

            return outputPredicateRef;
        }

        @Override
        public ScalarOperator visitArithmeticExpr(ArithmeticExpr node, Context context) {
            if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.BINARY_INFIX) {
                ScalarOperator left = visit(node.getChild(0), context.clone(node));
                ScalarOperator right = visit(node.getChild(1), context.clone(node));

                return new CallOperator(node.getOp().getName(), node.getType(), Lists.newArrayList(left, right),
                        node.getFn());
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_PREFIX) {
                ScalarOperator child = visit(node.getChild(0), context.clone(node));
                return new CallOperator(node.getOp().getName(), node.getType(), Lists.newArrayList(child),
                        node.getFn());
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_POSTFIX) {
                throw unsupportedException("nonsupport arithmetic expr");
            } else {
                throw unsupportedException("nonsupport arithmetic expr");
            }
        }

        @Override
        public ScalarOperator visitTimestampArithmeticExpr(TimestampArithmeticExpr node,
                                                           Context context) {
            List<ScalarOperator> arguments = Lists.newArrayList();
            for (Expr argument : node.getChildren()) {
                arguments.add(visit(argument, context.clone(node)));
            }

            return new CallOperator(node.getFn().getFunctionName().getFunction(), node.getType(), arguments,
                    node.getFn());
        }

        @Override
        public ScalarOperator visitConvertIntervalExpr(ConvertIntervalExpr node, Context context) {
            List<ScalarOperator> arguments = Lists.newArrayList();
            for (Expr argument : node.getChildren()) {
                arguments.add(visit(argument, context.clone(node)));
            }
            return new CallOperator(node.getFn().getFunctionName().getFunction(), node.getType(), arguments, node.getFn());

        }

        private LogicalPlan getSubqueryPlan(QueryStatement queryStatement) {
            QueryRelation relation = queryStatement.getQueryRelation();
            LogicalPlan subqueryPlan = SubqueryUtils.getLogicalPlan(session, cteContext, columnRefFactory,
                    relation, builder.getExpressionMapping());

            if (relation instanceof SelectRelation &&
                    !subqueryPlan.getCorrelation().isEmpty() && ((SelectRelation) relation).hasAggregation()) {
                throw new SemanticException(
                        "Unsupported correlated in predicate subquery with grouping or aggregation");
            }
            return subqueryPlan;
        }

        @Override
        public ScalarOperator visitMultiInPredicate(MultiInPredicate node, Context context) throws SemanticException {
            if (!(node.getChild(node.getNumberOfColumns()) instanceof Subquery)) {
                throw new SemanticException("multi-column IN predicate is only supported for subquery expressions");

            }
            QueryStatement queryStatement = ((Subquery) node.getChild(node.getNumberOfColumns())).getQueryStatement();
            LogicalPlan subqueryPlan = getSubqueryPlan(queryStatement);
            List<ColumnRefOperator> leftCorrelationColumns = Lists.newArrayList();
            List<ScalarOperator> leftExprs = Lists.newArrayList();
            for (int i = 0; i < node.getNumberOfColumns(); ++i) {
                ScalarOperator leftColRef = SqlToScalarOperatorTranslator.translate(node.getChild(i),
                        builder.getExpressionMapping(), leftCorrelationColumns, columnRefFactory);
                if (leftCorrelationColumns.size() > 0) {
                    throw new SemanticException("Unsupported complex nested in-subquery");
                }
                leftExprs.add(leftColRef);
            }
            List<ColumnRefOperator> rightColRefs = subqueryPlan.getOutputColumn();
            if (rightColRefs.size() != leftExprs.size()) {
                throw new SemanticException(
                        "subquery must return the same number of columns as provided by the IN predicate");
            }
            ScalarOperator inPredicateOperator = new MultiInPredicateOperator(node.isNotIn(), leftExprs, rightColRefs);
            ColumnRefOperator outputPredicateRef = columnRefFactory.create(inPredicateOperator,
                    inPredicateOperator.getType(), inPredicateOperator.isNullable());
            ((Subquery) node.getChild(node.getNumberOfColumns())).setUseSemiAnti(context.useSemiAnti);

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(inPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();

            // Note that the subquery operator type is not accurate, but we should never use it: MultiInPredicate should
            // be replaced by a semi-join earlier in logical planning. In particular, ImplicitCast rule throws an unhandled
            // exception for this operator.
            SubqueryOperator subqueryOperator = new SubqueryOperator(rightColRefs.get(0).getType(), queryStatement,
                    applyOperator, subqueryPlan.getRootBuilder());

            subqueryPlaceholders.put(outputPredicateRef, subqueryOperator);
            return outputPredicateRef;
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicate node, Context context)
                throws SemanticException {
            List<Expr> lhsSubQueries = Lists.newArrayList();
            node.getChild(0).collect(Subquery.class, lhsSubQueries);
            if (!lhsSubQueries.isEmpty()) {
                throw new SemanticException("Subquery in left-side child of in-predicate is not supported");
            }
            if (!(node.getChild(1) instanceof Subquery)) {
                return new InPredicateOperator(node.isNotIn(),
                        node.getChildren().stream()
                                .map(child -> visit(child, context))
                                .toArray(ScalarOperator[]::new));
            }

            QueryStatement queryStatement = ((Subquery) node.getChild(1)).getQueryStatement();
            LogicalPlan subqueryPlan = getSubqueryPlan(queryStatement);

            List<ColumnRefOperator> leftCorrelationColumns = Lists.newArrayList();
            ScalarOperator leftColRef = SqlToScalarOperatorTranslator.translate(node.getChild(0),
                    builder.getExpressionMapping(), leftCorrelationColumns, columnRefFactory);

            if (leftCorrelationColumns.size() > 0) {
                throw new SemanticException("Unsupported complex nested in-subquery");
            }

            List<ColumnRefOperator> rightColRefs = subqueryPlan.getOutputColumn();
            if (rightColRefs.size() > 1) {
                throw new SemanticException("subquery must return a single column when used in InPredicate");
            }
            ColumnRefOperator rightColRef = rightColRefs.get(0);

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator inPredicateOperator =
                    rewriter.rewrite(new InPredicateOperator(node.isNotIn(), true, leftColRef, rightColRef),
                            ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            ColumnRefOperator outputPredicateRef = columnRefFactory.create(inPredicateOperator,
                    inPredicateOperator.getType(), inPredicateOperator.isNullable());
            ((Subquery) node.getChild(1)).setUseSemiAnti(context.useSemiAnti);

            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(inPredicateOperator)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti).build();

            SubqueryOperator subqueryOperator = new SubqueryOperator(rightColRef.getType(), queryStatement,
                    applyOperator, subqueryPlan.getRootBuilder());

            subqueryPlaceholders.put(outputPredicateRef, subqueryOperator);

            return outputPredicateRef;
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicate node, Context context)
                throws SemanticException {
            return new IsNullPredicateOperator(node.isNotNull(),
                    visit(node.getChild(0), context.clone(node)));
        }

        @Override
        public ScalarOperator visitLikePredicate(LikePredicate node, Context context)
                throws SemanticException {
            ScalarOperator[] children = node.getChildren()
                    .stream()
                    .map(child -> visit(child, context.clone(node)))
                    .toArray(ScalarOperator[]::new);

            if (LikePredicate.Operator.LIKE.equals(node.getOp())) {
                return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, children);
            } else {
                return new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, children);
            }
        }

        @Override
        public ScalarOperator visitLiteral(LiteralExpr node, Context context) {
            if (node instanceof NullLiteral) {
                return ConstantOperator.createNull(node.getType());
            }

            return ConstantOperator.createObject(node.getRealObjectValue(), node.getType());
        }

        @Override
        public ScalarOperator visitFunctionCall(FunctionCallExpr node, Context context) {
            List<ScalarOperator> arguments = node.getChildren()
                    .stream()
                    .map(child -> visit(child, context.clone(node)))
                    .collect(Collectors.toList());

            CallOperator callOperator = new CallOperator(
                    node.getFnName().getFunction(),
                    node.getType(),
                    arguments,
                    node.getFn(),
                    node.getParams().isDistinct());
            callOperator.setHints(node.getHints());
            return callOperator;
        }

        @Override
        public ScalarOperator visitAnalyticExpr(AnalyticExpr node, Context context) {
            FunctionCallExpr functionCallExpr = node.getFnCall();

            List<ScalarOperator> arguments = functionCallExpr.getChildren()
                    .stream()
                    .map(child -> visit(child, context.clone(node)))
                    .collect(Collectors.toList());
            CallOperator callOperator =
                    new CallOperator(functionCallExpr.getFnName().getFunction(), functionCallExpr.getType(), arguments,
                            functionCallExpr.getFn(), functionCallExpr.getParams().isDistinct());
            callOperator.setIgnoreNulls(functionCallExpr.getIgnoreNulls());
            return callOperator;
        }

        @Override
        public ScalarOperator visitCastExpr(CastExpr node, Context context) {
            return new CastOperator(node.getType(), visit(node.getChild(0), context.clone(node)),
                    node.isImplicit());
        }

        @Override
        public ScalarOperator visitCaseWhenExpr(CaseExpr node, Context context) {
            ScalarOperator caseOperator = null;
            ScalarOperator elseOperator = null;
            int start = 0;
            int end = node.getChildren().size();

            if (node.hasCaseExpr()) {
                caseOperator = visit(node.getChild(0), context.clone(node));
                start++;
            }

            if (node.hasElseExpr()) {
                elseOperator = visit(node.getChild(end - 1), context.clone(node));
                end--;
            }

            List<ScalarOperator> when = Lists.newArrayList();
            for (int i = start; i < end; i++) {
                when.add(visit(node.getChild(i), context.clone(node)));
            }

            return new CaseWhenOperator(node.getType(), caseOperator, elseOperator, when);
        }

        @Override
        public ScalarOperator visitInformationFunction(InformationFunction node,
                                                       Context context) {
            if (node.getFuncType().equalsIgnoreCase("CONNECTION_ID")) {
                return new CallOperator(node.getFuncType(), node.getType(), Lists.newArrayList(
                        ConstantOperator.createBigint(node.getIntValue())));
            }

            if (node.getFuncType().equalsIgnoreCase("DATABASE") ||
                    node.getFuncType().equalsIgnoreCase("SCHEMA")) {
                return ConstantOperator.createVarchar(node.getStrValue());
            }

            return new CallOperator(node.getFuncType(), node.getType(), Lists.newArrayList(
                    ConstantOperator.createVarchar(node.getStrValue()),
                    ConstantOperator.createBigint(node.getIntValue())));
        }

        @Override
        public ScalarOperator visitVariableExpr(VariableExpr node, Context context) {
            if (node.isNull()) {
                return ConstantOperator.createNull(node.getType());
            } else {
                return new ConstantOperator(node.getValue(), node.getType());
            }
        }

        @Override
        public ScalarOperator visitGroupingFunctionCall(GroupingFunctionCallExpr node,
                                                        Context context) {
            ColumnRefOperator columnRef = expressionMapping.get(node);
            if (columnRef == null) {
                throw new StarRocksPlannerException("grouping function not translate to column reference",
                        ErrorType.INTERNAL_ERROR);
            }

            return columnRef;
        }

        @Override
        public ScalarOperator visitSubquery(Subquery node, Context context) {
            Preconditions.checkNotNull(context);
            QueryStatement queryStatement = node.getQueryStatement();
            QueryRelation queryRelation = queryStatement.getQueryRelation();

            // For case of set @var = (select v1 from test.t0)
            if (builder == null) {
                LogicalApplyOperator applyOperator = LogicalApplyOperator.builder()
                        .setUseSemiAnti(false)
                        .build();
                return new SubqueryOperator(Type.INVALID, queryStatement, applyOperator, null);
            }

            LogicalPlan subqueryPlan = SubqueryUtils.getLogicalPlan(session, cteContext,
                    columnRefFactory, queryRelation, builder.getExpressionMapping());
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
                    outerUsedColumns.union(
                            SqlToScalarOperatorTranslator.translate(outer, expressionMapping, columnRefFactory)
                                    .getUsedColumns());
                }
            }

            ColumnRefOperator outputPredicateRef =
                    columnRefFactory.create(subqueryOutput, subqueryOutput.getType(), subqueryOutput.isNullable());

            // The Apply's output column is the subquery's result
            LogicalApplyOperator applyOperator = LogicalApplyOperator.builder().setOutput(outputPredicateRef)
                    .setSubqueryOperator(subqueryOutput)
                    .setCorrelationColumnRefs(subqueryPlan.getCorrelation())
                    .setUseSemiAnti(context.useSemiAnti)
                    .setUnCorrelationSubqueryPredicateColumns(outerUsedColumns)
                    .setNeedCheckMaxRows(true).build();

            SubqueryOperator subqueryOperator = new SubqueryOperator(outputPredicateRef.getType(), queryStatement,
                    applyOperator, subqueryPlan.getRootBuilder());

            subqueryPlaceholders.put(outputPredicateRef, subqueryOperator);

            return outputPredicateRef;
        }

        @Override
        public ScalarOperator visitCloneExpr(CloneExpr node, Context context) {
            return new CloneOperator(visit(node.getChild(0), context.clone(node)));
        }

        @Override
        public ScalarOperator visitDictQueryExpr(DictQueryExpr node, Context context) {
            List<ScalarOperator> arguments = node.getChildren()
                    .stream()
                    .map(child -> visit(child, context.clone(node)))
                    .collect(Collectors.toList());
            return new DictQueryOperator(arguments, node.getDictQueryExpr(), node.getFn());
        }
    }

    static class IgnoreSlotVisitor extends Visitor {
        public IgnoreSlotVisitor() {
            super(new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                    new ColumnRefFactory(), Collections.emptyList(),
                    null, null, null, null);
        }

        @Override
        public ScalarOperator visitSlot(SlotRef node, Context context) {
            if (!node.isAnalyzed()) {
                // IgnoreSlotVisitor is for compatibility with some old Analyze logic that has not been migrated.
                // So if you need to visit SlotRef here, it must be the case where the old version of analyzed is true
                // (currently mainly used by some Load logic).
                // TODO: delete old analyze in Load
                LOG.warn("Can't use IgnoreSlotVisitor with not analyzed slot ref: " + node.toSql());
                throw unsupportedException("Can't use IgnoreSlotVisitor with not analyzed slot ref");
            }
            return new ColumnRefOperator(node.getSlotId().asInt(),
                    node.getType(), node.getColumnName(), node.isNullable());
        }
    }
}
