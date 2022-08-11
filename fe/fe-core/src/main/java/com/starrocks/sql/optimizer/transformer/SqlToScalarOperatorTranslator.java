// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LambdaArguments;
import com.starrocks.analysis.LambdaFunction;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ArrayElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
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
    private SqlToScalarOperatorTranslator() {
    }

    public static ColumnRefOperator findOrCreateColumnRefForExpr(Expr expression, ExpressionMapping expressionMapping,
                                                                 Map<ColumnRefOperator, ScalarOperator> projections,
                                                                 ColumnRefFactory columnRefFactory) {
        ColumnRefOperator columnRef;
        ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expression, expressionMapping);
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

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping) {
        List<ColumnRefOperator> correlation = new ArrayList<>();
        ScalarOperator rewriteScalarOp = translate(expression, expressionMapping, correlation);
        if (!correlation.isEmpty()) {
            throw unsupportedException("Only support use correlated columns in the where clause of subqueries");
        }
        return rewriteScalarOp;
    }

    public static ScalarOperator translate(Expr expression, ExpressionMapping expressionMapping,
                                           List<ColumnRefOperator> correlation) {
        ColumnRefOperator columnRefOperator = expressionMapping.get(expression);
        if (columnRefOperator != null) {
            return columnRefOperator;
        }

        Visitor visitor = new Visitor(expressionMapping, correlation);
        ScalarOperator result = visitor.visit(expression, null);

        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);

        requireNonNull(result, "translated expression is null");
        return result;
    }

    public static ScalarOperator translate(Expr expression) {
        IgnoreSlotVisitor visitor = new IgnoreSlotVisitor();
        return visitor.visit(expression, null);
    }

    public static ScalarOperator translateWithoutRewrite(Expr expression, ExpressionMapping expressionMapping) {
        ColumnRefOperator columnRefOperator = expressionMapping.get(expression);
        if (columnRefOperator != null) {
            return columnRefOperator;
        }

        Visitor visitor = new Visitor(expressionMapping, new ArrayList<>());
        ScalarOperator result = visitor.visit(expression, null);

        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);

        requireNonNull(result, "translated expression is null");
        return result;
    }

    static class Visitor extends AstVisitor<ScalarOperator, Void> {
        private final ExpressionMapping expressionMapping;
        private final List<ColumnRefOperator> correlation;

        private Visitor(ExpressionMapping expressionMapping, List<ColumnRefOperator> correlation) {
            this.expressionMapping = expressionMapping;
            this.correlation = correlation;
        }

        @Override
        public ScalarOperator visitExpression(Expr node, Void context) {
            throw unsupportedException(
                    "not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        public ScalarOperator visit(ParseNode node) {
            Expr expr = (Expr) node;
            if (expressionMapping.get(expr) != null && !(expr.isConstant())) {
                return expressionMapping.get(expr);
            }
            return super.visit(node);
        }

        @Override
        public ScalarOperator visitSlot(SlotRef node, Void context) {
            // no nested, so here be identified by unique slot_id
            if (node.getTblNameWithoutAnalyzed().getTbl() == "select") {
                return new ColumnRefOperator(node.getSlotId().asInt(), node.getType(), node.getColumnName(),
                        node.isNullable(), true);
            }
            ResolvedField resolvedField =
                    expressionMapping.getScope().resolveField(node, expressionMapping.getOuterScopeRelationId());
            // TODO(fzh) how to bind the relations between resolvedField and columnRefIndex?
            ColumnRefOperator columnRefOperator =
                    expressionMapping.getColumnRefWithIndex(resolvedField.getRelationFieldIndex());

            if (resolvedField.getScope().getRelationId().equals(expressionMapping.getOuterScopeRelationId())) {
                correlation.add(columnRefOperator);
            }
            return columnRefOperator;
        }

        @Override
        public ScalarOperator visitFieldReference(FieldReference node, Void context) {
            return expressionMapping.getColumnRefWithIndex(node.getFieldIndex());
        }

        @Override
        public ScalarOperator visitArrayExpr(ArrayExpr node, Void context) {
            List<ScalarOperator> arrayElements = new ArrayList<>();
            for (Expr expr : node.getChildren()) {
                arrayElements.add(visit(expr));
            }

            return new ArrayOperator(node.getType(), node.isNullable(), arrayElements);
        }

        @Override
        public ScalarOperator visitArrayElementExpr(ArrayElementExpr node, Void context) {
            Preconditions.checkState(node.getChildren().size() == 2);
            ScalarOperator arrayOperator = visit(node.getChild(0));
            ScalarOperator subscriptOperator = visit(node.getChild(1));
            return new ArrayElementOperator(node.getType(), arrayOperator, subscriptOperator);
        }

        @Override
        public ScalarOperator visitArraySliceExpr(ArraySliceExpr node, Void context) {
            ScalarOperator arrayOperator = visit(node.getChild(0));
            ScalarOperator lowerBound = visit(node.getChild(1));
            ScalarOperator upperBound = visit(node.getChild(2));
            return new ArraySliceOperator(node.getType(), Lists.newArrayList(arrayOperator, lowerBound, upperBound));
        }

        @Override
        public ScalarOperator visitArrowExpr(ArrowExpr node, Void context) {
            Preconditions.checkArgument(node.getChildren().size() == 2);

            // TODO(mofei) make it more elegant
            Function func = GlobalStateMgr.getCurrentState().getFunction(FunctionSet.JSON_QUERY_FUNC,
                    Function.CompareMode.IS_IDENTICAL);
            Preconditions.checkNotNull(func, "json_query function not exists");

            List<ScalarOperator> arguments = node.getChildren().stream().map(this::visit).collect(Collectors.toList());
            return new CallOperator(
                    FunctionSet.JSON_QUERY,
                    Type.JSON,
                    arguments,
                    func);
        }

        @Override
        public ScalarOperator visitLambdaFunction(LambdaFunction node, Void context) {
            Preconditions.checkArgument(node.getChildren().size() == 2);
            LambdaArguments args = (LambdaArguments) node.getChild(0);
            List<ColumnRefOperator> refs = Lists.newArrayList();
            for (int i = 0; i < args.getNames().size(); ++i) {
                ColumnRefOperator ref = new ColumnRefOperator(args.getArguments().get(i).getSlotId(),
                        args.getArguments().get(i).getType(), args.getArguments().get(i).getName(), true, true);
                refs.add(ref);
            }
            ScalarOperator arg = visit(node.getChild(1));
            return new LambdaFunctionOperator(refs, arg, Type.FUNCTION);
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicate node, Void context) {
            switch (node.getOp()) {
                case AND:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                            visit(node.getChild(0)), visit(node.getChild(1)));
                case OR:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                            visit(node.getChild(0)), visit(node.getChild(1)));
                case NOT:
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                            visit(node.getChild(0)));
                default:
                    throw new UnsupportedOperationException("nonsupport compound predicate type");
            }
        }

        @Override
        public ScalarOperator visitBetweenPredicate(BetweenPredicate node, Void context) throws SemanticException {
            return new BetweenPredicateOperator(node.isNotBetween(), visit(node.getChild(0)), visit(node.getChild(1)),
                    visit(node.getChild(2)));
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicate node, Void context) {
            switch (node.getOp()) {
                case EQ:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case NE:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.NE, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case GT:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case GE:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case LT:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case LE:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, visit(node.getChild(0)),
                            visit(node.getChild(1)));
                case EQ_FOR_NULL:
                    return new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                            visit(node.getChild(0)), visit(node.getChild(1)));
                default:
                    throw new UnsupportedOperationException("nonsupport binary predicate type");
            }
        }

        @Override
        public ScalarOperator visitExistsPredicate(ExistsPredicate node, Void context) throws SemanticException {
            ScalarOperator[] list = node.getChildren().stream().map(this::visit).toArray(ScalarOperator[]::new);
            return new ExistsPredicateOperator(node.isNotExists(), list);
        }

        @Override
        public ScalarOperator visitArithmeticExpr(ArithmeticExpr node, Void context) {
            if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.BINARY_INFIX) {
                ScalarOperator left = visit(node.getChild(0));
                ScalarOperator right = visit(node.getChild(1));

                return new CallOperator(node.getOp().getName(), node.getType(), Lists.newArrayList(left, right),
                        node.getFn());
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_PREFIX) {
                ScalarOperator child = visit(node.getChild(0));
                return new CallOperator(node.getOp().getName(), node.getType(), Lists.newArrayList(child),
                        node.getFn());
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_POSTFIX) {
                throw unsupportedException("nonsupport arithmetic expr");
            } else {
                throw unsupportedException("nonsupport arithmetic expr");
            }
        }

        @Override
        public ScalarOperator visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Void context) {
            List<ScalarOperator> arguments = Lists.newArrayList();
            for (Expr argument : node.getChildren()) {
                arguments.add(visit(argument));
            }

            return new CallOperator(node.getFn().getFunctionName().getFunction(), node.getType(), arguments,
                    node.getFn());
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicate node, Void context) throws SemanticException {
            return new InPredicateOperator(node.isNotIn(),
                    node.getChildren().stream().map(this::visit).toArray(ScalarOperator[]::new));
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicate node, Void context) throws SemanticException {
            return new IsNullPredicateOperator(node.isNotNull(), visit(node.getChild(0)));
        }

        @Override
        public ScalarOperator visitLikePredicate(LikePredicate node, Void context) throws SemanticException {
            ScalarOperator[] children = node.getChildren().stream().map(this::visit).toArray(ScalarOperator[]::new);

            if (LikePredicate.Operator.LIKE.equals(node.getOp())) {
                return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, children);
            } else {
                return new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, children);
            }
        }

        @Override
        public ScalarOperator visitLiteral(LiteralExpr node, Void context) {
            if (node instanceof NullLiteral) {
                return ConstantOperator.createNull(node.getType());
            }

            Object value = node.getRealValue();
            Type type = node.getType();

            if (type.isBoolean()) {
                return ConstantOperator.createBoolean((boolean) value);
            } else if (type.isTinyint()) {
                return ConstantOperator.createTinyInt((byte) node.getLongValue());
            } else if (type.isSmallint()) {
                return ConstantOperator.createSmallInt((short) node.getLongValue());
            } else if (type.isInt()) {
                return ConstantOperator.createInt((int) node.getLongValue());
            } else if (type.isBigint()) {
                return ConstantOperator.createBigint(node.getLongValue());
            } else if (type.isLargeint()) {
                return ConstantOperator.createLargeInt((BigInteger) value);
            } else if (type.isFloat()) {
                return ConstantOperator.createFloat((double) value);
            } else if (type.isDouble()) {
                return ConstantOperator.createDouble((double) value);
            } else if (type.isDate()) {
                DateLiteral dl = (DateLiteral) node;
                return ConstantOperator
                        .createDate(LocalDateTime.of((int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay(), 0, 0));
            } else if (type.isDatetime()) {
                DateLiteral dl = (DateLiteral) node;
                return ConstantOperator.createDatetime(LocalDateTime
                        .of((int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay(), (int) dl.getHour(),
                                (int) dl.getMinute(), (int) dl.getSecond()));
            } else if (type.isDecimalOfAnyVersion()) {
                return ConstantOperator.createDecimal((BigDecimal) value, type);
            } else if (type.isVarchar()) {
                return ConstantOperator.createVarchar((String) value);
            } else if (type.isChar()) {
                return ConstantOperator.createChar((String) value);
            } else {
                throw new UnsupportedOperationException("nonsupport constant type");
            }
        }

        @Override
        public ScalarOperator visitFunctionCall(FunctionCallExpr expr, Void context) {
            List<ScalarOperator> arguments = expr.getChildren().stream().map(this::visit).collect(Collectors.toList());
            return new CallOperator(
                    expr.getFnName().getFunction(),
                    expr.getType(),
                    arguments,
                    expr.getFn(),
                    expr.getParams().isDistinct());
        }

        @Override
        public ScalarOperator visitAnalyticExpr(AnalyticExpr expr, Void context) {
            FunctionCallExpr functionCallExpr = expr.getFnCall();

            List<ScalarOperator> arguments =
                    functionCallExpr.getChildren().stream().map(this::visit).collect(Collectors.toList());
            return new CallOperator(
                    functionCallExpr.getFnName().getFunction(),
                    functionCallExpr.getType(),
                    arguments,
                    functionCallExpr.getFn(),
                    functionCallExpr.getParams().isDistinct());
        }

        @Override
        public ScalarOperator visitCastExpr(CastExpr node, Void context) {
            return new CastOperator(node.getType(), visit(node.getChild(0)), node.isImplicit());
        }

        @Override
        public ScalarOperator visitCaseWhenExpr(CaseExpr node, Void context) {
            ScalarOperator caseOperator = null;
            ScalarOperator elseOperator = null;
            int start = 0;
            int end = node.getChildren().size();

            if (node.hasCaseExpr()) {
                caseOperator = visit(node.getChild(0));
                start++;
            }

            if (node.hasElseExpr()) {
                elseOperator = visit(node.getChild(end - 1));
                end--;
            }

            List<ScalarOperator> when = Lists.newArrayList();
            for (int i = start; i < end; i++) {
                when.add(visit(node.getChild(i)));
            }

            return new CaseWhenOperator(node.getType(), caseOperator, elseOperator, when);
        }

        @Override
        public ScalarOperator visitInformationFunction(InformationFunction node, Void context) {
            if (node.getFuncType().equalsIgnoreCase("CONNECTION_ID")) {
                return new CallOperator(node.getFuncType(), node.getType(), Lists.newArrayList(
                        ConstantOperator.createBigint(node.getIntValue())));
            }

            return new CallOperator(node.getFuncType(), node.getType(), Lists.newArrayList(
                    ConstantOperator.createVarchar(node.getStrValue()),
                    ConstantOperator.createBigint(node.getIntValue())));
        }

        @Override
        public ScalarOperator visitVariableExpr(VariableExpr node, Void context) {
            if (node.isNull()) {
                return ConstantOperator.createNull(node.getType());
            }

            switch (node.getType().getPrimitiveType()) {
                case BOOLEAN:
                    return ConstantOperator.createBoolean(node.getBoolValue());
                case TINYINT:
                    return ConstantOperator.createTinyInt((byte) node.getIntValue());
                case SMALLINT:
                    return ConstantOperator.createSmallInt((short) node.getIntValue());
                case INT:
                    return ConstantOperator.createInt((int) node.getIntValue());
                case BIGINT:
                    return ConstantOperator.createBigint(node.getIntValue());
                case FLOAT:
                case DOUBLE:
                    return ConstantOperator.createFloat(node.getFloatValue());
                case CHAR:
                case VARCHAR:
                    return ConstantOperator.createVarchar(node.getStrValue());
                default:
                    throw new StarRocksPlannerException("Not support variable type "
                            + node.getType().getPrimitiveType(), ErrorType.INTERNAL_ERROR);
            }
        }

        @Override
        public ScalarOperator visitGroupingFunctionCall(GroupingFunctionCallExpr node, Void context) {
            ColumnRefOperator columnRef = expressionMapping.get(node);
            if (columnRef == null) {
                throw new StarRocksPlannerException("grouping function not translate to column reference",
                        ErrorType.INTERNAL_ERROR);
            }

            return columnRef;
        }

        @Override
        public ScalarOperator visitSubquery(Subquery node, Void context) {
            throw unsupportedException("complex subquery on " + AST2SQL.toString(node));
        }

        @Override
        public ScalarOperator visitCloneExpr(CloneExpr node, Void context) {
            return new CloneOperator(visit(node.getChild(0)));
        }
    }

    static class IgnoreSlotVisitor extends Visitor {
        public IgnoreSlotVisitor() {
            super(new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                    Collections.EMPTY_LIST);
        }

        @Override
        public ScalarOperator visitSlot(SlotRef node, Void context) {
            return new ColumnRefOperator(node.getSlotId().asInt(),
                    node.getType(), node.getColumnName(), node.isNullable());
        }
    }
}
