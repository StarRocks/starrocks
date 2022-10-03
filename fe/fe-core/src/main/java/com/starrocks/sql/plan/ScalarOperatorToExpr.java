// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.DictMappingExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.PlaceHolderExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
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
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScalarOperatorToExpr {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorToExpr.class);

    public static Expr buildExecExpression(ScalarOperator expression, FormatterContext descTbl) {
        return expression.accept(new Formatter(), descTbl);
    }

    public static class FormatterContext {
        private final Map<ColumnRefOperator, Expr> colRefToExpr;
        private final Map<ColumnRefOperator, ScalarOperator> projectOperatorMap;
        private boolean implicitCast = false;

        public FormatterContext(Map<ColumnRefOperator, Expr> variableToSlotRef) {
            this.colRefToExpr = variableToSlotRef;
            this.projectOperatorMap = new HashMap<>();
        }

        public FormatterContext(Map<ColumnRefOperator, Expr> variableToSlotRef,
                                Map<ColumnRefOperator, ScalarOperator> projectOperatorMap) {
            this.colRefToExpr = variableToSlotRef;
            this.projectOperatorMap = projectOperatorMap;
        }

        public void setImplicitCast(boolean isImplicit) {
            this.implicitCast = isImplicit;
        }
    }

    public static class Formatter extends ScalarOperatorVisitor<Expr, FormatterContext> {
        @Override
        public Expr visit(ScalarOperator scalarOperator, FormatterContext context) {
            throw new UnsupportedOperationException(
                    "not yet implemented: visit scalar operator for " + scalarOperator.getClass().getName());
        }

        @Override
        public Expr visitVariableReference(ColumnRefOperator node, FormatterContext context) {
            if (context.projectOperatorMap.containsKey(node) && context.colRefToExpr.get(node) == null) {
                Expr expr = buildExecExpression(context.projectOperatorMap.get(node), context);
                context.colRefToExpr.put(node, expr);
                return expr;
            }

            Preconditions.checkState(context.colRefToExpr.containsKey(node));
            return context.colRefToExpr.get(node);
        }

        @Override
        public Expr visitArray(ArrayOperator node, FormatterContext context) {
            return new ArrayExpr(node.getType(),
                    node.getChildren().stream().map(e -> buildExecExpression(e, context)).collect(Collectors.toList()));
        }

        @Override
        public Expr visitArrayElement(ArrayElementOperator node, FormatterContext context) {
            return new ArrayElementExpr(node.getType(), buildExecExpression(node.getChild(0), context),
                    buildExecExpression(node.getChild(1), context));
        }

        @Override
        public Expr visitArraySlice(ArraySliceOperator node, FormatterContext context) {
            ArraySliceExpr arraySliceExpr = new ArraySliceExpr(buildExecExpression(node.getChild(0), context),
                    buildExecExpression(node.getChild(1), context),
                    buildExecExpression(node.getChild(2), context));
            arraySliceExpr.setType(node.getType());
            return arraySliceExpr;
        }

        @Override
        public Expr visitConstant(ConstantOperator literal, FormatterContext context) {
            try {
                Type type = literal.getType();
                if (literal.isNull()) {
                    NullLiteral nullLiteral = new NullLiteral();
                    nullLiteral.setType(literal.getType());
                    nullLiteral.setOriginType(Type.NULL);
                    return nullLiteral;
                }

                if (type.isBoolean()) {
                    return new BoolLiteral(literal.getBoolean());
                } else if (type.isTinyint()) {
                    return new IntLiteral(literal.getTinyInt(), Type.TINYINT);
                } else if (type.isSmallint()) {
                    return new IntLiteral(literal.getSmallint(), Type.SMALLINT);
                } else if (type.isInt()) {
                    return new IntLiteral(literal.getInt(), Type.INT);
                } else if (type.isBigint()) {
                    return new IntLiteral(literal.getBigint(), Type.BIGINT);
                } else if (type.isLargeint()) {
                    return new LargeIntLiteral(literal.getLargeInt().toString());
                } else if (type.isFloat()) {
                    return new FloatLiteral(literal.getDouble(), Type.FLOAT);
                } else if (type.isDouble()) {
                    return new FloatLiteral(literal.getDouble(), Type.DOUBLE);
                } else if (type.isDate()) {
                    LocalDateTime ldt = literal.getDate();
                    return new DateLiteral(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth());
                } else if (type.isDatetime()) {
                    LocalDateTime ldt = literal.getDate();
                    return new DateLiteral(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(), ldt.getHour(),
                            ldt.getMinute(), ldt.getSecond());
                } else if (type.isTime()) {
                    return new FloatLiteral((double) literal.getTime(), Type.TIME);
                } else if (type.isDecimalOfAnyVersion()) {
                    DecimalLiteral d = new DecimalLiteral(literal.getDecimal());
                    d.uncheckedCastTo(type);
                    return d;
                } else if (type.isVarchar() || type.isChar()) {
                    return new StringLiteral(literal.getVarchar());
                } else {
                    throw new UnsupportedOperationException("nonsupport constant type: " + type.toSql());
                }
            } catch (Exception ex) {
                throw new UnsupportedOperationException(ex.getMessage());
            }
        }

        @Override
        public Expr visitPredicate(PredicateOperator predicate, FormatterContext context) {
            return null;
        }

        @Override
        public Expr visitCompoundPredicate(CompoundPredicateOperator predicate, FormatterContext context) {
            Expr callExpr;
            if (CompoundPredicateOperator.CompoundType.AND.equals(predicate.getCompoundType())) {
                callExpr = new CompoundPredicate(CompoundPredicate.Operator.AND,
                        buildExecExpression(predicate.getChildren().get(0), context),
                        buildExecExpression(predicate.getChildren().get(1), context));
            } else if (CompoundPredicateOperator.CompoundType.OR.equals(predicate.getCompoundType())) {
                callExpr = new CompoundPredicate(CompoundPredicate.Operator.OR,
                        buildExecExpression(predicate.getChild(0), context),
                        buildExecExpression(predicate.getChild(1), context));
            } else {
                // Not
                callExpr = new CompoundPredicate(CompoundPredicate.Operator.NOT,
                        buildExecExpression(predicate.getChild(0), context), null);
            }
            callExpr.setType(Type.BOOLEAN);
            return callExpr;
        }

        public Expr visitBinaryPredicate(BinaryPredicateOperator predicate, FormatterContext context) {
            BinaryPredicate call;
            switch (predicate.getBinaryType()) {
                case EQ:
                    call = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case NE:
                    call = new BinaryPredicate(BinaryPredicate.Operator.NE,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case GE:
                    call = new BinaryPredicate(BinaryPredicate.Operator.GE,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case GT:
                    call = new BinaryPredicate(BinaryPredicate.Operator.GT,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case LE:
                    call = new BinaryPredicate(BinaryPredicate.Operator.LE,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case LT:
                    call = new BinaryPredicate(BinaryPredicate.Operator.LT,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                case EQ_FOR_NULL:
                    call = new BinaryPredicate(BinaryPredicate.Operator.EQ_FOR_NULL,
                            buildExecExpression(predicate.getChildren().get(0), context),
                            buildExecExpression(predicate.getChildren().get(1), context));
                    break;
                default:
                    return null;
            }

            call.setType(Type.BOOLEAN);
            return call;
        }

        @Override
        public Expr visitBetweenPredicate(BetweenPredicateOperator predicate, FormatterContext context) {
            BetweenPredicate call = new BetweenPredicate(buildExecExpression(predicate.getChild(0), context),
                    buildExecExpression(predicate.getChild(1), context),
                    buildExecExpression(predicate.getChild(2), context), predicate.isNotBetween());
            call.setType(Type.BOOLEAN);
            return call;
        }

        @Override
        public Expr visitExistsPredicate(ExistsPredicateOperator predicate, FormatterContext context) {
            // @FIXME: support subquery
            return null;
        }

        @Override
        public Expr visitInPredicate(InPredicateOperator predicate, FormatterContext context) {
            List<Expr> args = Lists.newArrayList();
            for (int i = 1; i < predicate.getChildren().size(); ++i) {
                args.add(buildExecExpression(predicate.getChild(i), context));
            }

            // @FIXME: support subquery
            InPredicate expr =
                    new InPredicate(buildExecExpression(predicate.getChild(0), context), args, predicate.isNotIn());

            expr.setOpcode(expr.isNotIn() ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN);

            expr.setType(Type.BOOLEAN);
            return expr;
        }

        static Function isNullFN = new Function(new FunctionName("is_null_pred"),
                new Type[] {Type.INVALID}, Type.BOOLEAN, false);
        static Function isNotNullFN = new Function(new FunctionName("is_not_null_pred"),
                new Type[] {Type.INVALID}, Type.BOOLEAN, false);

        {
            isNullFN.setBinaryType(TFunctionBinaryType.BUILTIN);
            isNotNullFN.setBinaryType(TFunctionBinaryType.BUILTIN);
        }

        @Override
        public Expr visitIsNullPredicate(IsNullPredicateOperator predicate, FormatterContext context) {
            Expr expr = new IsNullPredicate(buildExecExpression(predicate.getChild(0), context), predicate.isNotNull());

            // for set function name
            if (predicate.isNotNull()) {
                expr.setFn(isNotNullFN);
            } else {
                expr.setFn(isNullFN);
            }

            expr.setType(Type.BOOLEAN);
            return expr;
        }

        @Override
        public Expr visitLikePredicateOperator(LikePredicateOperator predicate, FormatterContext context) {
            Expr child1 = buildExecExpression(predicate.getChild(0), context);
            Expr child2 = buildExecExpression(predicate.getChild(1), context);

            LikePredicate expr;
            if (predicate.isRegexp()) {
                expr = new LikePredicate(LikePredicate.Operator.REGEXP, child1, child2);
            } else {
                expr = new LikePredicate(LikePredicate.Operator.LIKE, child1, child2);
            }

            expr.setFn(Expr.getBuiltinFunction(expr.getOp().name(), new Type[] {child1.getType(), child2.getType()},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

            expr.setType(Type.BOOLEAN);
            return expr;
        }

        @Override
        public Expr visitCall(CallOperator call, FormatterContext context) {
            String fnName = call.getFnName();
            Expr callExpr;
            switch (fnName.toLowerCase()) {
                /*
                 * Arithmetic
                 */
                case "add":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.ADD,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "subtract":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.SUBTRACT,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "multiply":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.MULTIPLY,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "divide":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.DIVIDE,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "int_divide":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.INT_DIVIDE,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "mod":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.MOD,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "bitand":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITAND,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "bitor":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITOR,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "bitxor":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITXOR,
                            buildExecExpression(call.getChildren().get(0), context),
                            buildExecExpression(call.getChildren().get(1), context));
                    break;
                case "bitnot":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITNOT,
                            buildExecExpression(call.getChildren().get(0), context), null);
                    break;
                // FixMe(kks): InformationFunction shouldn't be CallOperator
                case "database":
                case "schema":
                case "user":
                case "current_user":
                    callExpr = new InformationFunction(fnName,
                            ((ConstantOperator) call.getChild(0)).getVarchar(),
                            0);
                    break;
                case "connection_id":
                    callExpr = new InformationFunction(fnName,
                            "",
                            ((ConstantOperator) call.getChild(0)).getBigint());
                    break;
                default:
                    List<Expr> arg = call.getChildren().stream()
                            .map(expr -> buildExecExpression(expr, context))
                            .collect(Collectors.toList());
                    if (call.isCountStar()) {
                        callExpr = new FunctionCallExpr(call.getFnName(), FunctionParams.createStarParam());
                    } else {
                        callExpr = new FunctionCallExpr(call.getFnName(), new FunctionParams(call.isDistinct(), arg));
                    }
                    Preconditions.checkNotNull(call.getFunction());
                    callExpr.setFn(call.getFunction());
                    break;
            }
            callExpr.setType(call.getType());
            return callExpr;
        }

        @Override
        public Expr visitCastOperator(CastOperator operator, FormatterContext context) {
            CastExpr expr = new CastExpr(operator.getType(), buildExecExpression(operator.getChild(0), context));
            expr.setImplicit(context.implicitCast);
            return expr;
        }

        @Override
        public Expr visitCaseWhenOperator(CaseWhenOperator operator, FormatterContext context) {
            Expr caseExpr = null;
            Expr elseExpr = null;

            if (operator.hasCase()) {
                caseExpr = buildExecExpression(operator.getCaseClause(), context);
            }

            if (operator.hasElse()) {
                elseExpr = buildExecExpression(operator.getElseClause(), context);
            }

            List<CaseWhenClause> list = Lists.newArrayList();
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                list.add(new CaseWhenClause(
                        buildExecExpression(operator.getWhenClause(i), context),
                        buildExecExpression(operator.getThenClause(i), context)));
            }

            CaseExpr result = new CaseExpr(caseExpr, list, elseExpr);
            result.setType(operator.getType());
            return result;
        }

        @Override
        public Expr visitDictMappingOperator(DictMappingOperator operator, FormatterContext context) {
            final ColumnRefOperator dictColumn = operator.getDictColumn();
            final SlotRef dictExpr = (SlotRef) dictColumn.accept(this, context);
            final ScalarOperator call = operator.getOriginScalaOperator();
            final ColumnRefOperator key =
                    new ColumnRefOperator(call.getUsedColumns().getFirstId(), Type.VARCHAR,
                            operator.getDictColumn().getName(),
                            dictExpr.isNullable());
            
            // Because we need to rewrite the string column to PlaceHolder when we build DictExpr,
            // the PlaceHolder and the original string column have the same id,
            // so we need to save the original string column first and restore it after we build the expression

            // 1. save the previous expr, it was null or string column
            final Expr old = context.colRefToExpr.get(key);
            // 2. use a placeholder instead of string column to build DictMapping
            context.colRefToExpr.put(key, new PlaceHolderExpr(dictColumn.getId(), dictExpr.isNullable(), Type.VARCHAR));
            final Expr callExpr = buildExecExpression(call, context);
            // 3. recover the previous column
<<<<<<< HEAD
            context.colRefToExpr.put(key, old);

=======
            if (old != null) {
                context.colRefToExpr.put(key, old);
            }
>>>>>>> 2ac5807eb ([BugFix] fix uncorrect logical properties when apply LowCardinality optimize (#11793))
            Expr result = new DictMappingExpr(dictExpr, callExpr);
            result.setType(operator.getType());
            return result;
        }

        @Override
        public Expr visitCloneOperator(CloneOperator operator, FormatterContext context) {
            return new CloneExpr(buildExecExpression(operator.getChild(0), context));
        }
    }
}
