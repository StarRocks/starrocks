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

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CollectionElementExpr;
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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.VarBinaryLiteral;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.MapExpr;
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
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TFunctionBinaryType;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScalarOperatorToExpr {
    public static Expr buildExecExpression(ScalarOperator expression, FormatterContext descTbl) {
        return expression.accept(new Formatter(ScalarOperatorToExpr::buildExecExpression), descTbl);
    }

    public static Expr buildExprIgnoreSlot(ScalarOperator expression, FormatterContext descTbl) {
        return expression.accept(new IgnoreSlotFormatter(ScalarOperatorToExpr::buildExprIgnoreSlot), descTbl);
    }

    @FunctionalInterface
    interface BuildExpr {
        Expr build(ScalarOperator expression, FormatterContext descTbl);
    }

    public static class FormatterContext {
        private final Map<ColumnRefOperator, Expr> colRefToExpr;
        private final Map<ColumnRefOperator, ScalarOperator> projectOperatorMap;

        public FormatterContext(Map<ColumnRefOperator, Expr> variableToSlotRef) {
            this.colRefToExpr = variableToSlotRef;
            this.projectOperatorMap = new HashMap<>();
        }

        public FormatterContext(Map<ColumnRefOperator, Expr> variableToSlotRef,
                                Map<ColumnRefOperator, ScalarOperator> projectOperatorMap) {
            this.colRefToExpr = variableToSlotRef;
            this.projectOperatorMap = projectOperatorMap;
        }

    }

    public static class Formatter extends ScalarOperatorVisitor<Expr, FormatterContext> {
        protected BuildExpr buildExpr;

        Formatter(BuildExpr buildExpr) {
            this.buildExpr = buildExpr;
        }

        /**
         * For now, backend cannot see the TYPE_NULL and other derived types, like array<null>
         * So we need to do some hack when transforming ScalarOperator to Expr.
         */
        private static void hackTypeNull(Expr expr) {
            // For primitive types, this can be any legitimate type, for simplicity, we pick boolean.
            Type type = AnalyzerUtils.replaceNullType2Boolean(expr.getType());
            expr.setType(type);
        }

        @Override
        public Expr visit(ScalarOperator scalarOperator, FormatterContext context) {
            throw new UnsupportedOperationException(
                    "not yet implemented: visit scalar operator for " + scalarOperator.getClass().getName());
        }

        @Override
        public Expr visitVariableReference(ColumnRefOperator node, FormatterContext context) {
            Expr expr = context.colRefToExpr.get(node);
            if (context.projectOperatorMap.containsKey(node) && expr == null) {
                expr = buildExpr.build(context.projectOperatorMap.get(node), context);
                hackTypeNull(expr);
                context.colRefToExpr.put(node, expr);
                return expr;
            }

            if (expr.getType().isNull()) {
                hackTypeNull(expr);
            }
            return expr;
        }

        @Override
        public Expr visitSubfield(SubfieldOperator node, FormatterContext context) {
            SubfieldExpr expr = new SubfieldExpr(buildExpr.build(node.getChild(0), context), node.getType(),
                    node.getFieldNames());
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitArray(ArrayOperator node, FormatterContext context) {
            ArrayExpr expr = new ArrayExpr(node.getType(),
                    node.getChildren().stream().map(e -> buildExpr.build(e, context)).collect(Collectors.toList()));
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitMap(MapOperator node, FormatterContext context) {
            MapExpr expr = new MapExpr(node.getType(),
                    node.getChildren().stream().map(e -> buildExpr.build(e, context)).collect(Collectors.toList()));
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitCollectionElement(CollectionElementOperator node, FormatterContext context) {
            CollectionElementExpr expr =
                    new CollectionElementExpr(node.getType(), buildExpr.build(node.getChild(0), context),
                            buildExpr.build(node.getChild(1), context));
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitArraySlice(ArraySliceOperator node, FormatterContext context) {
            ArraySliceExpr expr = new ArraySliceExpr(buildExpr.build(node.getChild(0), context),
                    buildExpr.build(node.getChild(1), context),
                    buildExpr.build(node.getChild(2), context));
            expr.setType(node.getType());
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitConstant(ConstantOperator literal, FormatterContext context) {
            try {
                Type type = literal.getType();
                if (literal.isNull()) {
                    NullLiteral nullLiteral = new NullLiteral();
                    nullLiteral.setType(literal.getType());
                    hackTypeNull(nullLiteral);
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
                    LocalDateTime ldt = literal.getDatetime();
                    return new DateLiteral(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(), ldt.getHour(),
                            ldt.getMinute(), ldt.getSecond(), ldt.getNano() / 1000);
                } else if (type.isTime()) {
                    return new FloatLiteral(literal.getTime(), Type.TIME);
                } else if (type.isDecimalOfAnyVersion()) {
                    DecimalLiteral d = new DecimalLiteral(literal.getDecimal());
                    d.uncheckedCastTo(type);
                    return d;
                } else if (type.isVarchar() || type.isChar()) {
                    return new StringLiteral(literal.getVarchar());
                } else if (type.isBinaryType()) {
                    return new VarBinaryLiteral(literal.getBinary());
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
                        buildExpr.build(predicate.getChildren().get(0), context),
                        buildExpr.build(predicate.getChildren().get(1), context));
            } else if (CompoundPredicateOperator.CompoundType.OR.equals(predicate.getCompoundType())) {
                callExpr = new CompoundPredicate(CompoundPredicate.Operator.OR,
                        buildExpr.build(predicate.getChild(0), context),
                        buildExpr.build(predicate.getChild(1), context));
            } else {
                // Not
                callExpr = new CompoundPredicate(CompoundPredicate.Operator.NOT,
                        buildExpr.build(predicate.getChild(0), context), null);
            }
            callExpr.setType(Type.BOOLEAN);
            return callExpr;
        }

        public Expr visitBinaryPredicate(BinaryPredicateOperator predicate, FormatterContext context) {
            BinaryPredicate call = new BinaryPredicate(predicate.getBinaryType(),
                    buildExpr.build(predicate.getChildren().get(0), context),
                    buildExpr.build(predicate.getChildren().get(1), context));
            call.setType(Type.BOOLEAN);
            return call;
        }

        @Override
        public Expr visitBetweenPredicate(BetweenPredicateOperator predicate, FormatterContext context) {
            BetweenPredicate call = new BetweenPredicate(buildExpr.build(predicate.getChild(0), context),
                    buildExpr.build(predicate.getChild(1), context),
                    buildExpr.build(predicate.getChild(2), context), predicate.isNotBetween());
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
                args.add(buildExpr.build(predicate.getChild(i), context));
            }

            // @FIXME: support subquery
            InPredicate expr =
                    new InPredicate(buildExpr.build(predicate.getChild(0), context), args, predicate.isNotIn());

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
            Expr expr = new IsNullPredicate(buildExpr.build(predicate.getChild(0), context), predicate.isNotNull());

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
            Expr child1 = buildExpr.build(predicate.getChild(0), context);
            Expr child2 = buildExpr.build(predicate.getChild(1), context);

            LikePredicate expr;
            if (predicate.isRegexp()) {
                expr = new LikePredicate(LikePredicate.Operator.REGEXP, child1, child2);
            } else {
                expr = new LikePredicate(LikePredicate.Operator.LIKE, child1, child2);
            }

            expr.setFn(Expr.getBuiltinFunction(expr.getOp().name(),
                    new Type[] {child1.getType(), child2.getType()},
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
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "subtract":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.SUBTRACT,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "multiply":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.MULTIPLY,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "divide":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.DIVIDE,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "int_divide":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.INT_DIVIDE,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "mod":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.MOD,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bitand":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITAND,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bitor":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITOR,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bitxor":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITXOR,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bitnot":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITNOT,
                            buildExpr.build(call.getChildren().get(0), context), null);
                    break;
                case "bit_shift_left":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BIT_SHIFT_LEFT,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bit_shift_right":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BIT_SHIFT_RIGHT,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                case "bit_shift_right_logical":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL,
                            buildExpr.build(call.getChildren().get(0), context),
                            buildExpr.build(call.getChildren().get(1), context));
                    break;
                // FixMe(kks): InformationFunction shouldn't be CallOperator
                case "catalog":
                case "database":
                case "schema":
                case "user":
                case "current_user":
                case "current_role":
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
                            .map(expr -> buildExpr.build(expr, context))
                            .collect(Collectors.toList());
                    if (call.isCountStar()) {
                        callExpr = new FunctionCallExpr(call.getFnName(), FunctionParams.createStarParam());
                    } else {
                        callExpr = new FunctionCallExpr(call.getFnName(), new FunctionParams(call.isDistinct(), arg));
                    }
                    Preconditions.checkNotNull(call.getFunction());
                    callExpr.setFn(call.getFunction());
                    callExpr.setIgnoreNulls(call.getIgnoreNulls());
                    break;
            }
            callExpr.setType(call.getType());
            hackTypeNull(callExpr);
            return callExpr;
        }

        @Override
        public Expr visitCastOperator(CastOperator operator, FormatterContext context) {
            CastExpr expr =
                    new CastExpr(operator.getType(), buildExpr.build(operator.getChild(0), context));
            expr.setImplicit(operator.isImplicit());
            hackTypeNull(expr);
            return expr;
        }

        @Override
        public Expr visitCaseWhenOperator(CaseWhenOperator operator, FormatterContext context) {
            Expr caseExpr = null;
            Expr elseExpr = null;

            if (operator.hasCase()) {
                caseExpr = buildExpr.build(operator.getCaseClause(), context);
            }

            if (operator.hasElse()) {
                elseExpr = buildExpr.build(operator.getElseClause(), context);
            }

            List<CaseWhenClause> list = Lists.newArrayList();
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                list.add(new CaseWhenClause(
                        buildExpr.build(operator.getWhenClause(i), context),
                        buildExpr.build(operator.getThenClause(i), context)));
            }

            CaseExpr result = new CaseExpr(caseExpr, list, elseExpr);
            result.setType(operator.getType());
            hackTypeNull(result);
            return result;
        }

        @Override
        public Expr visitLambdaFunctionOperator(LambdaFunctionOperator operator, FormatterContext context) {
            // lambda arguments
            List<Expr> arguments = Lists.newArrayList();
            List<Expr> newArguments = Lists.newArrayList();
            for (ColumnRefOperator ref : operator.getRefColumns()) {
                SlotRef slot = new SlotRef(new SlotDescriptor(
                        new SlotId(ref.getId()), ref.getName(), ref.getType(), ref.isNullable()));
                slot.setTblName(new TableName(TableName.LAMBDA_FUNC_TABLE, TableName.LAMBDA_FUNC_TABLE));
                hackTypeNull(slot);
                context.colRefToExpr.put(ref, slot);
                arguments.add(slot);
            }
            // construct common sub operator map
            Map<SlotRef, Expr> commonSubOperatorMap =
                    Maps.newTreeMap(Comparator.comparing(ref -> ref.getSlotId().asInt()));

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operator.getColumnRefMap().entrySet()) {
                ColumnRefOperator ref = kv.getKey();
                SlotRef slot = new SlotRef(new SlotDescriptor(
                        new SlotId(ref.getId()), ref.getName(), ref.getType(), ref.isNullable()));
                hackTypeNull(slot);
                commonSubOperatorMap.put(slot, buildExpr.build(kv.getValue(), context));
                context.colRefToExpr.put(ref, slot);
            }
            // lambda expression and put it at the first
            final ScalarOperator lambdaOp = operator.getLambdaExpr();
            final Expr lambdaExpr = buildExpr.build(lambdaOp, context);
            newArguments.add(lambdaExpr);
            newArguments.addAll(arguments);

            LambdaFunctionExpr result = new LambdaFunctionExpr(newArguments, commonSubOperatorMap);
            result.setType(Type.FUNCTION);
            result.checkValid();
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
            final Expr callExpr = buildExpr.build(call, context);
            // 3. recover the previous column
            if (old != null) {
                context.colRefToExpr.put(key, old);
            }
            Expr result = new DictMappingExpr(dictExpr, callExpr);
            result.setType(operator.getType());
            hackTypeNull(result);
            return result;
        }

        @Override
        public Expr visitCloneOperator(CloneOperator operator, FormatterContext context) {
            return new CloneExpr(buildExpr.build(operator.getChild(0), context));
        }

        @Override
        public Expr visitSubqueryOperator(SubqueryOperator operator, FormatterContext context) {
            Subquery subquery = new Subquery(operator.getQueryStatement());
            subquery.setUseSemiAnti(operator.getApplyOperator().isUseSemiAnti());
            return subquery;
        }
    }

    static class IgnoreSlotFormatter extends Formatter {
        IgnoreSlotFormatter(BuildExpr buildExpr) {
            super(buildExpr);
        }

        @Override
        public Expr visitVariableReference(ColumnRefOperator node, FormatterContext context) {
            SlotDescriptor descriptor = new SlotDescriptor(new SlotId(node.getId()), node.getName(),
                    node.getType(), node.isNullable());
            return new SlotRef(descriptor);
        }
    }
}
