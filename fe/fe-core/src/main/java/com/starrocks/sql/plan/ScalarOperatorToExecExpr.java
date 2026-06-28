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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.expression.ExecArithmetic;
import com.starrocks.planner.expression.ExecArrayExpr;
import com.starrocks.planner.expression.ExecArraySlice;
import com.starrocks.planner.expression.ExecBetweenPredicate;
import com.starrocks.planner.expression.ExecBinaryPredicate;
import com.starrocks.planner.expression.ExecCaseWhen;
import com.starrocks.planner.expression.ExecCast;
import com.starrocks.planner.expression.ExecClone;
import com.starrocks.planner.expression.ExecCollectionElement;
import com.starrocks.planner.expression.ExecCompoundPredicate;
import com.starrocks.planner.expression.ExecDictMapping;
import com.starrocks.planner.expression.ExecDictQuery;
import com.starrocks.planner.expression.ExecDictionaryGet;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecFunctionCall;
import com.starrocks.planner.expression.ExecInPredicate;
import com.starrocks.planner.expression.ExecInformationFunction;
import com.starrocks.planner.expression.ExecIsNullPredicate;
import com.starrocks.planner.expression.ExecLambdaFunction;
import com.starrocks.planner.expression.ExecLikePredicate;
import com.starrocks.planner.expression.ExecLiteral;
import com.starrocks.planner.expression.ExecMapExpr;
import com.starrocks.planner.expression.ExecMatchExpr;
import com.starrocks.planner.expression.ExecPlaceHolder;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.planner.expression.ExecSubfield;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.common.UnsupportedException;
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
import com.starrocks.sql.optimizer.operator.scalar.DictQueryOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictionaryGetOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.spm.SPMFunctions;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FunctionType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Converts a {@link ScalarOperator} tree into an {@link ExecExpr} tree.
 */
public class ScalarOperatorToExecExpr {

    public static ExecExpr build(ScalarOperator expression, FormatterContext context) {
        return expression.accept(new Formatter(ScalarOperatorToExecExpr::build), context);
    }

    public static ExecExpr buildIgnoreSlot(ScalarOperator expression, FormatterContext context) {
        return expression.accept(new IgnoreSlotFormatter(ScalarOperatorToExecExpr::buildIgnoreSlot), context);
    }

    @FunctionalInterface
    interface BuildExecExpr {
        ExecExpr build(ScalarOperator expression, FormatterContext context);
    }

    /**
     * Context for the conversion, mapping ColumnRefOperators to their resolved ExecExpr.
     */
    public static class FormatterContext {
        private final Map<ColumnRefOperator, ExecExpr> colRefToExecExpr;
        private final Map<ColumnRefOperator, ScalarOperator> projectOperatorMap;

        public FormatterContext(Map<ColumnRefOperator, ExecExpr> colRefToExecExpr) {
            this.colRefToExecExpr = colRefToExecExpr;
            this.projectOperatorMap = new HashMap<>();
        }

        public FormatterContext(Map<ColumnRefOperator, ExecExpr> colRefToExecExpr,
                                Map<ColumnRefOperator, ScalarOperator> projectOperatorMap) {
            this.colRefToExecExpr = colRefToExecExpr;
            this.projectOperatorMap = projectOperatorMap;
        }

        public Map<ColumnRefOperator, ExecExpr> getColRefToExecExpr() {
            return colRefToExecExpr;
        }
    }

    public static class Formatter extends ScalarOperatorVisitor<ExecExpr, FormatterContext> {
        protected final BuildExecExpr buildExpr;

        Formatter(BuildExecExpr buildExpr) {
            this.buildExpr = buildExpr;
        }

        /**
         * Replace NULL_TYPE with BOOLEAN in the expression's type.
         * The backend cannot handle NULL types.
         */
        private static Type replaceNullType(Type type) {
            Type replaced = AnalyzerUtils.replaceNullType2Boolean(type);
            return Objects.equals(type, replaced) ? type : replaced;
        }

        @Override
        public ExecExpr visit(ScalarOperator scalarOperator, FormatterContext context) {
            throw new UnsupportedOperationException(
                    "not yet implemented: visit scalar operator for " + scalarOperator.getClass().getName());
        }

        @Override
        public ExecExpr visitVariableReference(ColumnRefOperator node, FormatterContext context) {
            ExecExpr expr = context.colRefToExecExpr.get(node);
            if (context.projectOperatorMap.containsKey(node) && expr == null) {
                final ScalarOperator projected = context.projectOperatorMap.get(node);
                if (projected.equals(node)) {
                    throw new SemanticException("Cannot convert ColumnRefOperator to ExecExpr, " +
                            "please check the input expression: " + node);
                }
                expr = buildExpr.build(projected, context);
                expr.setType(replaceNullType(expr.getType()));
                context.colRefToExecExpr.put(node, expr);
                return expr;
            }
            if (expr == null) {
                throw new SemanticException("Cannot convert ColumnRefOperator to ExecExpr, " +
                        "please check the input expression: " + node);
            }

            expr.setType(replaceNullType(expr.getType()));
            return expr;
        }

        @Override
        public ExecExpr visitSubfield(SubfieldOperator node, FormatterContext context) {
            ExecExpr child = buildExpr.build(node.getChild(0), context);
            Type type = replaceNullType(node.getType());
            ExecSubfield expr = new ExecSubfield(type, node.getFieldNames(), node.getCopyFlag(),
                    List.of(child));
            return expr;
        }

        @Override
        public ExecExpr visitArray(ArrayOperator node, FormatterContext context) {
            List<ExecExpr> children = node.getChildren().stream()
                    .map(e -> buildExpr.build(e, context))
                    .collect(Collectors.toList());
            Type type = replaceNullType(node.getType());
            return new ExecArrayExpr(type, children);
        }

        @Override
        public ExecExpr visitMap(MapOperator node, FormatterContext context) {
            List<ExecExpr> children = node.getChildren().stream()
                    .map(e -> buildExpr.build(e, context))
                    .collect(Collectors.toList());
            Type type = replaceNullType(node.getType());
            return new ExecMapExpr(type, children);
        }

        @Override
        public ExecExpr visitCollectionElement(CollectionElementOperator node, FormatterContext context) {
            ExecExpr collection = buildExpr.build(node.getChild(0), context);
            ExecExpr index = buildExpr.build(node.getChild(1), context);
            Type type = replaceNullType(node.getType());
            return new ExecCollectionElement(type, node.isCheckOutOfBounds(), List.of(collection, index));
        }

        @Override
        public ExecExpr visitArraySlice(ArraySliceOperator node, FormatterContext context) {
            ExecExpr array = buildExpr.build(node.getChild(0), context);
            ExecExpr lower = buildExpr.build(node.getChild(1), context);
            ExecExpr upper = buildExpr.build(node.getChild(2), context);
            Type type = replaceNullType(node.getType());
            return new ExecArraySlice(type, List.of(array, lower, upper));
        }

        @Override
        public ExecExpr visitConstant(ConstantOperator literal, FormatterContext context) {
            Type type = replaceNullType(literal.getType());
            ExecLiteral expr = new ExecLiteral(literal, type);
            if (literal.isNull()) {
                expr.setOriginType(com.starrocks.type.NullType.NULL);
            }
            return expr;
        }

        @Override
        public ExecExpr visitPredicate(PredicateOperator predicate, FormatterContext context) {
            return null;
        }

        @Override
        public ExecExpr visitCompoundPredicate(CompoundPredicateOperator predicate, FormatterContext context) {
            ExecExpr result;
            if (CompoundPredicateOperator.CompoundType.AND.equals(predicate.getCompoundType())) {
                result = new ExecCompoundPredicate(CompoundPredicate.Operator.AND,
                        buildExpr.build(predicate.getChild(0), context),
                        buildExpr.build(predicate.getChild(1), context));
            } else if (CompoundPredicateOperator.CompoundType.OR.equals(predicate.getCompoundType())) {
                result = new ExecCompoundPredicate(CompoundPredicate.Operator.OR,
                        buildExpr.build(predicate.getChild(0), context),
                        buildExpr.build(predicate.getChild(1), context));
            } else {
                // NOT
                result = new ExecCompoundPredicate(CompoundPredicate.Operator.NOT,
                        List.of(buildExpr.build(predicate.getChild(0), context)));
            }
            result.setIsIndexOnlyFilter(predicate.isIndexOnlyFilter());
            return result;
        }

        @Override
        public ExecExpr visitBinaryPredicate(BinaryPredicateOperator predicate, FormatterContext context) {
            ExecBinaryPredicate result = new ExecBinaryPredicate(predicate.getBinaryType(),
                    buildExpr.build(predicate.getChild(0), context),
                    buildExpr.build(predicate.getChild(1), context));
            result.setIsIndexOnlyFilter(predicate.isIndexOnlyFilter());
            return result;
        }

        @Override
        public ExecExpr visitBetweenPredicate(BetweenPredicateOperator predicate, FormatterContext context) {
            List<ExecExpr> children = List.of(
                    buildExpr.build(predicate.getChild(0), context),
                    buildExpr.build(predicate.getChild(1), context),
                    buildExpr.build(predicate.getChild(2), context));
            return new ExecBetweenPredicate(predicate.isNotBetween(), children);
        }

        @Override
        public ExecExpr visitExistsPredicate(ExistsPredicateOperator predicate, FormatterContext context) {
            return null;
        }

        @Override
        public ExecExpr visitInPredicate(InPredicateOperator predicate, FormatterContext context) {
            List<ExecExpr> children = new ArrayList<>();
            children.add(buildExpr.build(predicate.getChild(0), context));
            for (int i = 1; i < predicate.getChildren().size(); ++i) {
                children.add(buildExpr.build(predicate.getChild(i), context));
            }
            return new ExecInPredicate(predicate.isNotIn(), children);
        }

        @Override
        public ExecExpr visitLargeInPredicate(LargeInPredicateOperator predicate, FormatterContext context) {
            throw new LargeInPredicateException(
                    "LargeInPredicate was not transformed to join during optimization. " +
                            "This may occur in unsupported contexts such as CASE WHEN expressions. " +
                            "Retrying query with LargeInPredicate optimization disabled.");
        }

        @Override
        public ExecExpr visitIsNullPredicate(IsNullPredicateOperator predicate, FormatterContext context) {
            return new ExecIsNullPredicate(predicate.isNotNull(),
                    buildExpr.build(predicate.getChild(0), context));
        }

        @Override
        public ExecExpr visitLikePredicateOperator(LikePredicateOperator predicate, FormatterContext context) {
            ExecExpr child0 = buildExpr.build(predicate.getChild(0), context);
            ExecExpr child1 = buildExpr.build(predicate.getChild(1), context);

            // Resolve the built-in function for LIKE/REGEXP at construction time
            String opName = predicate.isRegexp() ? "REGEXP" : "LIKE";
            Function fn = ExprUtils.getBuiltinFunction(opName,
                    new Type[] {child0.getType(), child1.getType()},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            return new ExecLikePredicate(predicate.isRegexp(), fn, List.of(child0, child1));
        }

        @Override
        public ExecExpr visitMatchExprOperator(MatchExprOperator operator, FormatterContext context) {
            ExecExpr child0 = buildExpr.build(operator.getChild(0), context);
            ExecExpr child1 = buildExpr.build(operator.getChild(1), context);
            return new ExecMatchExpr(operator.getMatchOperator(), List.of(child0, child1));
        }

        @Override
        public ExecExpr visitCall(CallOperator call, FormatterContext context) {
            String fnName = call.getFnName();
            ExecExpr result;

            if (!FeConstants.runningUnitTest && SPMFunctions.isSPMFunctions(call)) {
                throw UnsupportedException.unsupportedException("spm function only used in create baseline stmt");
            }

            switch (fnName.toLowerCase()) {
                // Arithmetic
                case "add":
                    result = buildArithmetic(ArithmeticExpr.Operator.ADD, call, context);
                    break;
                case "subtract":
                    result = buildArithmetic(ArithmeticExpr.Operator.SUBTRACT, call, context);
                    break;
                case "multiply":
                    result = buildArithmetic(ArithmeticExpr.Operator.MULTIPLY, call, context);
                    break;
                case "divide":
                    result = buildArithmetic(ArithmeticExpr.Operator.DIVIDE, call, context);
                    break;
                case "int_divide":
                    result = buildArithmetic(ArithmeticExpr.Operator.INT_DIVIDE, call, context);
                    break;
                case "mod":
                    result = buildArithmetic(ArithmeticExpr.Operator.MOD, call, context);
                    break;
                case "bitand":
                    result = buildArithmetic(ArithmeticExpr.Operator.BITAND, call, context);
                    break;
                case "bitor":
                    result = buildArithmetic(ArithmeticExpr.Operator.BITOR, call, context);
                    break;
                case "bitxor":
                    result = buildArithmetic(ArithmeticExpr.Operator.BITXOR, call, context);
                    break;
                case "bitnot":
                    result = new ExecArithmetic(call.getType(), ArithmeticExpr.Operator.BITNOT,
                            List.of(buildExpr.build(call.getChild(0), context)));
                    break;
                case "bit_shift_left":
                    result = buildArithmetic(ArithmeticExpr.Operator.BIT_SHIFT_LEFT, call, context);
                    break;
                case "bit_shift_right":
                    result = buildArithmetic(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT, call, context);
                    break;
                case "bit_shift_right_logical":
                    result = buildArithmetic(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL, call, context);
                    break;

                // Information functions
                case "catalog":
                case "database":
                case "schema":
                case "user":
                case "current_user":
                case "current_role":
                case "current_group":
                case "current_warehouse":
                case "session_id":
                    result = new ExecInformationFunction(call.getType(), fnName,
                            ((ConstantOperator) call.getChild(0)).getVarchar(), 0);
                    break;
                case "connection_id":
                    result = new ExecInformationFunction(call.getType(), fnName,
                            "", ((ConstantOperator) call.getChild(0)).getBigint());
                    break;

                // Non-deterministic functions with special child handling
                case "rand":
                case "random":
                case "uuid":
                case "sleep": {
                    List<ExecExpr> args = new ArrayList<>();
                    if (call.getChildren().size() == 2) {
                        args.add(buildExpr.build(call.getChild(0), context));
                    }
                    Preconditions.checkNotNull(call.getFunction());
                    boolean isAgg = call.isAggregate();
                    result = new ExecFunctionCall(call.getType(), call.getFunction(), fnName,
                            args, false, call.getIgnoreNulls(), isAgg, false);
                    break;
                }

                // Default: regular function call
                default: {
                    List<ExecExpr> args;
                    if (call.isCountStar()) {
                        args = new ArrayList<>();
                    } else {
                        args = call.getChildren().stream()
                                .map(e -> buildExpr.build(e, context))
                                .collect(Collectors.toList());
                    }
                    Preconditions.checkNotNull(call.getFunction());
                    boolean isAgg = call.isAggregate();
                    result = new ExecFunctionCall(call.getType(), call.getFunction(), fnName,
                            args, call.isDistinct(), call.getIgnoreNulls(), isAgg, false,
                            call.isCountStar());
                    break;
                }
            }

            result.setType(replaceNullType(call.getType()));
            if (result instanceof ExecFunctionCall) {
                ((ExecFunctionCall) result).setNullable(call.isNullable());
            }
            return result;
        }

        private ExecExpr buildArithmetic(ArithmeticExpr.Operator op, CallOperator call,
                                         FormatterContext context) {
            List<ExecExpr> children = new ArrayList<>();
            children.add(buildExpr.build(call.getChild(0), context));
            if (call.getChildren().size() > 1) {
                children.add(buildExpr.build(call.getChild(1), context));
            }
            return new ExecArithmetic(call.getType(), op, children);
        }

        @Override
        public ExecExpr visitCastOperator(CastOperator operator, FormatterContext context) {
            ExecExpr child = buildExpr.build(operator.getChild(0), context);
            Type type = replaceNullType(operator.getType());
            return new ExecCast(type, child, operator.isImplicit());
        }

        @Override
        public ExecExpr visitCaseWhenOperator(CaseWhenOperator operator, FormatterContext context) {
            // Build children in the same order as CaseExpr expects:
            // [caseExpr?] [whenExpr, thenExpr]+ [elseExpr?]
            List<ExecExpr> children = new ArrayList<>();

            if (operator.hasCase()) {
                children.add(buildExpr.build(operator.getCaseClause(), context));
            }

            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                children.add(buildExpr.build(operator.getWhenClause(i), context));
                children.add(buildExpr.build(operator.getThenClause(i), context));
            }

            if (operator.hasElse()) {
                children.add(buildExpr.build(operator.getElseClause(), context));
            }

            Type type = replaceNullType(operator.getType());
            return new ExecCaseWhen(type, operator.hasCase(), operator.hasElse(), children);
        }

        @Override
        public ExecExpr visitLambdaFunctionOperator(LambdaFunctionOperator operator, FormatterContext context) {
            // Lambda arguments → ExecSlotRef
            List<ExecExpr> arguments = new ArrayList<>();
            for (ColumnRefOperator ref : operator.getRefColumns()) {
                SlotDescriptor slotDesc = new SlotDescriptor(
                        new SlotId(ref.getId()), ref.getName(), ref.getType(), ref.isNullable());
                ExecSlotRef slot = new ExecSlotRef(slotDesc);
                slot.setType(replaceNullType(slot.getType()));
                context.colRefToExecExpr.put(ref, slot);
                arguments.add(slot);
            }

            // Common sub-operator map
            TreeMap<Integer, Map.Entry<ExecSlotRef, ExecExpr>> commonSubMap =
                    new TreeMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operator.getColumnRefMap().entrySet()) {
                ColumnRefOperator ref = kv.getKey();
                SlotDescriptor slotDesc = new SlotDescriptor(
                        new SlotId(ref.getId()), ref.getName(), ref.getType(), ref.isNullable());
                ExecSlotRef slot = new ExecSlotRef(slotDesc);
                slot.setType(replaceNullType(slot.getType()));
                ExecExpr subExpr = buildExpr.build(kv.getValue(), context);
                commonSubMap.put(ref.getId(), Map.entry(slot, subExpr));
                context.colRefToExecExpr.put(ref, slot);
            }

            // Build lambda body
            ExecExpr lambdaExpr = buildExpr.build(operator.getLambdaExpr(), context);

            // Assemble children: [lambdaBody, arg1, arg2, ..., commonSlot1, commonSlot2, ..., commonExpr1, commonExpr2, ...]
            List<ExecExpr> children = new ArrayList<>();
            children.add(lambdaExpr);
            children.addAll(arguments);
            if (!commonSubMap.isEmpty()) {
                for (Map.Entry<Integer, Map.Entry<ExecSlotRef, ExecExpr>> entry : commonSubMap.entrySet()) {
                    children.add(entry.getValue().getKey());
                }
                for (Map.Entry<Integer, Map.Entry<ExecSlotRef, ExecExpr>> entry : commonSubMap.entrySet()) {
                    children.add(entry.getValue().getValue());
                }
            }

            // Determine nondeterminism from the ScalarOperator tree
            boolean isNondeterministic = containsNonDeterministicFunction(operator.getLambdaExpr());

            ExecLambdaFunction result = new ExecLambdaFunction(
                    FunctionType.FUNCTION, commonSubMap.size(), isNondeterministic, children);

            Preconditions.checkState(result.getNumChildren() >= 2 + 2 * commonSubMap.size(),
                    "lambda expr's children num " + result.getNumChildren() + " should >= " +
                            (2 + 2 * commonSubMap.size()));

            return result;
        }

        @Override
        public ExecExpr visitDictMappingOperator(DictMappingOperator operator, FormatterContext context) {
            final ColumnRefOperator dictColumn = operator.getDictColumn();
            final ExecSlotRef dictExpr = (ExecSlotRef) dictColumn.accept(this, context);
            final ScalarOperator call = operator.getOriginScalaOperator();
            final ColumnRefOperator key = call.getColumnRefs().get(0);

            // Save the previous expression for this key
            final ExecExpr old = context.colRefToExecExpr.get(key);

            // Use a placeholder instead of the original column for DictMapping
            if (key.getType().isArrayType()) {
                context.colRefToExecExpr.put(key,
                        new ExecPlaceHolder(dictColumn.getId(), dictExpr.isNullable(), ArrayType.ARRAY_VARCHAR));
            } else {
                context.colRefToExecExpr.put(key,
                        new ExecPlaceHolder(dictColumn.getId(), dictExpr.isNullable(), VarcharType.VARCHAR));
            }
            final ExecExpr callExpr = buildExpr.build(call, context);

            // Recover the previous column
            if (old != null) {
                context.colRefToExecExpr.put(key, old);
            }

            List<ExecExpr> children;
            if (operator.getStringProvideOperator() != null) {
                final ExecExpr stringExpr = buildExpr.build(operator.getStringProvideOperator(), context);
                children = List.of(dictExpr, callExpr, stringExpr);
            } else {
                children = List.of(dictExpr, callExpr);
            }

            ExecDictMapping result = new ExecDictMapping(replaceNullType(operator.getType()), children);
            return result;
        }

        @Override
        public ExecExpr visitCloneOperator(CloneOperator operator, FormatterContext context) {
            ExecExpr child = buildExpr.build(operator.getChild(0), context);
            return new ExecClone(child.getType(), child);
        }

        @Override
        public ExecExpr visitSubqueryOperator(SubqueryOperator operator, FormatterContext context) {
            // Subquery should not appear in the execution plan's ExecExpr tree.
            throw new UnsupportedOperationException("SubqueryOperator cannot be converted to ExecExpr");
        }

        @Override
        public ExecExpr visitDictQueryOperator(DictQueryOperator operator, FormatterContext context) {
            List<ExecExpr> children = operator.getChildren().stream()
                    .map(e -> buildExpr.build(e, context))
                    .collect(Collectors.toList());
            return new ExecDictQuery(operator.getType(), operator.getDictQueryExpr(), children);
        }

        @Override
        public ExecExpr visitDictionaryGetOperator(DictionaryGetOperator operator, FormatterContext context) {
            List<ExecExpr> children = operator.getChildren().stream()
                    .map(e -> buildExpr.build(e, context))
                    .collect(Collectors.toList());
            return new ExecDictionaryGet(operator.getType(),
                    operator.getDictionaryId(), operator.getDictionaryTxnId(),
                    operator.getKeySize(), operator.getNullIfNotExist(), children);
        }
    }

    /**
     * Variant that creates ExecSlotRef directly from ColumnRefOperator,
     * without looking up in the context map. Used for cases where
     * slot descriptors haven't been registered yet.
     */
    static class IgnoreSlotFormatter extends Formatter {
        IgnoreSlotFormatter(BuildExecExpr buildExpr) {
            super(buildExpr);
        }

        @Override
        public ExecExpr visitVariableReference(ColumnRefOperator node, FormatterContext context) {
            SlotDescriptor descriptor = new SlotDescriptor(
                    new SlotId(node.getId()), node.getName(), node.getType(), node.isNullable());
            return new ExecSlotRef(descriptor);
        }
    }

    /**
     * Check if a ScalarOperator tree contains any non-deterministic function call.
     */
    private static boolean containsNonDeterministicFunction(ScalarOperator operator) {
        if (operator instanceof CallOperator) {
            String fnName = ((CallOperator) operator).getFnName();
            if (fnName != null && FunctionSet.allNonDeterministicFunctions.contains(fnName)) {
                return true;
            }
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (containsNonDeterministicFunction(child)) {
                return true;
            }
        }
        return false;
    }
}
