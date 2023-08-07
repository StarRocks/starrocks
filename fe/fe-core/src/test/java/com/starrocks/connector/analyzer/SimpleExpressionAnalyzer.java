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


package com.starrocks.connector.analyzer;

import com.google.common.base.Joiner;
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
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.common.TypeManager;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyticAnalyzer.verifyAnalyticExpression;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;


// This analyzer is used to test the compatibility of query SQL, do not need to verify
// the column valid, focus on analyze functions, and this analyzer do not support analyze
// UDF
public class SimpleExpressionAnalyzer {

    public void analyze(Expr expression, AnalyzeState analyzeState) {
        SimpleExpressionAnalyzer.Visitor visitor = new SimpleExpressionAnalyzer.Visitor(analyzeState);
        bottomUpAnalyze(visitor, expression);
    }

    private boolean isHighOrderFunction(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            // expand this in the future.
            if (((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ARRAY_MAP) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ARRAY_FILTER)) {
                return true;
            } else if (((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.TRANSFORM)) {
                // transform just a alias of array_map
                ((FunctionCallExpr) expr).resetFnName("", FunctionSet.ARRAY_MAP);
                return true;
            }
        }
        return false;
    }

    private Expr rewriteHighOrderFunction(Expr expr) {
        Preconditions.checkState(expr instanceof FunctionCallExpr);
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        if (functionCallExpr.getFnName().getFunction().equals(FunctionSet.ARRAY_FILTER)
                && functionCallExpr.getChild(0) instanceof LambdaFunctionExpr) {
            // array_filter(lambda_func_expr, arr1...) -> array_filter(arr1, array_map(lambda_func_expr, arr1...))
            FunctionCallExpr arrayMap = new FunctionCallExpr(FunctionSet.ARRAY_MAP,
                    Lists.newArrayList(functionCallExpr.getChildren()));
            arrayMap.setType(Type.BOOLEAN);
            Expr arr1 = functionCallExpr.getChild(1);
            functionCallExpr.clearChildren();
            functionCallExpr.addChild(arr1);
            functionCallExpr.addChild(arrayMap);
            return arrayMap;
        }
        return null;
    }

    // only high-order functions can use lambda functions.
    void analyzeHighOrderFunction(SimpleExpressionAnalyzer.Visitor visitor, Expr expression) {
        if (!isHighOrderFunction(expression)) {
            throw new SemanticException("Lambda Functions can only be used in supported high-order functions.");
        }
        int childSize = expression.getChildren().size();
        // move the lambda function to the first if it is at the last.
        if (expression.getChild(childSize - 1) instanceof LambdaFunctionExpr) {
            Expr last = expression.getChild(childSize - 1);
            for (int i = childSize - 1; i > 0; i--) {
                expression.setChild(i, expression.getChild(i - 1));
            }
            expression.setChild(0, last);
        }
        // the first child is lambdaFunction, following input arrays
        for (int i = 1; i < childSize; ++i) {
            Expr expr = expression.getChild(i);
            bottomUpAnalyze(visitor, expr);
            if (expr instanceof NullLiteral) {
                expr.setType(Type.ARRAY_INT); // Let it have item type.
            }
            if (!expr.getType().isArrayType()) {
                throw new SemanticException("Lambda inputs should be arrays.");
            }
            Type itemType = ((ArrayType) expr.getType()).getItemType();
            if (itemType == Type.NULL) { // Since slot_ref with Type.NULL is rewritten to Literal in toThrift(),
                // rather than a common columnRef, so change its type here.
                itemType = Type.BOOLEAN;
            }
        }
        // visit LambdaFunction
        visitor.visit(expression.getChild(0));
        Expr res = rewriteHighOrderFunction(expression);
        if (res != null) {
            visitor.visit(res);
        }
    }

    private void bottomUpAnalyze(SimpleExpressionAnalyzer.Visitor visitor, Expr expression) {
        if (expression.hasLambdaFunction(expression)) {
            analyzeHighOrderFunction(visitor, expression);
        } else {
            for (Expr expr : expression.getChildren()) {
                bottomUpAnalyze(visitor, expr);
            }
        }
        visitor.visit(expression);
    }

    static class Visitor extends ExpressionAnalyzer.Visitor {
        private final AnalyzeState analyzeState;

        public Visitor(AnalyzeState analyzeState) {
            super(analyzeState, null);
            this.analyzeState = analyzeState;
        }

        @Override
        public Void visitExpression(Expr node, Scope scope) {
            throw unsupportedException("not yet implemented: expression analyzer for " + node.getClass().getName());
        }

        private void handleResolvedField(SlotRef slot, ResolvedField resolvedField) {
            analyzeState.addColumnReference(slot, FieldId.from(resolvedField));
        }

        @Override
        public Void visitSubfieldExpr(SubfieldExpr node, Scope scope) {
            Expr child = node.getChild(0);
            // User enter an invalid sql, like SELECT 'col'.b FROM tbl;
            // 'col' will be parsed as StringLiteral, it's invalid.
            // TODO(SmithCruise) We should handle this problem in parser in the future.
            Preconditions.checkArgument(child.getType().isStructType(),
                    String.format("%s must be a struct type, check if you are using `'`", child.toSql()));

            List<String> fieldNames = node.getFieldNames();
            Type tmpType = child.getType();
            for (String fieldName : fieldNames) {
                StructType structType = (StructType) tmpType;
                StructField structField = structType.getField(fieldName);
                if (structField == null) {
                    throw new SemanticException("Struct subfield '%s' cannot be resolved", fieldName);
                }
                tmpType = structField.getType();
            }

            node.setType(tmpType);
            return null;
        }

        @Override
        public Void visitSlot(SlotRef node, Scope scope) {
            // Do not know the slot type, use string as default
            node.setType(Type.STRING);
            return null;
        }

        @Override
        public Void visitFieldReference(FieldReference node, Scope scope) {
            Field field = scope.getRelationFields().getFieldByIndex(node.getFieldIndex());
            node.setType(field.getType());
            return null;
        }

        @Override
        public Void visitArrayExpr(ArrayExpr node, Scope scope) {
            if (!node.getChildren().isEmpty()) {
                try {
                    Type targetItemType;
                    if (node.getType() != null) {
                        targetItemType = ((ArrayType) node.getType()).getItemType();
                    } else {
                        targetItemType = TypeManager.getCommonSuperType(
                                node.getChildren().stream().map(Expr::getType).collect(Collectors.toList()));
                    }

                    for (int i = 0; i < node.getChildren().size(); i++) {
                        if (!node.getChildren().get(i).getType().matchesType(targetItemType)) {
                            node.castChild(targetItemType, i);
                        }
                    }

                    node.setType(new ArrayType(targetItemType));
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else {
                node.setType(Type.ARRAY_NULL);
            }
            return null;
        }

        @Override
        public Void visitCollectionElementExpr(CollectionElementExpr node, Scope scope) {
            Expr expr = node.getChild(0);
            Expr subscript = node.getChild(1);
            if (!expr.getType().isArrayType() && !expr.getType().isMapType()) {
                throw new SemanticException("cannot subscript type " + expr.getType()
                        + " because it is not an array or a map");
            }
            if (expr.getType().isArrayType()) {
                if (!subscript.getType().isNumericType()) {
                    throw new SemanticException("array subscript must have type integer");
                }
                try {
                    if (subscript.getType().getPrimitiveType() != PrimitiveType.INT) {
                        node.castChild(Type.INT, 1);
                    }
                    node.setType(((ArrayType) expr.getType()).getItemType());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else {
                try {
                    if (subscript.getType().getPrimitiveType() !=
                            ((MapType) expr.getType()).getKeyType().getPrimitiveType()) {
                        node.castChild(((MapType) expr.getType()).getKeyType(), 1);
                    }
                    node.setType(((MapType) expr.getType()).getValueType());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            }

            return null;
        }

        @Override
        public Void visitArraySliceExpr(ArraySliceExpr node, Scope scope) {
            if (!node.getChild(0).getType().isArrayType()) {
                throw new SemanticException("cannot subscript type" +
                        node.getChild(0).getType() + " because it is not an array");
            }
            node.setType(node.getChild(0).getType());
            return null;
        }

        @Override
        public Void visitArrowExpr(ArrowExpr node, Scope scope) {
            Expr item = node.getChild(0);
            Expr key = node.getChild(1);
            if (!key.isLiteral() || !key.getType().isStringType()) {
                throw new SemanticException("right operand of -> should be string literal, but got " + key);
            }
            if (!item.getType().isJsonType()) {
                throw new SemanticException(
                        "-> operator could only be used for json column, but got " + item.getType());
            }
            node.setType(Type.JSON);
            return null;
        }

        @Override
        public Void visitLambdaFunctionExpr(LambdaFunctionExpr node, Scope scope) {
            if (scope.getLambdaInputs().size() == 0) {
                throw new SemanticException("Lambda Functions can only be used in high-order functions with arrays.");
            }
            if (scope.getLambdaInputs().size() != node.getChildren().size() - 1) {
                throw new SemanticException("Lambda arguments should equal to lambda input arrays.");
            }
            // process lambda arguments
            Set<String> set = new HashSet<>();
            List<LambdaArgument> args = Lists.newArrayList();
            for (int i = 1; i < node.getChildren().size(); ++i) {
                args.add((LambdaArgument) node.getChild(i));
                String name = ((LambdaArgument) node.getChild(i)).getName();
                if (set.contains(name)) {
                    throw new SemanticException("Lambda argument: " + name + " is duplicated.");
                }
                set.add(name);
                // bind argument with input arrays' data type and nullable info
                ((LambdaArgument) node.getChild(i)).setNullable(scope.getLambdaInputs().get(i - 1).isNullable());
                node.getChild(i).setType(scope.getLambdaInputs().get(i - 1).getType());
            }

            // construct a new scope to analyze the lambda function
            SimpleExpressionAnalyzer.analyzeExpression(node.getChild(0), this.analyzeState);
            node.setType(Type.FUNCTION);
            scope.clearLambdaInputs();
            return null;
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicate node, Scope scope) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                Type type = node.getChild(i).getType();
                if (!type.isBoolean() && !type.isNull()) {
                    throw new SemanticException("Operand '%s' part of predicate " +
                            "'%s' should return type 'BOOLEAN' but returns type '%s'.",
                            AstToStringBuilder.toString(node), AstToStringBuilder.toString(node.getChild(i)), type.toSql());
                }
            }

            node.setType(Type.BOOLEAN);
            return null;
        }

        @Override
        public Void visitBetweenPredicate(BetweenPredicate node, Scope scope) {
            predicateBaseAndCheck(node);

            List<Type> list = node.getChildren().stream().map(Expr::getType).collect(Collectors.toList());
            Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(list);

            for (Type type : list) {
                if (!Type.canCastTo(type, compatibleType)) {
                    throw new SemanticException(
                            "between predicate type " + type.toSql() + " with type " + compatibleType.toSql()
                                    + " is invalid.");
                }
            }

            return null;
        }

        @Override
        public Void visitBinaryPredicate(BinaryPredicate node, Scope scope) {
            Type type1 = node.getChild(0).getType();
            Type type2 = node.getChild(1).getType();

            Type compatibleType = TypeManager.getCompatibleTypeForBinary(node.getOp(), type1, type2);
            // check child type can be cast
            final String ERROR_MSG = "Column type %s does not support binary predicate operation.";
            if (!Type.canCastTo(type1, compatibleType)) {
                throw new SemanticException(String.format(ERROR_MSG, type1.toSql()));
            }

            if (!Type.canCastTo(type2, compatibleType)) {
                throw new SemanticException(String.format(ERROR_MSG, type1.toSql()));
            }

            node.setType(Type.BOOLEAN);
            return null;
        }

        @Override
        public Void visitArithmeticExpr(ArithmeticExpr node, Scope scope) {
            if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.BINARY_INFIX) {
                ArithmeticExpr.Operator op = node.getOp();
                Type t1 = node.getChild(0).getType().getNumResultType();
                Type t2 = node.getChild(1).getType().getNumResultType();
                if (t1.isDecimalV3() || t2.isDecimalV3()) {
                    try {
                        node.rewriteDecimalOperation();
                    } catch (AnalysisException ex) {
                        throw new SemanticException(ex.getMessage());
                    }
                    Type lhsType = node.getChild(0).getType();
                    Type rhsType = node.getChild(1).getType();
                    Type resultType = node.getType();
                    Type[] args = {lhsType, rhsType};
                    Function fn = Expr.getBuiltinFunction(op.getName(), args, Function.CompareMode.IS_IDENTICAL);
                    // In resolved function instance, it's argTypes and resultType are wildcard decimal type
                    // (both precision and and scale are -1, only used in function instance resolution), it's
                    // illegal for a function and expression to has a wildcard decimal type as its type in BE,
                    // so here substitute wildcard decimal types with real decimal types.
                    Function newFn = new ScalarFunction(fn.getFunctionName(), args, resultType, fn.hasVarArgs());
                    node.setType(resultType);
                    node.setFn(newFn);
                    return null;
                }
                // Find result type of this operator
                Type commonType;
                switch (op) {
                    case MULTIPLY:
                    case ADD:
                    case SUBTRACT:
                        // numeric ops must be promoted to highest-resolution type
                        // (otherwise we can't guarantee that a <op> b won't overflow/underflow)
                        commonType = ArithmeticExpr.getBiggerType(ArithmeticExpr.getCommonType(t1, t2));
                        break;
                    case MOD:
                        commonType = ArithmeticExpr.getCommonType(t1, t2);
                        break;
                    case DIVIDE:
                        commonType = ArithmeticExpr.getCommonType(t1, t2);
                        if (commonType.isFixedPointType()) {
                            commonType = Type.DOUBLE;
                        }
                        break;
                    case INT_DIVIDE:
                    case BITAND:
                    case BITOR:
                    case BITXOR:
                        commonType = ArithmeticExpr.getCommonType(t1, t2);
                        if (!commonType.isFixedPointType()) {
                            commonType = Type.BIGINT;
                        }
                        break;
                    case BIT_SHIFT_LEFT:
                    case BIT_SHIFT_RIGHT:
                    case BIT_SHIFT_RIGHT_LOGICAL:
                        commonType = t1;
                        break;
                    default:
                        // the programmer forgot to deal with a case
                        throw unsupportedException("Unknown arithmetic operation " + op + " in: " + node);
                }

                if (node.getChild(0).getType().equals(Type.NULL) && node.getChild(1).getType().equals(Type.NULL)) {
                    commonType = Type.NULL;
                }

                if (!Type.NULL.equals(node.getChild(0).getType()) && !Type.canCastTo(t1, commonType)) {
                    throw new SemanticException(
                            "cast type " + node.getChild(0).getType().toSql() + " with type " + commonType.toSql()
                                    + " is invalid.");
                }

                if (!Type.NULL.equals(node.getChild(1).getType()) && !Type.canCastTo(t2, commonType)) {
                    throw new SemanticException(
                            "cast type " + node.getChild(1).getType().toSql() + " with type " + commonType.toSql()
                                    + " is invalid.");
                }

                Function fn = Expr.getBuiltinFunction(op.getName(), new Type[] {commonType, commonType},
                        Function.CompareMode.IS_SUPERTYPE_OF);

                /*
                 * commonType is the common type of the parameters of the function,
                 * and fn.getReturnType() is the return type of the function after execution
                 * So we use fn.getReturnType() as node type
                 */
                node.setType(fn.getReturnType());
                node.setFn(fn);
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_PREFIX) {

                Function fn = Expr.getBuiltinFunction(
                        node.getOp().getName(), new Type[] {Type.BIGINT}, Function.CompareMode.IS_SUPERTYPE_OF);

                node.setType(Type.BIGINT);
                node.setFn(fn);
            } else if (node.getOp().getPos() == ArithmeticExpr.OperatorPosition.UNARY_POSTFIX) {
                throw unsupportedException("not yet implemented: expression analyzer for " + node.getClass().getName());
            } else {
                throw unsupportedException("not yet implemented: expression analyzer for " + node.getClass().getName());
            }

            return null;
        }

        List<String> addDateFunctions = Lists.newArrayList(FunctionSet.DATE_ADD,
                FunctionSet.ADDDATE, FunctionSet.DAYS_ADD, FunctionSet.TIMESTAMPADD);
        List<String> subDateFunctions = Lists.newArrayList(FunctionSet.DATE_SUB, FunctionSet.SUBDATE,
                FunctionSet.DAYS_SUB);

        @Override
        public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Scope scope) {
            node.setChild(0, TypeManager.addCastExpr(node.getChild(0), Type.DATETIME));

            String funcOpName;
            if (node.getFuncName() != null) {
                if (addDateFunctions.contains(node.getFuncName())) {
                    funcOpName = String.format("%sS_%s", node.getTimeUnitIdent(), "add");
                } else if (subDateFunctions.contains(node.getFuncName())) {
                    funcOpName = String.format("%sS_%s", node.getTimeUnitIdent(), "sub");
                } else {
                    node.setChild(1, TypeManager.addCastExpr(node.getChild(1), Type.DATETIME));
                    funcOpName = String.format("%sS_%s", node.getTimeUnitIdent(), "diff");
                }
            } else {
                funcOpName = String.format("%sS_%s", node.getTimeUnitIdent(),
                        (node.getOp() == ArithmeticExpr.Operator.ADD) ? "add" : "sub");
            }

            Type[] argumentTypes = node.getChildren().stream().map(Expr::getType)
                    .toArray(Type[]::new);
            Function fn = Expr.getBuiltinFunction(funcOpName.toLowerCase(), argumentTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                throw new SemanticException("No matching function with signature: %s(%s).", funcOpName, Joiner.on(", ")
                        .join(Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.toList())));
            }
            node.setType(fn.getReturnType());
            node.setFn(fn);
            return null;
        }

        @Override
        public Void visitExistsPredicate(ExistsPredicate node, Scope scope) {
            predicateBaseAndCheck(node);
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate node, Scope scope) {
            predicateBaseAndCheck(node);

            // check compatible type
            List<Type> list = node.getChildren().stream().map(Expr::getType).collect(Collectors.toList());
            Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(list);

            for (Type type : list) {
                // TODO(mofei) support it
                if (type.isJsonType()) {
                    throw new SemanticException("InPredicate of JSON is not supported");
                }
                if (!Type.canCastTo(type, compatibleType)) {
                    throw new SemanticException(
                            "in predicate type " + type.toSql() + " with type " + compatibleType.toSql()
                                    + " is invalid.");
                }
            }

            return null;
        }

        @Override
        public Void visitLiteral(LiteralExpr node, Scope scope) {
            if (node instanceof LargeIntLiteral) {
                BigInteger value = ((LargeIntLiteral) node).getValue();
                if (value.compareTo(LargeIntLiteral.LARGE_INT_MIN) < 0 ||
                        value.compareTo(LargeIntLiteral.LARGE_INT_MAX) > 0) {
                    throw new SemanticException("Number Overflow. literal: " + value);
                }
            }
            return null;
        }

        @Override
        public Void visitIsNullPredicate(IsNullPredicate node, Scope scope) {
            predicateBaseAndCheck(node);
            return null;
        }

        @Override
        public Void visitLikePredicate(LikePredicate node, Scope scope) {
            predicateBaseAndCheck(node);

            Type type1 = node.getChild(0).getType();
            Type type2 = node.getChild(1).getType();

            if (!type1.isStringType() && !type1.isNull()) {
                throw new SemanticException(
                        "left operand of " + node.getOp().toString() + " must be of type STRING: " +
                                AstToStringBuilder.toString(node));
            }

            if (!type2.isStringType() && !type2.isNull()) {
                throw new SemanticException(
                        "right operand of " + node.getOp().toString() + " must be of type STRING: " +
                                AstToStringBuilder.toString(node));
            }

            // check pattern
            if (LikePredicate.Operator.REGEXP.equals(node.getOp()) && !type2.isNull() && node.getChild(1).isLiteral()) {
                try {
                    Pattern.compile(((StringLiteral) node.getChild(1)).getValue());
                } catch (PatternSyntaxException e) {
                    throw new SemanticException("Invalid regular expression in '" + AstToStringBuilder.toString(node) + "'");
                }
            }

            return null;
        }

        // 1. set type = Type.BOOLEAN
        // 2. check child type is metric
        private void predicateBaseAndCheck(Predicate node) {
            node.setType(Type.BOOLEAN);

            for (Expr expr : node.getChildren()) {
                if (expr.getType().isOnlyMetricType() ||
                        (expr.getType() instanceof ArrayType && !(node instanceof IsNullPredicate))) {
                    throw new SemanticException("HLL, BITMAP, PERCENTILE and ARRAY type couldn't as Predicate");
                }
            }
        }

        @Override
        public Void visitCastExpr(CastExpr cast, Scope context) {
            Type castType;
            // If cast expr is implicit, targetTypeDef is null
            if (cast.isImplicit()) {
                castType = cast.getType();
            } else {
                castType = cast.getTargetTypeDef().getType();
            }
            if (!Type.canCastTo(cast.getChild(0).getType(), castType)) {
                throw new SemanticException("Invalid type cast from " + cast.getChild(0).getType().toSql() + " to "
                        + castType.toSql() + " in sql `" +
                        AstToStringBuilder.toString(cast.getChild(0)).replace("%", "%%") + "`");
            }

            cast.setType(castType);
            return null;
        }

        @Override
        public Void visitGroupingFunctionCall(GroupingFunctionCallExpr node, Scope scope) {
            if (node.getChildren().size() < 1) {
                throw new SemanticException("GROUPING functions required at least one parameters");
            }
            if (node.getChildren().stream().anyMatch(e -> !(e instanceof SlotRef))) {
                throw new SemanticException("grouping functions only support column.");
            }

            Type[] childTypes = new Type[1];
            childTypes[0] = Type.BIGINT;
            Function fn = Expr.getBuiltinFunction(node.getFnName().getFunction(),
                    childTypes, Function.CompareMode.IS_IDENTICAL);

            node.setFn(fn);
            node.setType(fn.getReturnType());
            return null;
        }

        @Override
        public Void visitCaseWhenExpr(CaseExpr node, Scope context) {
            int start = 0;
            int end = node.getChildren().size();

            Expr caseExpr = null;
            Expr elseExpr = null;

            if (node.hasCaseExpr()) {
                caseExpr = node.getChild(0);
                start++;
            }

            if (node.hasElseExpr()) {
                elseExpr = node.getChild(end - 1);
                end--;
            }

            // check is scalar type
            if (node.getChildren().stream().anyMatch(d -> !d.getType().isScalarType())) {
                throw new SemanticException("case-when only support scalar type");
            }

            // check when type
            List<Type> whenTypes = Lists.newArrayList();

            if (null != caseExpr) {
                whenTypes.add(caseExpr.getType());
            }

            for (int i = start; i < end; i = i + 2) {
                whenTypes.add(node.getChild(i).getType());
            }

            Type compatibleType = Type.NULL;
            if (null != caseExpr) {
                compatibleType = TypeManager.getCompatibleTypeForCaseWhen(whenTypes);
            }

            for (Type type : whenTypes) {
                if (!Type.canCastTo(type, compatibleType)) {
                    throw new SemanticException("Invalid when type cast " + type.toSql()
                            + " to " + compatibleType.toSql());
                }
            }

            // check then type/else type
            List<Type> thenTypes = Lists.newArrayList();

            for (int i = start + 1; i < end; i = i + 2) {
                thenTypes.add(node.getChild(i).getType());
            }

            if (null != elseExpr) {
                thenTypes.add(elseExpr.getType());
            }

            Type returnType = thenTypes.stream().allMatch(Type.NULL::equals) ? Type.BOOLEAN :
                    TypeManager.getCompatibleTypeForCaseWhen(thenTypes);
            for (Type type : thenTypes) {
                if (!Type.canCastTo(type, returnType)) {
                    throw new SemanticException("Invalid then type cast " + type.toSql()
                            + " to " + returnType.toSql());
                }
            }

            node.setType(returnType);
            return null;
        }

        @Override
        public Void visitSubquery(Subquery node, Scope context) {
            SimpleQueryAnalyzer queryAnalyzer = new SimpleQueryAnalyzer();
            queryAnalyzer.analyze(node.getQueryStatement());
            // Do not know the subquery type, use string as default
            node.setType(Type.STRING);
            return null;
        }

        @Override
        public Void visitAnalyticExpr(AnalyticExpr node, Scope context) {
            visit(node.getFnCall(), context);
            node.setType(node.getFnCall().getType());
            if (node.getWindow() != null) {
                if (node.getWindow().getLeftBoundary() != null &&
                        node.getWindow().getLeftBoundary().getExpr() != null) {
                    visit(node.getWindow().getLeftBoundary().getExpr(), context);
                }
                if (node.getWindow().getRightBoundary() != null &&
                        node.getWindow().getRightBoundary().getExpr() != null) {
                    visit(node.getWindow().getRightBoundary().getExpr(), context);
                }
            }
            node.getPartitionExprs().forEach(e -> visit(e, context));
            node.getOrderByElements().stream().map(OrderByElement::getExpr).forEach(e -> visit(e, context));
            verifyAnalyticExpression(node);
            return null;
        }

        @Override
        public Void visitDefaultValueExpr(DefaultValueExpr node, Scope context) {
            node.setType(Type.VARCHAR);
            return null;
        }

        @Override
        public Void visitCloneExpr(CloneExpr node, Scope context) {
            return null;
        }
    }


    public static void analyzeExpression(Expr expression, AnalyzeState state) {
        SimpleExpressionAnalyzer expressionAnalyzer = new SimpleExpressionAnalyzer();
        expressionAnalyzer.analyze(expression, state);
    }

}
