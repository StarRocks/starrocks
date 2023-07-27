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

package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.re2j.Pattern;
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
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.PlaceHolderExpr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.RolePrivilegeCollectionV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyticAnalyzer.verifyAnalyticExpression;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class ExpressionAnalyzer {
    private static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsT]+.*$");
    private final ConnectContext session;

    public ExpressionAnalyzer(ConnectContext session) {
        if (session == null) {
            // For some load requests, the ConnectContext will be null
            session = new ConnectContext();
        }
        this.session = session;
    }

    public void analyze(Expr expression, AnalyzeState analyzeState, Scope scope) {
        Visitor visitor = new Visitor(analyzeState, session);
        bottomUpAnalyze(visitor, expression, scope);
    }

    public void analyzeIgnoreSlot(Expr expression, AnalyzeState analyzeState, Scope scope) {
        IgnoreSlotVisitor visitor = new IgnoreSlotVisitor(analyzeState, session);
        bottomUpAnalyze(visitor, expression, scope);
    }

    private boolean isArrayHighOrderFunction(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            if (((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ARRAY_MAP) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ARRAY_FILTER) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ANY_MATCH) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ALL_MATCH) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.ARRAY_SORTBY)) {
                return true;
            } else if (((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.TRANSFORM)) {
                // transform just a alias of array_map
                ((FunctionCallExpr) expr).resetFnName("", FunctionSet.ARRAY_MAP);
                return true;
            }
        }
        return false;
    }

    private boolean isMapHighOrderFunction(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            if (((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.MAP_FILTER) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.TRANSFORM_VALUES) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.TRANSFORM_KEYS) ||
                    ((FunctionCallExpr) expr).getFnName().getFunction().equals(FunctionSet.MAP_APPLY)) {
                return true;
            }
        }
        return false;
    }

    private boolean isHighOrderFunction(Expr expr) {
        return isArrayHighOrderFunction(expr) || isMapHighOrderFunction(expr);
    }

    private void rewriteHighOrderFunction(Expr expr, Visitor visitor, Scope scope) {
        Preconditions.checkState(expr instanceof FunctionCallExpr);
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        if (!(functionCallExpr.getChild(0) instanceof LambdaFunctionExpr)) {
            return;
        }
        switch (functionCallExpr.getFnName().getFunction()) {
            case FunctionSet.ARRAY_FILTER: {
                // array_filter(lambda_func_expr, arr1...) -> array_filter(arr1, array_map(lambda_func_expr, arr1...))
                FunctionCallExpr arrayMap = new FunctionCallExpr(FunctionSet.ARRAY_MAP,
                        Lists.newArrayList(functionCallExpr.getChildren()));
                arrayMap.setType(Type.ARRAY_BOOLEAN);
                Expr arr1 = functionCallExpr.getChild(1);
                functionCallExpr.clearChildren();
                functionCallExpr.addChild(arr1);
                functionCallExpr.addChild(arrayMap);
                visitor.visit(arrayMap, scope);
                break;
            }
            case FunctionSet.ALL_MATCH:
            case FunctionSet.ANY_MATCH: {
                // func(lambda_func_expr, arr1...) -> func(array_map(lambda_func_expr, arr1...))
                FunctionCallExpr arrayMap = new FunctionCallExpr(FunctionSet.ARRAY_MAP,
                        Lists.newArrayList(functionCallExpr.getChildren()));
                arrayMap.setType(Type.ARRAY_BOOLEAN);
                functionCallExpr.clearChildren();
                functionCallExpr.addChild(arrayMap);
                visitor.visit(arrayMap, scope);
                break;
            }
            case FunctionSet.ARRAY_SORTBY: {
                // array_sortby(lambda_func_expr, arr1...) -> array_sortby(arr1, array_map(lambda_func_expr, arr1...))
                FunctionCallExpr arrayMap = new FunctionCallExpr(FunctionSet.ARRAY_MAP,
                        Lists.newArrayList(functionCallExpr.getChildren()));
                Expr arr1 = functionCallExpr.getChild(1);
                functionCallExpr.clearChildren();
                functionCallExpr.addChild(arr1);
                functionCallExpr.addChild(arrayMap);
                functionCallExpr.setType(arr1.getType());
                visitor.visit(arrayMap, scope);
                break;
            }
            case FunctionSet.MAP_FILTER:
                // map_filter((k,v)->(k,expr),map) -> map_filter(map, map_values(map_apply((k,v)->(k,expr),map)))
                FunctionCallExpr mapApply = new FunctionCallExpr(FunctionSet.MAP_APPLY,
                        Lists.newArrayList(functionCallExpr.getChildren()));
                Expr map = functionCallExpr.getChild(1);
                visitor.visit(mapApply, scope);

                FunctionCallExpr mapValues = new FunctionCallExpr(FunctionSet.MAP_VALUES, Lists.newArrayList(mapApply));
                visitor.visit(mapValues, scope);
                functionCallExpr.clearChildren();
                functionCallExpr.addChild(map);
                functionCallExpr.addChild(mapValues);
                break;
        }
    }

    // only high-order functions can use lambda functions.
    void analyzeHighOrderFunction(Visitor visitor, Expr expression, Scope scope) {
        if (!isHighOrderFunction(expression)) {
            String funcName = "";
            if (expression instanceof FunctionCallExpr) {
                funcName = ((FunctionCallExpr) expression).getFnName().getFunction();
            } else {
                funcName = expression.toString();
            }
            throw new SemanticException(funcName + " can't use lambda functions, " +
                    "as it is not a supported high-order function", expression.getPos());
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
        if (isArrayHighOrderFunction(expression)) {
            // the first child is lambdaFunction, following input arrays
            for (int i = 1; i < childSize; ++i) {
                Expr expr = expression.getChild(i);
                bottomUpAnalyze(visitor, expr, scope);
            }
            // putting lambda inputs should after analyze
            for (int i = 1; i < childSize; ++i) {
                Expr expr = expression.getChild(i);
                if (expr instanceof NullLiteral) {
                    expr.setType(Type.ARRAY_INT); // Let it have item type.
                }
                if (!expr.getType().isArrayType()) {
                    throw new SemanticException(i + "th lambda input should be arrays", expr.getPos());
                }
                Type itemType = ((ArrayType) expr.getType()).getItemType();
                scope.putLambdaInput(new PlaceHolderExpr(-1, expr.isNullable(), itemType));
            }
        } else {
            Preconditions.checkState(expression instanceof FunctionCallExpr);
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expression;
            // map_apply(func, map)
            if (functionCallExpr.getFnName().getFunction().equals(FunctionSet.MAP_APPLY)) {
                if (!(expression.getChild(0).getChild(0) instanceof MapExpr)) {
                    throw new SemanticException("The right part of map lambda function (" +
                            expression.getChild(0).toSql() + ") should have key and value arguments",
                            expression.getChild(0).getPos());
                }
            } else {
                if (expression.getChild(0).getChild(0) instanceof MapExpr) {
                    throw new SemanticException("The right part of map lambda function (" +
                            expression.getChild(0).toSql() + ") should have only one arguments",
                            expression.getChild(0).getPos());
                }
            }
            if (expression.getChild(0).getChildren().size() != 3) {
                Expr child = expression.getChild(0);
                throw new SemanticException("The left part of map lambda function (" +
                        child.toSql() + ") should have 2 arguments, but there are "
                        + (child.getChildren().size() - 1) + " arguments", child.getPos());
            }
            Expr expr = expression.getChild(1);
            bottomUpAnalyze(visitor, expr, scope);
            if (expr instanceof NullLiteral) {
                expr.setType(Type.ANY_MAP); // Let it have item type.
            }
            if (!expr.getType().isMapType()) {
                throw new SemanticException("Lambda inputs should be maps", expr.getPos());
            }
            Type keyType = ((MapType) expr.getType()).getKeyType();
            Type valueType = ((MapType) expr.getType()).getValueType();
            scope.putLambdaInput(new PlaceHolderExpr(-1, true, keyType));
            scope.putLambdaInput(new PlaceHolderExpr(-2, true, valueType));
            // lambda functions should be rewritten before visited
            if ((functionCallExpr.getFnName().getFunction().equals(FunctionSet.MAP_FILTER)) ||
                    functionCallExpr.getFnName().getFunction().equals(FunctionSet.TRANSFORM_VALUES)) {
                // (k,v) -> expr => (k,v) -> (k,expr)
                Expr lambdaFunc = functionCallExpr.getChild(0);
                LambdaArgument larg = (LambdaArgument) lambdaFunc.getChild(1);
                Expr slotRef = new SlotRef(null, larg.getName(), larg.getName());
                lambdaFunc.setChild(0, new MapExpr(Type.ANY_MAP, Lists.newArrayList(slotRef,
                        lambdaFunc.getChild(0))));
                if (functionCallExpr.getFnName().getFunction().equals(FunctionSet.TRANSFORM_VALUES)) {
                    functionCallExpr.resetFnName("", FunctionSet.MAP_APPLY);
                }
            } else if ((functionCallExpr.getFnName().getFunction().equals(FunctionSet.TRANSFORM_KEYS))) {
                // (k,v) -> expr => (k,v) -> (expr, v)
                Expr lambdaFunc = functionCallExpr.getChild(0);
                LambdaArgument larg = (LambdaArgument) lambdaFunc.getChild(2);
                Expr slotRef = new SlotRef(null, larg.getName(), larg.getName());
                lambdaFunc.setChild(0, new MapExpr(Type.ANY_MAP, Lists.newArrayList(
                        lambdaFunc.getChild(0), slotRef)));
                functionCallExpr.resetFnName("", FunctionSet.MAP_APPLY);
            }
        }
        // visit LambdaFunction
        visitor.visit(expression.getChild(0), scope);
        rewriteHighOrderFunction(expression, visitor, scope);
        scope.clearLambdaInputs();
    }

    private void bottomUpAnalyze(Visitor visitor, Expr expression, Scope scope) {
        if (expression.hasLambdaFunction(expression)) {
            analyzeHighOrderFunction(visitor, expression, scope);
        } else {
            for (Expr expr : expression.getChildren()) {
                bottomUpAnalyze(visitor, expr, scope);
            }
        }
        visitor.visit(expression, scope);
    }

    public static class Visitor extends AstVisitor<Void, Scope> {
        private static final List<String> ADD_DATE_FUNCTIONS = Lists.newArrayList(FunctionSet.DATE_ADD,
                FunctionSet.ADDDATE, FunctionSet.DAYS_ADD, FunctionSet.TIMESTAMPADD);
        private static final List<String> SUB_DATE_FUNCTIONS =
                Lists.newArrayList(FunctionSet.DATE_SUB, FunctionSet.SUBDATE,
                        FunctionSet.DAYS_SUB);

        private final AnalyzeState analyzeState;
        private final ConnectContext session;

        public Visitor(AnalyzeState analyzeState, ConnectContext session) {
            this.analyzeState = analyzeState;
            this.session = session;
        }

        @Override
        public Void visitExpression(Expr node, Scope scope) {
            throw new SemanticException("not yet implemented: expression analyzer for " + node.getClass().getName(),
                    node.getPos());
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
            if (!child.getType().isStructType()) {
                throw new SemanticException(child.toSql() + " must be a struct type, check if you are using `'`",
                        child.getPos());
            }

            List<String> fieldNames = node.getFieldNames();
            List<String> rightNames = Lists.newArrayList();
            Type tmpType = child.getType();
            for (String fieldName : fieldNames) {
                StructType structType = (StructType) tmpType;
                StructField structField = structType.getField(fieldName);
                if (structField == null) {
                    throw new SemanticException(String.format("Struct subfield '%s' cannot be resolved", fieldName),
                            node.getPos());
                }
                rightNames.add(structField.getName());
                tmpType = structField.getType();
            }

            // set right field names
            node.setFieldNames(rightNames);
            node.setType(tmpType);
            return null;
        }

        @Override
        public Void visitSlot(SlotRef node, Scope scope) {
            ResolvedField resolvedField = scope.resolveField(node);
            node.setType(resolvedField.getField().getType());
            node.setTblName(resolvedField.getField().getRelationAlias());
            // help to get nullable info in Analyzer phase
            // now it is used in creating mv to decide nullable of fields
            node.setNullable(resolvedField.getField().isNullable());

            if (node.getType().isStructType()) {
                // If SlotRef is a struct type, it needs special treatment, reset SlotRef's col, label name.
                node.setCol(resolvedField.getField().getName());
                node.setLabel(resolvedField.getField().getName());

                if (resolvedField.getField().getTmpUsedStructFieldPos().size() > 0) {
                    // This SlotRef is accessing subfield
                    node.setUsedStructFieldPos(resolvedField.getField().getTmpUsedStructFieldPos());
                    node.resetStructInfo();
                }
            }

            handleResolvedField(node, resolvedField);
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
        public Void visitMapExpr(MapExpr node, Scope scope) {
            if (!node.getChildren().isEmpty()) {
                Type originalType = node.getType();
                if (originalType == Type.ANY_MAP) {
                    Type keyType = node.getKeyCommonType();
                    Type valueType = node.getValueCommonType();
                    node.setType(new MapType(keyType, valueType));
                }
            } else {
                node.setType(new MapType(Type.NULL, Type.NULL));
            }
            return null;
        }

        @Override
        public Void visitCollectionElementExpr(CollectionElementExpr node, Scope scope) {
            Expr expr = node.getChild(0);
            Expr subscript = node.getChild(1);
            if (expr.getType().isArrayType()) {
                if (!subscript.getType().isNumericType()) {
                    throw new SemanticException("array subscript must have type integer", subscript.getPos());
                }
                try {
                    if (subscript.getType().getPrimitiveType() != PrimitiveType.INT) {
                        node.castChild(Type.INT, 1);
                    }
                    node.setType(((ArrayType) expr.getType()).getItemType());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else if (expr.getType().isMapType()) {
                try {
                    if (subscript.getType().getPrimitiveType() !=
                            ((MapType) expr.getType()).getKeyType().getPrimitiveType()) {
                        node.castChild(((MapType) expr.getType()).getKeyType(), 1);
                    }
                    node.setType(((MapType) expr.getType()).getValueType());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else if (expr.getType().isStructType()) {
                if (!(subscript instanceof IntLiteral)) {
                    throw new SemanticException("struct subscript must have integer pos", subscript.getPos());
                }
                long index = ((IntLiteral) subscript).getValue();
                long fieldSize = ((StructType) expr.getType()).getFields().size();
                if (fieldSize < Math.abs(index)) {
                    throw new SemanticException("the pos is out of struct subfields", subscript.getPos());
                } else if (index == 0) {
                    throw new SemanticException("the pos can't set to zero", subscript.getPos());
                }

                index = index > 0 ? index - 1 : fieldSize + index;
                StructField structField = ((StructType) expr.getType()).getFields().get((int) index);
                node.setType(structField.getType());
            } else {
                throw new SemanticException("cannot subscript type " + expr.getType()
                        + " because it is not an array or a map or a struct", expr.getPos());
            }

            return null;
        }

        @Override
        public Void visitArraySliceExpr(ArraySliceExpr node, Scope scope) {
            if (!node.getChild(0).getType().isArrayType()) {
                throw new SemanticException("cannot subscript type" +
                        node.getChild(0).getType() + " because it is not an array", node.getChild(0).getPos());
            }
            node.setType(node.getChild(0).getType());
            return null;
        }

        @Override
        public Void visitArrowExpr(ArrowExpr node, Scope scope) {
            Expr item = node.getChild(0);
            Expr key = node.getChild(1);
            if (!key.isLiteral() || !key.getType().isStringType()) {
                throw new SemanticException("right operand of -> should be string literal, but got " + key,
                        key.getPos());
            }
            if (!item.getType().isJsonType()) {
                throw new SemanticException(
                        "-> operator could only be used for json column, but got " + item.getType(), item.getPos());
            }
            node.setType(Type.JSON);
            return null;
        }

        @Override
        public Void visitLambdaFunctionExpr(LambdaFunctionExpr node, Scope scope) { // (x,y) -> x+y or (k,v) -> (k1,v1)
            if (scope.getLambdaInputs().size() == 0) {
                throw new SemanticException(
                        "Lambda Functions can only be used in high-order functions with arrays/maps",
                        node.getPos());
            }
            if (scope.getLambdaInputs().size() != node.getChildren().size() - 1) {
                throw new SemanticException("Lambda arguments should equal to lambda input arrays", node.getPos());
            }

            // process lambda arguments
            Set<String> set = new HashSet<>();
            List<LambdaArgument> args = Lists.newArrayList();
            for (int i = 1; i < node.getChildren().size(); ++i) {
                args.add((LambdaArgument) node.getChild(i));
                String name = ((LambdaArgument) node.getChild(i)).getName();
                if (set.contains(name)) {
                    throw new SemanticException("Lambda argument: " + name + " is duplicated",
                            node.getChild(i).getPos());
                }
                set.add(name);
                // bind argument with input arrays' data type and nullable info
                ((LambdaArgument) node.getChild(i)).setNullable(scope.getLambdaInputs().get(i - 1).isNullable());
                node.getChild(i).setType(scope.getLambdaInputs().get(i - 1).getType());
            }

            // construct a new scope to analyze the lambda function
            Scope lambdaScope = new Scope(args, scope);
            ExpressionAnalyzer.analyzeExpression(node.getChild(0), this.analyzeState, lambdaScope, this.session);
            node.setType(Type.FUNCTION);
            scope.clearLambdaInputs();
            return null;
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicate node, Scope scope) {
            node.setType(Type.BOOLEAN);
            for (int i = 0; i < node.getChildren().size(); i++) {
                Expr child = node.getChild(i);
                if (child.getType().isBoolean() || child.getType().isNull()) {
                    // do nothing
                } else if (!session.getSessionVariable().isEnableStrictType() &&
                        Type.canCastTo(child.getType(), Type.BOOLEAN)) {
                    node.getChildren().set(i, new CastExpr(Type.BOOLEAN, child));
                } else {
                    throw new SemanticException(child.toSql() + " can not be converted to boolean type.");
                }
            }
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
                                    + " is invalid", node.getPos());
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
            final String ERROR_MSG = "Column type %s does not support binary predicate operation with type %s";
            if (!Type.canCastTo(type1, compatibleType)) {
                throw new SemanticException(String.format(ERROR_MSG, type1.toSql(), type2.toSql()), node.getPos());
            }

            if (!Type.canCastTo(type2, compatibleType)) {
                throw new SemanticException(String.format(ERROR_MSG, type1.toSql(), type2.toSql()), node.getPos());
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
                    ArithmeticExpr.TypeTriple typeTriple = null;
                    try {
                        typeTriple = node.rewriteDecimalOperation();
                    } catch (AnalysisException ex) {
                        throw new SemanticException(ex.getMessage());
                    }
                    Preconditions.checkArgument(typeTriple != null);
                    Type[] args = {typeTriple.lhsTargetType, typeTriple.rhsTargetType};
                    Function fn = Expr.getBuiltinFunction(op.getName(), args, Function.CompareMode.IS_IDENTICAL);
                    // In resolved function instance, it's argTypes and resultType are wildcard decimal type
                    // (both precision and and scale are -1, only used in function instance resolution), it's
                    // illegal for a function and expression to has a wildcard decimal type as its type in BE,
                    // so here substitute wildcard decimal types with real decimal types.
                    Function newFn = new ScalarFunction(fn.getFunctionName(), args, typeTriple.returnType, fn.hasVarArgs());
                    node.setType(typeTriple.returnType);
                    node.setFn(newFn);
                    return null;
                }
                // Find result type of this operator
                Type lhsType;
                Type rhsType;
                switch (op) {
                    case MULTIPLY:
                    case ADD:
                    case SUBTRACT:
                        // numeric ops must be promoted to highest-resolution type
                        // (otherwise we can't guarantee that a <op> b won't overflow/underflow)
                        lhsType = ArithmeticExpr.getBiggerType(ArithmeticExpr.getCommonType(t1, t2));
                        rhsType = lhsType;
                        break;
                    case MOD:
                        lhsType = ArithmeticExpr.getCommonType(t1, t2);
                        rhsType = lhsType;
                        break;
                    case DIVIDE:
                        lhsType = ArithmeticExpr.getCommonType(t1, t2);
                        if (lhsType.isFixedPointType()) {
                            lhsType = Type.DOUBLE;
                        }
                        rhsType = lhsType;
                        break;
                    case INT_DIVIDE:
                    case BITAND:
                    case BITOR:
                    case BITXOR:
                        lhsType = ArithmeticExpr.getCommonType(t1, t2);
                        if (!lhsType.isFixedPointType()) {
                            lhsType = Type.BIGINT;
                        }
                        rhsType = lhsType;
                        break;
                    case BIT_SHIFT_LEFT:
                    case BIT_SHIFT_RIGHT:
                    case BIT_SHIFT_RIGHT_LOGICAL:
                        lhsType = t1;
                        // only support bigint function
                        rhsType = Type.BIGINT;
                        break;
                    default:
                        // the programmer forgot to deal with a case
                        throw new SemanticException("Unknown arithmetic operation " + op + " in: " + node,
                                node.getPos());
                }

                if (node.getChild(0).getType().equals(Type.NULL) && node.getChild(1).getType().equals(Type.NULL)) {
                    lhsType = Type.NULL;
                    rhsType = Type.NULL;
                }

                if (lhsType.isInvalid() || rhsType.isInvalid()) {
                    throw new SemanticException("Any function type can not cast to " + Type.INVALID.toSql());
                }

                if (!Type.NULL.equals(node.getChild(0).getType()) && !Type.canCastTo(t1, lhsType)) {
                    throw new SemanticException(
                            "cast type " + node.getChild(0).getType().toSql() + " with type " + lhsType.toSql()
                                    + " is invalid", node.getPos());
                }

                if (!Type.NULL.equals(node.getChild(1).getType()) && !Type.canCastTo(t2, rhsType)) {
                    throw new SemanticException(
                            "cast type " + node.getChild(1).getType().toSql() + " with type " + rhsType.toSql()
                                    + " is invalid", node.getPos());
                }

                Function fn = Expr.getBuiltinFunction(op.getName(), new Type[] {lhsType, rhsType},
                        Function.CompareMode.IS_SUPERTYPE_OF);

                if (fn == null) {
                    throw new SemanticException(String.format(
                            "No matching function '%s' with operand types %s and %s", node.getOp().getName(), t1, t2));
                }

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
                throw new SemanticException("not yet implemented: expression analyzer for " + node.getClass().getName(),
                        node.getPos());
            } else {
                throw new SemanticException("not yet implemented: expression analyzer for " + node.getClass().getName(),
                        node.getPos());
            }

            return null;
        }

        @Override
        public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Scope scope) {
            node.setChild(0, TypeManager.addCastExpr(node.getChild(0), Type.DATETIME));

            String funcOpName;
            if (node.getFuncName() != null) {
                if (ADD_DATE_FUNCTIONS.contains(node.getFuncName())) {
                    funcOpName = String.format("%sS_%s", node.getTimeUnitIdent(), "add");
                } else if (SUB_DATE_FUNCTIONS.contains(node.getFuncName())) {
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
                String msg = String.format("No matching function with signature: %s(%s)", funcOpName, Joiner.on(", ")
                        .join(Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.toList())));
                throw new SemanticException(msg, node.getPos());
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

            List<Expr> queryExpressions = Lists.newArrayList();
            node.collect(arg -> arg instanceof Subquery, queryExpressions);
            if (queryExpressions.size() > 0 && node.getChildren().size() > 2) {
                throw new SemanticException("In Predicate only support literal expression list", node.getPos());
            }

            // check compatible type
            List<Type> list = node.getChildren().stream().map(Expr::getType).collect(Collectors.toList());
            Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(list);

            if (compatibleType == Type.INVALID) {
                throw new SemanticException("The input types (" + list.stream().map(Type::toSql).collect(
                        Collectors.joining(",")) + ") of in predict are not compatible", node.getPos());
            }

            for (Expr child : node.getChildren()) {
                Type type = child.getType();
                if (type.isJsonType() && queryExpressions.size() > 0) { // TODO: enable it after support join on JSON
                    throw new SemanticException("In predicate of JSON does not support subquery", child.getPos());
                }
                if (!Type.canCastTo(type, compatibleType)) {
                    throw new SemanticException(
                            "in predicate type " + type.toSql() + " with type " + compatibleType.toSql()
                                    + " is invalid", child.getPos());
                }
            }

            return null;
        }

        @Override
        public Void visitMultiInPredicate(MultiInPredicate node, Scope scope) {
            predicateBaseAndCheck(node);
            List<Type> leftTypes =
                    node.getChildren().stream().limit(node.getNumberOfColumns()).map(Expr::getType)
                            .collect(Collectors.toList());

            Subquery inSubquery = (Subquery) node.getChild(node.getNumberOfColumns());
            List<Type> rightTypes =
                    inSubquery.getQueryStatement().getQueryRelation().getOutputExpression().stream().map(Expr::getType).
                            collect(Collectors.toList());
            if (leftTypes.size() != rightTypes.size()) {
                throw new SemanticException(
                        "subquery must return the same number of columns as provided by the IN predicate",
                        node.getPos());
            }

            for (int i = 0; i < rightTypes.size(); ++i) {
                if (leftTypes.get(i).isJsonType() || rightTypes.get(i).isJsonType() || leftTypes.get(i).isMapType() ||
                        rightTypes.get(i).isMapType() || leftTypes.get(i).isStructType() ||
                        rightTypes.get(i).isStructType()) {
                    throw new SemanticException("InPredicate of JSON, Map, Struct types is not supported");
                }
                if (!Type.canCastTo(leftTypes.get(i), rightTypes.get(i))) {
                    throw new SemanticException(
                            "in predicate type " + leftTypes.get(i).toSql() + " with type " + rightTypes.get(i).toSql()
                                    + " is invalid");
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
                    throw new SemanticException(PARSER_ERROR_MSG.numOverflow(value.toString()), node.getPos());
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
                                AstToStringBuilder.toString(node), node.getPos());
            }

            if (!type2.isStringType() && !type2.isNull()) {
                throw new SemanticException(
                        "right operand of " + node.getOp().toString() + " must be of type STRING: " +
                                AstToStringBuilder.toString(node), node.getPos());
            }

            // check pattern
            if (LikePredicate.Operator.REGEXP.equals(node.getOp()) && !type2.isNull() && node.getChild(1).isLiteral()) {
                try {
                    Pattern.compile(((StringLiteral) node.getChild(1)).getValue());
                } catch (PatternSyntaxException e) {
                    throw new SemanticException(
                            "Invalid regular expression in '" + AstToStringBuilder.toString(node) + "'", node.getPos());
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
                        (expr.getType().isComplexType() && !(node instanceof IsNullPredicate) &&
                                !(node instanceof InPredicate))) {
                    throw new SemanticException(
                            "HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate", node.getPos());
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
                        AstToStringBuilder.toString(cast.getChild(0)).replace("%", "%%") + "`",
                        cast.getPos());
            }

            cast.setType(castType);
            return null;
        }

        @Override
        public Void visitFunctionCall(FunctionCallExpr node, Scope scope) {
            Type[] argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);

            if (node.isNondeterministicBuiltinFnName()) {
                ExprId exprId = analyzeState.getNextNondeterministicId();
                node.setNondeterministicId(exprId);
            }

            Function fn;
            String fnName = node.getFnName().getFunction();

            // throw exception direct
            checkFunction(fnName, node, argumentTypes);

            if (fnName.equals(FunctionSet.COUNT) && node.getParams().isDistinct()) {
                // Compatible with the logic of the original search function "count distinct"
                // TODO: fix how we equal count distinct.
                fn = Expr.getBuiltinFunction(FunctionSet.COUNT, new Type[] {argumentTypes[0]},
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            } else if (fnName.equals(FunctionSet.EXCHANGE_BYTES) || fnName.equals(FunctionSet.EXCHANGE_SPEED)) {
                fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                fn = fn.copy();
                fn.setArgsType(argumentTypes); // as accepting various types
                fn.setIsNullable(false);
            } else if (fnName.equals(FunctionSet.ARRAY_AGG)) {
                // move order by expr to node child, and extract is_asc and null_first information.
                fn = Expr.getBuiltinFunction(fnName, new Type[] {argumentTypes[0]},
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                fn = fn.copy();
                List<OrderByElement> orderByElements = node.getParams().getOrderByElements();
                List<Boolean> isAscOrder = new ArrayList<>();
                List<Boolean> nullsFirst = new ArrayList<>();
                if (orderByElements != null) {
                    for (OrderByElement elem : orderByElements) {
                        isAscOrder.add(elem.getIsAsc());
                        nullsFirst.add(elem.getNullsFirstParam());
                    }
                }
                Type[] argsTypes = new Type[argumentTypes.length];
                for (int i = 0; i < argumentTypes.length; ++i) {
                    argsTypes[i] = argumentTypes[i] == Type.NULL ? Type.BOOLEAN : argumentTypes[i];
                }
                fn.setArgsType(argsTypes); // as accepting various types
                ArrayList<Type> structTypes = new ArrayList<>(argsTypes.length);
                for (Type t : argsTypes) {
                    structTypes.add(new ArrayType(t));
                }
                ((AggregateFunction) fn).setIntermediateType(new StructType(structTypes));
                ((AggregateFunction) fn).setIsAscOrder(isAscOrder);
                ((AggregateFunction) fn).setNullsFirst(nullsFirst);
                fn.setRetType(new ArrayType(argsTypes[0]));     // return null if scalar agg with empty input
            } else if (FunctionSet.PERCENTILE_DISC.equals(fnName)) {
                argumentTypes[1] = Type.DOUBLE;
                fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
                // correct decimal's precision and scale
                if (fn.getArgs()[0].isDecimalV3()) {
                    List<Type> argTypes = Arrays.asList(argumentTypes[0], fn.getArgs()[1]);

                    AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), argTypes, argumentTypes[0],
                            ((AggregateFunction) fn).getIntermediateType(), fn.hasVarArgs());

                    newFn.setFunctionId(fn.getFunctionId());
                    newFn.setChecksum(fn.getChecksum());
                    newFn.setBinaryType(fn.getBinaryType());
                    newFn.setHasVarArgs(fn.hasVarArgs());
                    newFn.setId(fn.getId());
                    newFn.setUserVisible(fn.isUserVisible());
                    newFn.setisAnalyticFn(((AggregateFunction) fn).isAnalyticFn());

                    fn = newFn;
                }
            } else if (FunctionSet.CONCAT.equals(fnName) && node.getChildren().stream().anyMatch(child ->
                    child.getType().isArrayType())) {
                List<Type> arrayTypes = Arrays.stream(argumentTypes).map(argumentType -> {
                    if (argumentType.isArrayType()) {
                        return argumentType;
                    } else {
                        return new ArrayType(argumentType);
                    }
                }).collect(Collectors.toList());
                // check if all array types are compatible
                TypeManager.getCommonSuperType(arrayTypes);
                for (int i = 0; i < argumentTypes.length; ++i) {
                    if (!argumentTypes[i].isArrayType()) {
                        node.setChild(i, new ArrayExpr(new ArrayType(argumentTypes[i]),
                                Lists.newArrayList(node.getChild(i))));
                    }
                }

                argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
                node.resetFnName(null, FunctionSet.ARRAY_CONCAT);
                if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV3(FunctionSet.ARRAY_CONCAT, argumentTypes)) {
                    fn = DecimalV3FunctionAnalyzer.getDecimalV3Function(session, node, argumentTypes);
                } else {
                    fn = Expr.getBuiltinFunction(FunctionSet.ARRAY_CONCAT, argumentTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                }
            } else if (FunctionSet.NAMED_STRUCT.equals(fnName)) {
                // deriver struct type
                fn = Expr.getBuiltinFunction(FunctionSet.NAMED_STRUCT, argumentTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                fn = fn.copy();
                ArrayList<StructField> sf = Lists.newArrayList();
                for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                    StringLiteral literal = (StringLiteral) node.getChild(i);
                    sf.add(new StructField(literal.getStringValue(), node.getChild(i + 1).getType()));
                }
                fn.setRetType(new StructType(sf));
            } else if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV3(fnName, argumentTypes)) {
                // Since the priority of decimal version is higher than double version (according functionId),
                // and in `Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF` mode, `Expr.getBuiltinFunction` always
                // return decimal version even if the input parameters are not decimal, such as (INT, INT),
                // lacking of specific decimal type process defined in `getDecimalV3Function`. So we force round functions
                // to go through `getDecimalV3Function` here
                fn = DecimalV3FunctionAnalyzer.getDecimalV3Function(session, node, argumentTypes);
            } else if (Arrays.stream(argumentTypes).anyMatch(arg -> arg.matchesType(Type.TIME))) {
                fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                if (fn instanceof AggregateFunction) {
                    throw new SemanticException("Time Type can not used in" + fnName + " function", node.getPos());
                }
            } else if (FunctionSet.STR_TO_DATE.equals(fnName)) {
                fn = getStrToDateFunction(node, argumentTypes);
            } else if (FunctionSet.ARRAY_GENERATE.equals(fnName)) {
                fn = getArrayGenerateFunction(node);
                argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
            } else if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV2(fnName, argumentTypes)) {
                fn = DecimalV3FunctionAnalyzer.getDecimalV2Function(node, argumentTypes);
            } else {
                fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }

            if (fn == null) {
                fn = AnalyzerUtils.getUdfFunction(session, node.getFnName(), argumentTypes);
            }

            if (fn == null) {
                String msg = String.format("No matching function with signature: %s(%s)",
                        fnName,
                        node.getParams().isStar() ? "*" : Joiner.on(", ")
                                .join(Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.toList())));
                throw new SemanticException(msg, node.getPos());
            }

            if (fn instanceof TableFunction) {
                throw new SemanticException("Table function cannot be used in expression", node.getPos());
            }

            for (int i = 0; i < fn.getNumArgs(); i++) {
                if (!argumentTypes[i].matchesType(fn.getArgs()[i]) &&
                        !Type.canCastTo(argumentTypes[i], fn.getArgs()[i])) {
                    String msg = String.format("No matching function with signature: %s(%s)", fnName,
                            node.getParams().isStar() ? "*" :
                                    Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.joining(", ")));
                    throw new SemanticException(msg, node.getPos());
                }
            }

            if (fn.hasVarArgs()) {
                Type varType = fn.getArgs()[fn.getNumArgs() - 1];
                for (int i = fn.getNumArgs(); i < argumentTypes.length; i++) {
                    if (!argumentTypes[i].matchesType(varType) &&
                            !Type.canCastTo(argumentTypes[i], varType)) {
                        String msg = String.format("Variadic function %s(%s) can't support type: %s", fnName,
                                Arrays.stream(fn.getArgs()).map(Type::toSql).collect(Collectors.joining(", ")),
                                argumentTypes[i]);
                        throw new SemanticException(msg, node.getPos());
                    }
                }
            }

            node.setFn(fn);
            node.setType(fn.getReturnType());
            FunctionAnalyzer.analyze(node);
            return null;
        }

        private void checkFunction(String fnName, FunctionCallExpr node, Type[] argumentTypes) {
            switch (fnName) {
                case FunctionSet.TIME_SLICE:
                case FunctionSet.DATE_SLICE:
                    if (!(node.getChild(1) instanceof IntLiteral)) {
                        throw new SemanticException(
                                fnName + " requires second parameter must be a constant interval", node.getPos());
                    }
                    if (((IntLiteral) node.getChild(1)).getValue() <= 0) {
                        throw new SemanticException(
                                fnName + " requires second parameter must be greater than 0", node.getPos());
                    }
                    break;
                case FunctionSet.ARRAY_FILTER:
                    if (node.getChildren().size() != 2) {
                        throw new SemanticException(fnName + " should have 2 array inputs or lambda functions",
                                node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The first input of " + fnName +
                                " should be an array or a lambda function", node.getPos());
                    }
                    if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                        throw new SemanticException("The second input of " + fnName +
                                " should be an array or a lambda function", node.getPos());
                    }
                    // force the second array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(1).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException("The second input of array_filter " +
                                node.getChild(1).getType().toString() + "  can't cast to ARRAY<BOOL>", node.getPos());
                    }
                    break;
                case FunctionSet.ALL_MATCH:
                case FunctionSet.ANY_MATCH:
                    if (node.getChildren().size() != 1) {
                        throw new SemanticException(fnName + " should have a input array", node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The first input of " + fnName + " should be an array",
                                node.getPos());
                    }
                    // force the second array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(0).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException("The second input of " + fnName +
                                node.getChild(0).getType().toString() + "  can't cast to ARRAY<BOOL>", node.getPos());
                    }
                    break;
                case FunctionSet.ARRAY_SORTBY:
                    if (node.getChildren().size() != 2) {
                        throw new SemanticException(fnName + " should have 2 array inputs or lambda functions",
                                node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The first input of " + fnName +
                                " should be an array or a lambda function", node.getPos());
                    }
                    if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                        throw new SemanticException("The second input of " + fnName +
                                " should be an array or a lambda function", node.getPos());
                    }
                    break;
                case FunctionSet.ARRAY_GENERATE:
                    if (node.getChildren().size() < 1 || node.getChildren().size() > 3) {
                        throw new SemanticException(fnName + " has wrong input numbers");
                    }
                    for (Expr expr : node.getChildren()) {
                        if ((expr instanceof SlotRef) && node.getChildren().size() != 3) {
                            throw new SemanticException(fnName + " with IntColumn doesn't support default parameters");
                        }
                        if (!(expr instanceof IntLiteral) && !(expr instanceof LargeIntLiteral) &&
                                !(expr instanceof SlotRef) && !(expr instanceof NullLiteral)) {
                            throw new SemanticException(fnName + "'s parameter only support Integer");
                        }
                    }
                    break;
                case FunctionSet.MAP_FILTER:
                    if (node.getChildren().size() != 2) {
                        throw new SemanticException(fnName + " should have 2 inputs, " +
                                "but there are just " + node.getChildren().size() + " inputs.");
                    }
                    if (!node.getChild(0).getType().isMapType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The first input of " + fnName +
                                " should be a map or a lambda function.");
                    }
                    if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                        throw new SemanticException("The second input of " + fnName +
                                " should be a array or a lambda function.");
                    }
                    // force the second array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(1).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException("The second input of map_filter " +
                                node.getChild(1).getType().toString() + "  can't cast to ARRAY<BOOL>");
                    }
                    break;
                case FunctionSet.ARRAY_AGG: {
                    for (int i = 1; i < argumentTypes.length; ++i) {
                        if (argumentTypes[i].isComplexType()) {
                            throw new SemanticException("array_agg can't support order by nested types, " +
                                    "but " + i + "-th input is " + argumentTypes[i].toSql());
                        }
                    }
                    break;
                }
                case FunctionSet.NAMED_STRUCT: {
                    if (node.getChildren().size() < 2) {
                        throw new SemanticException(fnName + " should have at least two inputs", node.getPos());
                    }
                    if (node.getChildren().size() % 2 != 0) {
                        throw new SemanticException(fnName + " arguments must be in name/value pairs", node.getPos());
                    }

                    Set<String> check = Sets.newHashSet();
                    for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                        if (!(node.getChild(i) instanceof StringLiteral)) {
                            throw new SemanticException(
                                    "The " + (i + 1) + "-th input of named_struct must be string literal",
                                    node.getPos());
                        }

                        String name = ((StringLiteral) node.getChild(i)).getValue();
                        if (check.contains(name.toLowerCase())) {
                            throw new SemanticException("named_struct contains duplicate subfield name: " +
                                    name + " at " + (i + 1) + "-th input", node.getPos());
                        }

                        check.add(name.toLowerCase());
                    }
                    break;
                }
                case FunctionSet.ROW: {
                    if (node.getChildren().size() < 1) {
                        throw new SemanticException(fnName + " should have at least one input.", node.getPos());
                    }
                    break;
                }
                case FunctionSet.ARRAY_AVG:
                case FunctionSet.ARRAY_MAX:
                case FunctionSet.ARRAY_MIN:
                case FunctionSet.ARRAY_SORT:
                case FunctionSet.ARRAY_SUM:
                case FunctionSet.ARRAY_CUM_SUM:
                case FunctionSet.ARRAY_DIFFERENCE:
                case FunctionSet.ARRAY_DISTINCT:
                case FunctionSet.ARRAY_LENGTH:
                case FunctionSet.ARRAY_TO_BITMAP: {
                    if (node.getChildren().size() != 1) {
                        throw new SemanticException(fnName + " should have only one input", node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The only one input of " + fnName +
                                " should be an array, rather than " + node.getChild(0).getType().toSql(),
                                node.getPos());
                    }

                    break;
                }
                case FunctionSet.ARRAY_CONTAINS_ALL:
                case FunctionSet.ARRAYS_OVERLAP: {
                    if (node.getChildren().size() != 2) {
                        throw new SemanticException(fnName + " should have only two inputs", node.getPos());
                    }
                    for (int i = 0; i < node.getChildren().size(); ++i) {
                        if (!node.getChild(i).getType().isArrayType() && !node.getChild(i).getType().isNull()) {
                            throw new SemanticException((i + 1) + "-th input of " + fnName +
                                    " should be an array, rather than " + node.getChild(i).getType().toSql(),
                                    node.getPos());
                        }
                    }
                    break;
                }
                case FunctionSet.ARRAY_INTERSECT:
                case FunctionSet.ARRAY_CONCAT: {
                    if (node.getChildren().isEmpty()) {
                        throw new SemanticException(fnName + " should have at least one input.", node.getPos());
                    }
                    for (int i = 0; i < node.getChildren().size(); ++i) {
                        if (!node.getChild(i).getType().isArrayType() && !node.getChild(i).getType().isNull()) {
                            throw new SemanticException((i + 1) + "-th input of " + fnName +
                                    " should be an array, rather than " + node.getChild(i).getType().toSql(),
                                    node.getPos());
                        }
                    }
                    break;
                }
                case FunctionSet.ARRAY_CONTAINS:
                case FunctionSet.ARRAY_APPEND:
                case FunctionSet.ARRAY_JOIN:
                case FunctionSet.ARRAY_POSITION:
                case FunctionSet.ARRAY_REMOVE:
                case FunctionSet.ARRAY_SLICE: {
                    if (node.getChildren().isEmpty()) {
                        throw new SemanticException(fnName + " should have at least one input.", node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException("The first input of " + fnName +
                                " should be an array", node.getPos());
                    }
                    break;
                }
            }
        }

        private Function getStrToDateFunction(FunctionCallExpr node, Type[] argumentTypes) {
            /*
             * @TODO: Determine the return type of this function
             * If is format is constant and don't contains time part, return date type, to compatible with mysql.
             * In fact we don't want to support str_to_date return date like mysql, reason:
             * 1. The return type of FE/BE str_to_date function signature is datetime, return date
             *    let type different, it's will throw unpredictable error
             * 2. Support return date and datetime at same time in one function is complicated.
             * 3. The meaning of the function is confusing. In mysql, will return date if format is a constant
             *    string and it's not contains "%H/%M/%S" pattern, but it's a trick logic, if format is a variable
             *    expression, like: str_to_date(col1, col2), and the col2 is '%Y%m%d', the result always be
             *    datetime.
             */
            Function fn = Expr.getBuiltinFunction(node.getFnName().getFunction(),
                    argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                return null;
            }

            if (!node.getChild(1).isConstant()) {
                return fn;
            }

            ExpressionMapping expressionMapping =
                    new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                            Lists.newArrayList());

            ScalarOperator format = SqlToScalarOperatorTranslator.translate(node.getChild(1), expressionMapping,
                    new ColumnRefFactory());
            if (format.isConstantRef() && !HAS_TIME_PART.matcher(format.toString()).matches()) {
                return Expr.getBuiltinFunction("str2date", argumentTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }

            return fn;
        }

        private Function getArrayGenerateFunction(FunctionCallExpr node) {
            // add the default parameters for array_generate
            if (node.getChildren().size() == 1) {
                LiteralExpr secondParam = (LiteralExpr) node.getChild(0);
                node.clearChildren();
                node.addChild(new IntLiteral(1));
                node.addChild(secondParam);
            }
            if (node.getChildren().size() == 2) {
                int idx = 0;
                BigInteger[] childValues = new BigInteger[2];
                Boolean hasNUll = false;
                for (Expr expr : node.getChildren()) {
                    if (expr instanceof NullLiteral) {
                        hasNUll = true;
                    } else if (expr instanceof IntLiteral) {
                        childValues[idx++] = BigInteger.valueOf(((IntLiteral) expr).getValue());
                    } else {
                        childValues[idx++] = ((LargeIntLiteral) expr).getValue();
                    }
                }

                if (hasNUll || childValues[0].compareTo(childValues[1]) < 0) {
                    node.addChild(new IntLiteral(1));
                } else {
                    node.addChild(new IntLiteral(-1));
                }
            }
            Type[] argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
            return Expr.getBuiltinFunction(FunctionSet.ARRAY_GENERATE, argumentTypes,
                    Function.CompareMode.IS_SUPERTYPE_OF);
        }

        @Override
        public Void visitGroupingFunctionCall(GroupingFunctionCallExpr node, Scope scope) {
            if (node.getChildren().size() < 1) {
                throw new SemanticException("GROUPING functions required at least one parameters", node.getPos());
            }
            if (node.getChildren().stream().anyMatch(e -> !(e instanceof SlotRef))) {
                throw new SemanticException("grouping functions only support column", node.getPos());
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

            // check when type
            List<Type> whenTypes = Lists.newArrayList();

            if (null != caseExpr) {
                whenTypes.add(caseExpr.getType());
            }

            for (int i = start; i < end; i = i + 2) {
                whenTypes.add(node.getChild(i).getType());
            }

            Type compatibleType = Type.BOOLEAN;
            if (null != caseExpr) {
                compatibleType = TypeManager.getCompatibleTypeForCaseWhen(whenTypes);
            }

            for (Type type : whenTypes) {
                if (!Type.canCastTo(type, compatibleType)) {
                    throw new SemanticException("Invalid when type cast " + type.toSql()
                            + " to " + compatibleType.toSql(), node.getPos());
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
                            + " to " + returnType.toSql(), node.getPos());
                }
            }

            node.setType(returnType);
            return null;
        }

        @Override
        public Void visitSubquery(Subquery node, Scope context) {
            QueryAnalyzer queryAnalyzer = new QueryAnalyzer(session);
            queryAnalyzer.analyze(node.getQueryStatement(), context);
            node.setType(node.getQueryStatement().getQueryRelation().getRelationFields().getFieldByIndex(0).getType());
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
        public Void visitInformationFunction(InformationFunction node, Scope context) {
            String funcType = node.getFuncType();
            if (funcType.equalsIgnoreCase("DATABASE") || funcType.equalsIgnoreCase("SCHEMA")) {
                node.setType(Type.VARCHAR);
                node.setStrValue(ClusterNamespace.getNameFromFullName(session.getDatabase()));
            } else if (funcType.equalsIgnoreCase("USER")) {
                node.setType(Type.VARCHAR);

                String user = session.getQualifiedUser();
                String remoteIP = session.getRemoteIP();

                node.setStrValue(new UserIdentity(user, remoteIP).toString());
            } else if (funcType.equalsIgnoreCase("CURRENT_USER")) {
                node.setType(Type.VARCHAR);
                node.setStrValue(session.getCurrentUserIdentity().toString());
            } else if (funcType.equalsIgnoreCase("CURRENT_ROLE")) {
                node.setType(Type.VARCHAR);
                AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                List<String> roleName = new ArrayList<>();

                try {
                    for (Long roleId : session.getCurrentRoleIds()) {
                        RolePrivilegeCollectionV2 rolePrivilegeCollection =
                                manager.getRolePrivilegeCollectionUnlocked(roleId, false);
                        if (rolePrivilegeCollection != null) {
                            roleName.add(rolePrivilegeCollection.getName());
                        }
                    }
                } catch (PrivilegeException e) {
                    throw new SemanticException(e.getMessage());
                }

                if (roleName.isEmpty()) {
                    node.setStrValue("NONE");
                } else {
                    node.setStrValue(Joiner.on(", ").join(roleName));
                }
            } else if (funcType.equalsIgnoreCase("CONNECTION_ID")) {
                node.setType(Type.BIGINT);
                node.setIntValue(session.getConnectionId());
                node.setStrValue("");
            } else if (funcType.equalsIgnoreCase("CATALOG")) {
                node.setType(Type.VARCHAR);
                node.setStrValue(session.getCurrentCatalog());
            }
            return null;
        }

        @Override
        public Void visitVariableExpr(VariableExpr node, Scope context) {
            try {
                if (node.getSetType().equals(SetType.USER)) {
                    UserVariable userVariable = session.getUserVariables(node.getName());
                    // If referring to an uninitialized variable, its value is NULL and a string type.
                    if (userVariable == null) {
                        node.setType(Type.STRING);
                        node.setIsNull();
                        return null;
                    }

                    Type variableType = userVariable.getEvaluatedExpression().getType();
                    node.setType(variableType);

                    if (userVariable.getEvaluatedExpression() instanceof NullLiteral) {
                        node.setIsNull();
                    } else {
                        node.setValue(userVariable.getEvaluatedExpression().getRealObjectValue());
                    }
                } else {
                    VariableMgr.fillValue(session.getSessionVariable(), node);
                    if (!Strings.isNullOrEmpty(node.getName()) &&
                            node.getName().equalsIgnoreCase(SessionVariable.SQL_MODE)) {
                        node.setType(Type.VARCHAR);
                        node.setValue(SqlModeHelper.decode((long) node.getValue()));
                    }
                }
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
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

    static class IgnoreSlotVisitor extends Visitor {
        public IgnoreSlotVisitor(AnalyzeState analyzeState, ConnectContext session) {
            super(analyzeState, session);
        }

        @Override
        public Void visitSlot(SlotRef node, Scope scope) {
            return null;
        }
    }

    public static void analyzeExpression(Expr expression, AnalyzeState state, Scope scope, ConnectContext session) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(session);
        expressionAnalyzer.analyze(expression, state, scope);
    }

    public static void analyzeExpressionIgnoreSlot(Expr expression, ConnectContext session) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(session);
        expressionAnalyzer.analyzeIgnoreSlot(expression, new AnalyzeState(),
                new Scope(RelationId.anonymous(), new RelationFields()));
    }

}

