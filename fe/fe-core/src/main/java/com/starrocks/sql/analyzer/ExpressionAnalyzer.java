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
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DictQueryExpr;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MatchExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NamedArgument;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.PlaceHolderExpr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.UserVariableExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.DictionaryGetExpr;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.thrift.TFunctionBinaryType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyticAnalyzer.verifyAnalyticExpression;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class ExpressionAnalyzer {
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

    public void analyzeWithVisitor(Expr expression, AnalyzeState analyzeState, Scope scope, Visitor visitor) {
        bottomUpAnalyze(visitor, expression, scope);
    }

    public void analyzeWithoutUpdateState(Expr expression, AnalyzeState analyzeState, Scope scope) {
        Visitor visitor = new Visitor(analyzeState, session) {
            @Override
            protected void handleResolvedField(SlotRef slot, ResolvedField resolvedField) {
                // do not put the slotRef in analyzeState
            }
        };
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
                    "as it is not a supported high-order function");
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
                    throw new SemanticException(i + "-th lambda input ( " + expr + " ) should be arrays, " +
                            "but real type is " + expr.getType().toSql());
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
                    throw new SemanticException("Map lambda function (" +
                            expression.getChild(0).toSql() + ") should be like (k,v) -> (f(k),f(v))",
                            expression.getChild(0).getPos());
                }
            } else {
                if (expression.getChild(0).getChild(0) instanceof MapExpr) {
                    throw new SemanticException("Map lambda function (" +
                            expression.getChild(0).toSql() + ") should be like (k,v) -> f(k,v)",
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
                throw new SemanticException("Lambda input ( " + expr.toSql() + " ) should be a map, but real type is "
                        + expr.getType().toSql());
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
        boolean hasLambdaFunc = false;
        try {
            hasLambdaFunc = expression.hasLambdaFunction(expression);
        } catch (SemanticException e) {
            throw e.appendOnlyOnceMsg(expression.toSql(), expression.getPos());
        }
        if (hasLambdaFunc) {
            String originalSQL = expression.toSql();
            try {
                analyzeHighOrderFunction(visitor, expression, scope);
                visitor.visit(expression, scope);
            } catch (SemanticException e) {
                throw e.appendOnlyOnceMsg(originalSQL, expression.getPos());
            }
        } else {
            for (Expr expr : expression.getChildren()) {
                bottomUpAnalyze(visitor, expr, scope);
            }
            visitor.visit(expression, scope);
        }
    }

    public static class Visitor implements AstVisitor<Void, Scope> {
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

        protected void handleResolvedField(SlotRef slot, ResolvedField resolvedField) {
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
                node.setColumnName(resolvedField.getField().getName());
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
                    if (!keyType.isValidMapKeyType()) {
                        throw new SemanticException("Map key don't supported type: " + keyType, node.getPos());
                    }
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
            Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(list, true);

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

            Type compatibleType =
                    TypeManager.getCompatibleTypeForBinary(!node.getOp().isNotRangeComparison(), type1, type2);
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
            if (!queryExpressions.isEmpty() && node.getChildren().size() > 2) {
                throw new SemanticException("In Predicate only support literal expression list", node.getPos());
            }

            // check compatible type
            List<Type> list = node.getChildren().stream().map(Expr::getType).collect(Collectors.toList());
            Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(list, false);

            if (compatibleType == Type.INVALID) {
                throw new SemanticException("The input types (" + list.stream().map(Type::toSql).collect(
                        Collectors.joining(",")) + ") of in predict are not compatible", node.getPos());
            }

            for (Expr child : node.getChildren()) {
                Type type = child.getType();
                if (type.isJsonType() && !queryExpressions.isEmpty()) { // TODO: enable it after support join on JSON
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

        @Override
        public Void visitMatchExpr(MatchExpr node, Scope scope) {
            Type type1 = node.getChild(0).getType();
            Type type2 = node.getChild(1).getType();

            if (!type1.isStringType() && !type1.isNull()) {
                throw new SemanticException("left operand of MATCH must be of type STRING with NOT NULL");
            }

            if (!(node.getChild(0) instanceof SlotRef)) {
                throw new SemanticException("left operand of MATCH must be column ref");
            }

            if (!(node.getChild(1) instanceof StringLiteral) || type2.isNull()) {
                throw new SemanticException("right operand of MATCH must be of type StringLiteral with NOT NULL");
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
            if (node.isNondeterministicBuiltinFnName()) {
                ExprId exprId = analyzeState.getNextNondeterministicId();
                node.setNondeterministicId(exprId);
            }
            Type[] argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
            String fnName = node.getFnName().getFunction();
            // check fn & throw exception direct if analyze failed
            checkFunction(fnName, node, argumentTypes);
            // get function by function expression and argument types
            Function fn = FunctionAnalyzer.getAnalyzedFunction(session, node, argumentTypes);
            if (fn == null) {
                String msg = String.format("No matching function with signature: %s(%s)",
                        fnName,
                        node.getParams().isStar() ? "*" : Joiner.on(", ")
                                .join(Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.toList())));
                throw new SemanticException(msg, node.getPos());
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
                        throw new SemanticException(fnName + " should have 2 array inputs or lambda functions, " +
                                "but really have " + node.getChildren().size() + " inputs",
                                node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException(fnName + "'s first input " + node.getChild(0).toSql() +
                                " should be an array or a lambda function, but real type is " +
                                node.getChild(0).getType().toSql(), node.getPos());
                    }
                    if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                        throw new SemanticException(fnName + "'s second input " + node.getChild(1).toSql() +
                                " should be an array or a lambda function, but real type is " +
                                node.getChild(1).getType().toSql(), node.getPos());
                    }
                    // force the second array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(1).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException(fnName + "'s second input " + node.getChild(1).toSql() +
                                " can't cast from " + node.getChild(1).getType().toSql() + " to ARRAY<BOOL>",
                                node.getPos());
                    }
                    break;
                case FunctionSet.ALL_MATCH:
                case FunctionSet.ANY_MATCH:
                    if (node.getChildren().size() != 1) {
                        throw new SemanticException(fnName + " should have a input array", node.getPos());
                    }
                    if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException(fnName + "'s input " + node.getChild(0).toSql() + " should be " +
                                "an array, but real type is " + node.getChild(0).getType().toSql(), node.getPos());
                    }
                    // force the input array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(0).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException(fnName + "'s input " +
                                node.getChild(0).toSql() + " can't cast from " +
                                node.getChild(0).getType().toSql() + " to ARRAY<BOOL>", node.getPos());
                    }
                    break;
                case FunctionSet.ARRAY_SORTBY:
                    int nodeChildrenSize = node.getChildren().size();
                    if (nodeChildrenSize < 2) {
                        throw new SemanticException(
                                fnName + " should have at least 2 inputs inputs or lambda functions, " +
                                        "but really have " + node.getChildren().size() + " inputs",
                                node.getPos());
                    }
                    if (nodeChildrenSize == 2) {
                        if (!node.getChild(0).getType().isArrayType() && !node.getChild(0).getType().isNull()) {
                            throw new SemanticException(fnName + "'s first input " + node.getChild(0).toSql() +
                                    " should be an array or a lambda function, but real type is " +
                                    node.getChild(0).getType().toSql(), node.getPos());
                        }
                        if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                            throw new SemanticException(fnName + "'s second input " + node.getChild(1).toSql() +
                                    " should be an array or a lambda function, but real type is " +
                                    node.getChild(1).getType().toSql(), node.getPos());
                        }
                    } else {
                        for (Expr expr : node.getChildren()) {
                            if (!expr.getType().isArrayType()) {
                                throw new SemanticException(
                                        "function args must be array, but real type is " + expr.getType().toSql(),
                                        node.getPos());
                            }
                            if (!(expr.getType().canOrderBy() || expr.getType().isJsonType())) {
                                throw new SemanticException(
                                        "function args must be can be order by orderable type or json type, but real type is " +
                                                expr.getType().toSql(),
                                        node.getPos());
                            }
                        }
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
                        if (!(expr.getType().isFixedPointType()) && !expr.getType().isNull()) {
                            throw new SemanticException(fnName + "'s parameter only support Integer");
                        }
                    }
                    break;
                case FunctionSet.MAP_FILTER:
                    if (node.getChildren().size() != 2) {
                        throw new SemanticException(fnName + " should have 2 inputs, " +
                                "but there are just " + node.getChildren().size() + " inputs");
                    }
                    if (!node.getChild(0).getType().isMapType() && !node.getChild(0).getType().isNull()) {
                        throw new SemanticException(fnName + "'s first input " + node.getChild(0).toSql() +
                                " should be a map or a lambda function, but real type is " +
                                node.getChild(0).getType().toSql());
                    }
                    if (!node.getChild(1).getType().isArrayType() && !node.getChild(1).getType().isNull()) {
                        throw new SemanticException(fnName + "'s second input " + node.getChild(1).toSql() +
                                " should be an array or a lambda function, but real type is " +
                                node.getChild(1).getType().toSql());
                    }
                    // force the second array be of Type.ARRAY_BOOLEAN
                    if (!Type.canCastTo(node.getChild(1).getType(), Type.ARRAY_BOOLEAN)) {
                        throw new SemanticException(fnName + "'s second input " + node.getChild(1).toSql() +
                                " can't cast from " + node.getChild(1).getType().toSql() + " to ARRAY<BOOL>");
                    }
                    break;
                case FunctionSet.GROUP_CONCAT:
                case FunctionSet.ARRAY_AGG: {
                    if (node.getChildren().size() == 0) {
                        throw new SemanticException(fnName + " should have at least one input", node.getPos());
                    }
                    int start = argumentTypes.length - node.getParams().getOrderByElemNum();
                    if (fnName.equals(FunctionSet.GROUP_CONCAT)) {
                        if (start < 2) {
                            throw new SemanticException(fnName + " should have output expressions before [ORDER BY]",
                                    node.getPos());
                        }
                        if (node.getParams().isDistinct() && !node.getChild(start - 1).isConstant()) {
                            throw new SemanticException(fnName + " distinct should use constant separator", node.getPos());
                        }

                    } else if (fnName.equals(FunctionSet.ARRAY_AGG) && start != 1) {
                        throw new SemanticException(fnName + " should have exact one output expressions before" +
                                " [ORDER BY]", node.getPos());
                    }
                    for (int i = start; i < argumentTypes.length; ++i) {
                        if (!argumentTypes[i].canOrderBy()) {
                            throw new SemanticException(fnName + " can't support order by the " + i +
                                    "-th input with type of " + argumentTypes[i].toSql(), node.getPos());
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
                case FunctionSet.ARRAY_CONTAINS_SEQ:
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

                case FunctionSet.IS_ROLE_IN_SESSION: {
                    if (node.getChildren().size() != 1) {
                        throw new SemanticException("IS_ROLE_IN_SESSION currently only supports a single parameter");
                    }

                    if (!(node.getChild(0) instanceof StringLiteral)) {
                        throw new SemanticException("IS_ROLE_IN_SESSION currently only supports constant parameters");
                    }
                    break;
                }
                case FunctionSet.ARRAY_FLATTEN: {
                    if (node.getChildren().size() != 1) {
                        throw new SemanticException(fnName + " should have only one input", node.getPos());
                    }
                    Type inputType = node.getChild(0).getType();
                    if (!inputType.isArrayType() && !inputType.isNull()) {
                        throw new SemanticException("The only one input of " + fnName +
                                " should be an array of arrays, rather than " + inputType.toSql(), node.getPos());
                    }
                    if (inputType.isArrayType() && !((ArrayType) inputType).getItemType().isArrayType()) {
                        throw new SemanticException("The only one input of " + fnName +
                                " should be an array of arrays, rather than " + inputType.toSql(), node.getPos());
                    }
                    break;
                }
                case FunctionSet.FIELD: {
                    if (node.getChildren().size() < 2) {
                        throw new SemanticException("Incorrect parameter count in" +
                                " the call to native function 'field'");
                    }
                    break;
                }
            }
        }

        @Override
        public Void visitGroupingFunctionCall(GroupingFunctionCallExpr node, Scope scope) {
            if (node.getChildren().size() < 1) {
                throw new SemanticException("GROUPING functions required at least one parameters", node.getPos());
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
        public Void visitSubqueryExpr(Subquery node, Scope context) {
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
            if (funcType.equalsIgnoreCase(FunctionSet.DATABASE) || funcType.equalsIgnoreCase(FunctionSet.SCHEMA)) {
                node.setType(Type.VARCHAR);
                node.setStrValue(ClusterNamespace.getNameFromFullName(session.getDatabase()));
            } else if (funcType.equalsIgnoreCase(FunctionSet.USER)
                    || funcType.equalsIgnoreCase(FunctionSet.SESSION_USER)) {
                node.setType(Type.VARCHAR);

                String user = session.getQualifiedUser();
                String remoteIP = session.getRemoteIP();

                node.setStrValue(new UserIdentity(user, remoteIP).toString());
            } else if (funcType.equalsIgnoreCase(FunctionSet.CURRENT_USER)) {
                node.setType(Type.VARCHAR);
                node.setStrValue(session.getCurrentUserIdentity().toString());
            } else if (funcType.equalsIgnoreCase(FunctionSet.CURRENT_ROLE)) {
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
            } else if (funcType.equalsIgnoreCase(FunctionSet.CURRENT_GROUP)) {
                node.setType(Type.VARCHAR);
                Set<String> groupName = session.getGroups();
                if (groupName.isEmpty()) {
                    node.setStrValue("NONE");
                } else {
                    node.setStrValue(Joiner.on(", ").join(groupName));
                }
            } else if (funcType.equalsIgnoreCase(FunctionSet.CONNECTION_ID)) {
                node.setType(Type.BIGINT);
                node.setIntValue(session.getConnectionId());
                node.setStrValue("");
            } else if (funcType.equalsIgnoreCase(FunctionSet.CATALOG)) {
                node.setType(Type.VARCHAR);
                node.setStrValue(session.getCurrentCatalog());
            } else if (funcType.equalsIgnoreCase(FunctionSet.SESSION_ID)) {
                node.setType(Type.VARCHAR);
                node.setStrValue(session.getSessionId().toString());
            }
            return null;
        }

        @Override
        public Void visitVariableExpr(VariableExpr node, Scope context) {
            try {
                GlobalStateMgr.getCurrentState().getVariableMgr().fillValue(session.getSessionVariable(), node);
                if (!Strings.isNullOrEmpty(node.getName()) &&
                        node.getName().equalsIgnoreCase(SessionVariable.SQL_MODE)) {
                    node.setType(Type.VARCHAR);
                    node.setValue(SqlModeHelper.decode((long) node.getValue()));
                } else if (!Strings.isNullOrEmpty(node.getName()) &&
                        node.getName().equalsIgnoreCase(SessionVariable.AUTO_COMMIT)) {
                    node.setType(Type.BIGINT);
                    node.setValue(((boolean) node.getValue()) ? (long) (1) : (long) 0);
                }
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitUserVariableExpr(UserVariableExpr node, Scope context) {
            UserVariable userVariable;
            if (session.getUserVariablesCopyInWrite() == null) {
                userVariable = session.getUserVariable(node.getName());
            } else {
                userVariable = session.getUserVariableCopyInWrite(node.getName());
            }

            if (userVariable == null) {
                node.setValue(NullLiteral.create(Type.STRING));
            } else {
                node.setValue(userVariable.getEvaluatedExpression());
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

        @Override
        public Void visitDictQueryExpr(DictQueryExpr node, Scope context) {
            if (RunMode.isSharedDataMode()) {
                throw new SemanticException("dict_mapping function do not support shared data mode");
            }
            List<Expr> params = node.getParams().exprs();
            if (!(params.get(0) instanceof StringLiteral)) {
                throw new SemanticException("dict_mapping function first param table_name should be string literal");
            }
            String[] dictTableFullName = ((StringLiteral) params.get(0)).getStringValue().split("\\.");
            TableName tableName;
            if (dictTableFullName.length == 1) {
                tableName = new TableName(null, dictTableFullName[0]);
                tableName.normalization(session);
            } else if (dictTableFullName.length == 2) {
                tableName = new TableName(dictTableFullName[0], dictTableFullName[1]);
            } else {
                throw new SemanticException("dict_mapping function first param table_name should be 'db.tbl' or 'tbl' format");
            }

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
            if (db == null) {
                throw new SemanticException("Database %s is not found", tableName.getDb());
            }
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName.getTbl());
            if (table == null) {
                throw new SemanticException("dict table %s is not found", tableName.getTbl());
            }
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("dict table type is not OlapTable, type=" + table.getClass());
            }
            if (table instanceof MaterializedView) {
                throw new SemanticException("dict table can't be materialized view");
            }
            OlapTable dictTable = (OlapTable) table;

            if (dictTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                throw new SemanticException("dict table " + tableName + " should be primary key table");
            }

            // verify keys length and type
            List<Column> keyColumns = new ArrayList<>();
            Column valueColumn = null;
            for (Column column : dictTable.getBaseSchema()) {
                if (column.isKey()) {
                    keyColumns.add(column);
                }
                if (column.isAutoIncrement()) {
                    valueColumn = column;
                }
            }
            // (table, keys..., value_column, null_if_not_found)
            int valueColumnIdx;
            int nullIfNotFoundIdx;
            if (params.size() == keyColumns.size() + 1) {
                valueColumnIdx = -1;
                nullIfNotFoundIdx = -1;
            } else if (params.size() == keyColumns.size() + 2) {
                if (params.get(params.size() - 1).getType().getPrimitiveType().isStringType()) {
                    valueColumnIdx = params.size() - 1;
                    nullIfNotFoundIdx = -1;
                } else {
                    nullIfNotFoundIdx = params.size() - 1;
                    valueColumnIdx = -1;
                }
            } else if (params.size() == keyColumns.size() + 3) {
                valueColumnIdx = params.size() - 2;
                nullIfNotFoundIdx = params.size() - 1;
            } else {
                throw new SemanticException(String.format("dict_mapping function param size should be %d - %d",
                        keyColumns.size() + 1, keyColumns.size() + 3));
            }

            String valueField;
            if (valueColumnIdx >= 0) {
                Expr valueFieldExpr = params.get(valueColumnIdx);
                if (!(valueFieldExpr instanceof StringLiteral)) {
                    throw new SemanticException("dict_mapping function value_column param should be STRING constant");
                }
                valueField = ((StringLiteral) valueFieldExpr).getStringValue();
                valueColumn = dictTable.getBaseColumn(valueField);
                if (valueColumn == null) {
                    throw new SemanticException("dict_mapping function can't find value column " + valueField + " in dict table");
                }
            } else {
                if (valueColumn == null) {
                    throw new SemanticException("dict_mapping function can't find auto increment column in dict table");
                }
                valueField = valueColumn.getName();
            }

            boolean nullIfNotFound = false;
            if (nullIfNotFoundIdx >= 0) {
                Expr nullIfNotFoundExpr = params.get(nullIfNotFoundIdx);
                if (!(nullIfNotFoundExpr instanceof BoolLiteral)) {
                    throw new SemanticException("dict_mapping function null_if_not_found param should be bool constant");
                }
                nullIfNotFound = ((BoolLiteral) nullIfNotFoundExpr).getValue();
            }

            List<Type> expectTypes = new ArrayList<>();
            expectTypes.add(Type.VARCHAR);
            for (Column keyColumn : keyColumns) {
                expectTypes.add(ScalarType.createType(keyColumn.getType().getPrimitiveType()));
            }
            if (valueColumnIdx >= 0) {
                expectTypes.add(Type.VARCHAR);
            }
            if (nullIfNotFoundIdx >= 0) {
                expectTypes.add(Type.BOOLEAN);
            }

            List<Type> actualTypes = node.getChildren().stream()
                    .map(expr -> ScalarType.createType(expr.getType().getPrimitiveType())).collect(Collectors.toList());
            if (!Objects.equals(expectTypes, actualTypes)) {
                List<String> expectTypeNames = new ArrayList<>();
                expectTypeNames.add("VARCHAR dict_table");
                for (int i = 0; i < keyColumns.size(); i++) {
                    expectTypeNames.add(expectTypes.get(i + 1).canonicalName() + " " + keyColumns.get(i).getName());
                }
                if (valueColumnIdx >= 0) {
                    expectTypeNames.add("VARCHAR value_field_name");
                }
                if (nullIfNotFoundIdx >= 0) {
                    expectTypeNames.add("BOOLEAN null_if_not_found");
                }

                for (int i = 0; i < node.getChildren().size(); ++i) {
                    Expr actual = node.getChildren().get(i);
                    Type expectedType = expectTypes.get(i);
                    if (!Type.canCastTo(actual.getType(), expectedType)) {
                        List<String> actualTypeNames = actualTypes.stream().map(Type::canonicalName).collect(Collectors.toList());
                        throw new SemanticException(
                                String.format("dict_mapping function params not match expected,\nExpect: %s\nActual: %s",
                                        String.join(", ", expectTypeNames), String.join(", ", actualTypeNames)));
                    }

                    Expr castExpr = new CastExpr(expectedType, actual);
                    node.getChildren().set(i, castExpr);
                }
            }

            Type valueType = ScalarType.createType(valueColumn.getType().getPrimitiveType());

            final TDictQueryExpr dictQueryExpr = new TDictQueryExpr();
            dictQueryExpr.setDb_name(tableName.getDb());
            dictQueryExpr.setTbl_name(tableName.getTbl());

            Map<Long, Long> partitionVersion = new HashMap<>();
            dictTable.getAllPhysicalPartitions().forEach(p -> partitionVersion.put(p.getId(), p.getVisibleVersion()));
            dictQueryExpr.setPartition_version(partitionVersion);

            List<String> keyFields = keyColumns.stream().map(Column::getName).collect(Collectors.toList());
            dictQueryExpr.setKey_fields(keyFields);
            dictQueryExpr.setValue_field(valueField);
            // For compatibility reason, we do not change the "strict_mode" variable in TDictQueryExpr
            dictQueryExpr.setStrict_mode(!nullIfNotFound);
            node.setType(valueType);

            Function fn = new Function(FunctionName.createFnName(FunctionSet.DICT_MAPPING), actualTypes, valueType, false);
            fn.setBinaryType(TFunctionBinaryType.BUILTIN);
            node.setFn(fn);

            node.setDictQueryExpr(dictQueryExpr);
            return null;
        }

        @Override
        public Void visitParameterExpr(Parameter node, Scope context) {
            return null;
        }

        @Override
        public Void visitDictionaryGetExpr(DictionaryGetExpr node, Scope context) {
            List<Expr> params = node.getChildren();
            if (params.size() < 2) {
                throw new SemanticException("dictionary_get function get illegal param list");
            }

            if (!(params.get(0) instanceof StringLiteral)) {
                throw new SemanticException("dictionary_get function first param dictionary_name should be string literal");
            }

            String dictionaryName = ((StringLiteral) params.get(0)).getValue();
            if (!GlobalStateMgr.getCurrentState().getDictionaryMgr().isExist(dictionaryName)) {
                throw new SemanticException("dictionary: " + dictionaryName + " does not exist");
            }
            Dictionary dictionary = GlobalStateMgr.getCurrentState().getDictionaryMgr().getDictionaryByName(dictionaryName);

            if (!node.getSkipStateCheck()) {
                if (dictionary.getState() == Dictionary.DictionaryState.CANCELLED) {
                    throw new SemanticException("dictionary: " + dictionaryName + " is in CANCELLED state");
                }

                if (dictionary.getState() == Dictionary.DictionaryState.UNINITIALIZED) {
                    throw new SemanticException("dictionary: " + dictionaryName + " is in UNINITIALIZED state");
                }

                if (dictionary.getReadLatest() && dictionary.getState() == Dictionary.DictionaryState.REFRESHING) {
                    throw new SemanticException("dictionary_read_latest is ON, dictionary: " +
                            dictionaryName + " is in REFRESHING state");
                }
            }

            List<String> dictionaryKeys = dictionary.getKeys();
            int dictionaryKeysSize = dictionaryKeys.size();
            int paramDictionaryKeysSize = params.size() - 1;
            if (!(paramDictionaryKeysSize == dictionaryKeysSize || paramDictionaryKeysSize == dictionaryKeysSize + 1)) {
                throw new SemanticException("dictionary: " + dictionaryName + " has expected keys size: " +
                        Integer.toString(dictionaryKeysSize) + " keys: " +
                        "[" + String.join(", ", dictionaryKeys) + "]" +
                        " plus null_if_not_exist flag(optional)" +
                        " but param given: " + Integer.toString(paramDictionaryKeysSize));
            }

            if (paramDictionaryKeysSize == dictionaryKeysSize + 1 && !(params.get(params.size() - 1) instanceof BoolLiteral)) {
                throw new SemanticException("dictionary: " + dictionaryName + " has invalid parameter for `null_if_not_exist` "
                        + "invalid parameter: " + params.get(params.size() - 1).toString());
            }

            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                    dictionary.getCatalogName(), dictionary.getDbName(), dictionary.getQueryableObject());
            if (table == null) {
                throw new SemanticException("dict table %s is not found", table.getName());
            }

            List<Column> schema = table.getFullSchema();
            List<Type> paramType = new ArrayList<>();
            List<Column> keysColumn = new ArrayList<>();
            paramType.add(Type.VARCHAR);
            for (String key : dictionaryKeys) {
                for (int i = 0; i < schema.size(); ++i) {
                    if (key.equals(schema.get(i).getName())) {
                        keysColumn.add(schema.get(i));
                        paramType.add(schema.get(i).getType());
                    }
                }
            }

            List<String> dictionaryValues = dictionary.getValues();
            List<Column> valuesColumn = new ArrayList<>();
            for (String value : dictionaryValues) {
                for (int i = 0; i < schema.size(); ++i) {
                    if (value.equals(schema.get(i).getName())) {
                        valuesColumn.add(schema.get(i));
                    }
                }
            }

            for (int i = 0; i < keysColumn.size(); ++i) {
                Expr parmExpr = params.get(i + 1);
                Column column = keysColumn.get(i);
                if (!column.getType().equals(parmExpr.getType())) {
                    if (!Type.canCastTo(column.getType(), parmExpr.getType())) {
                        throw new SemanticException("column type " + column.getType().toSql()
                                + " cast from " + parmExpr.getType().toSql() + " is invalid.");
                    } else {
                        parmExpr = new CastExpr(column.getType(), parmExpr);
                        params.set(i + 1, parmExpr);
                        node.getChildren().set(i + 1, parmExpr);
                    }
                }
            }

            boolean nullIfNotExist = (paramDictionaryKeysSize == dictionaryKeysSize + 1) ?
                    ((BoolLiteral) params.get(params.size() - 1)).getValue() : false;
            node.setNullIfNotExist(nullIfNotExist);
            node.setDictionaryId(dictionary.getDictionaryId());
            node.setDictionaryTxnId(GlobalStateMgr.getCurrentState().getDictionaryMgr().
                    getLastSuccessTxnId(dictionary.getDictionaryId()));
            node.setKeySize(dictionary.getKeys().size());

            ArrayList<StructField> structFields = new ArrayList<>(valuesColumn.size());
            for (Column column : valuesColumn) {
                String fieldName = column.getName();
                Type fieldType = column.getType();
                structFields.add(new StructField(fieldName, fieldType));
            }
            StructType returnType = new StructType(structFields);
            node.setType(returnType);
            return null;
        }

        @Override
        public Void visitNamedArgument(NamedArgument node, Scope context) {
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

    static class ResolveSlotVisitor extends Visitor {

        private java.util.function.Consumer<SlotRef> resolver;

        public ResolveSlotVisitor(AnalyzeState state, ConnectContext session,
                                  java.util.function.Consumer<SlotRef> slotResolver) {
            super(state, session);
            resolver = slotResolver;
        }

        @Override
        public Void visitSlot(SlotRef node, Scope scope) {
            resolver.accept(node);
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

    public static void analyzeExpressionResolveSlot(Expr expression, ConnectContext session,
                                                    Consumer<SlotRef> slotRefConsumer) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(session);
        AnalyzeState state = new AnalyzeState();
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields());

        ResolveSlotVisitor visitor = new ResolveSlotVisitor(new AnalyzeState(), ConnectContext.get(), slotRefConsumer);
        expressionAnalyzer.analyzeWithVisitor(expression, state, scope, visitor);
    }
}

