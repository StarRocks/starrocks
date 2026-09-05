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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.HashSet;
import java.util.Set;

final class AIFunctionAnalyzer {
    private static final Set<String> AI_RESERVED_OPTION_KEYS = Set.of("model", "messages", "stream");

    private AIFunctionAnalyzer() {
    }

    static void analyze(FunctionCallExpr node) {
        if (!FunctionSet.AI_COMPLETE.equals(node.getFunctionName())) {
            throw new SemanticException("Unsupported AI function '" + node.getFunctionName() + "'", node.getPos());
        }

        int argumentCount = node.getChildren().size();
        boolean hasOptionMap = argumentCount > 1 && node.getChild(argumentCount - 1).getType().isMapType();
        boolean hasExplicitModel = argumentCount - (hasOptionMap ? 1 : 0) == 2;

        try {
            AIModelConfigs.validateSystemChat(hasExplicitModel
                    ? AIModelConfigs.DefaultModelRequirement.OPTIONAL
                    : AIModelConfigs.DefaultModelRequirement.REQUIRED);
        } catch (StarRocksPlannerException e) {
            throw new SemanticException(e.getMessage(), node.getPos());
        }

        if (hasExplicitModel) {
            validateExplicitModel(node.getChild(0), node);
        }
        if (hasOptionMap) {
            validateAIOptionMap(node.getChild(argumentCount - 1), node);
        }
    }

    private static void validateExplicitModel(Expr model, FunctionCallExpr node) {
        if (!model.isConstant()) {
            return;
        }
        Expr folded = foldAIConstant(model);
        folded = unwrapCasts(folded);
        if (folded instanceof StringLiteral && ((StringLiteral) folded).getValue().trim().isEmpty()) {
            throw new SemanticException("ai_complete explicit model must not be empty", node.getPos());
        }
    }

    private static void validateAIOptionMap(Expr optionMap, FunctionCallExpr node) {
        if (!optionMap.isConstant()) {
            throw new SemanticException("ai_complete requires a constant option MAP", node.getPos());
        }
        Expr userOptionMap = optionMap;
        while (userOptionMap instanceof CastExpr && ((CastExpr) userOptionMap).isImplicit()) {
            userOptionMap = userOptionMap.getChild(0);
        }
        MapType optionMapType = (MapType) userOptionMap.getType();
        if (isUntypedEmptyMap(userOptionMap) || isUntypedMapType(optionMapType)) {
            return;
        }
        if (!optionMapType.getKeyType().isVarchar()) {
            if (validateMapConcatExpression(userOptionMap, optionMapType,
                    !hasExplicitCast(userOptionMap), node)) {
                return;
            }
            throw new SemanticException("ai_complete option MAP must have VARCHAR keys", node.getPos());
        }

        Expr originalValue = unwrapCasts(userOptionMap);
        Expr folded = originalValue instanceof MapExpr || originalValue instanceof NullLiteral
                ? originalValue : unwrapCasts(foldAIConstant(userOptionMap));
        if (folded instanceof NullLiteral) {
            validateJSONCompatibleType(optionMapType.getValueType(), "option MAP");
            return;
        }
        if (!(folded instanceof MapExpr)) {
            if (validateMapConcatExpression(userOptionMap, optionMapType,
                    !hasExplicitCast(userOptionMap), node)) {
                return;
            }
            validateJSONCompatibleType(optionMapType.getValueType(), "option MAP");
            // BE revalidates value-dependent key and finite-number constraints before serializing the request.
            // FE still enforces constant input and the complete recursive type contract, and performs stronger
            // checks whenever constant folding exposes a literal MapExpr.
            return;
        }

        MapExpr map = (MapExpr) folded;
        if (map.getChildren().isEmpty()) {
            validateJSONCompatibleType(optionMapType.getValueType(), "option MAP");
            return;
        }
        validateMapLiteral(map, optionMapType, true, userOptionMap instanceof MapExpr, node);
    }

    private static void validateMapLiteral(MapExpr map, MapType mapType, boolean topLevel,
                                           boolean allowUntypedPlaceholders, FunctionCallExpr node) {
        if (!mapType.getKeyType().isVarchar()) {
            String message = topLevel ? "ai_complete option MAP must have VARCHAR keys"
                    : "ai_complete nested MAP keys must be VARCHAR";
            throw new SemanticException(message, node.getPos());
        }

        Set<String> keys = new HashSet<>();
        for (int i = 0; i < map.getChildren().size(); i += 2) {
            Expr keyExpression = unwrapCasts(foldAIConstantAsType(map.getChild(i), mapType.getKeyType()));
            if (keyExpression instanceof NullLiteral) {
                throw new SemanticException("ai_complete option MAP contains a NULL option key", node.getPos());
            }
            if (!(keyExpression instanceof StringLiteral)) {
                if (!keyExpression.isConstant() || !keyExpression.getType().isVarchar()) {
                    throw new SemanticException("ai_complete option MAP keys must be constant VARCHAR expressions",
                            node.getPos());
                }
                validateOptionValueExpression(map.getChild(i + 1), mapType.getValueType(),
                        allowUntypedPlaceholders, node);
                continue;
            }

            String key = ((StringLiteral) keyExpression).getValue();
            if (key.isEmpty()) {
                throw new SemanticException("ai_complete option MAP contains an empty option key", node.getPos());
            }
            if (!keys.add(key)) {
                throw new SemanticException("ai_complete option MAP contains duplicate option key '" + key + "'",
                        node.getPos());
            }
            if (topLevel && AI_RESERVED_OPTION_KEYS.contains(key)) {
                throw new SemanticException("ai_complete option MAP contains reserved option key '" + key + "'",
                        node.getPos());
            }

            validateOptionValueExpression(map.getChild(i + 1), mapType.getValueType(),
                    allowUntypedPlaceholders, node);
        }
    }

    private static void validateOptionValueExpression(Expr expression, Type effectiveType,
                                                      boolean allowUntypedPlaceholders, FunctionCallExpr node) {
        boolean allowNestedPlaceholders = allowUntypedPlaceholders && !hasExplicitCast(expression);
        Expr structuralValue = unwrapImplicitCasts(expression);
        Expr value = unwrapCasts(expression);
        if (allowNestedPlaceholders && isUntypedEmptyMap(structuralValue)) {
            return;
        }
        if (allowNestedPlaceholders && structuralValue instanceof NullLiteral) {
            return;
        }
        if (value instanceof NullLiteral) {
            validateJSONCompatibleType(effectiveType, "option MAP");
            return;
        }
        if (value instanceof MapExpr) {
            if (value.getChildren().isEmpty()) {
                validateJSONCompatibleType(effectiveType, "option MAP");
                return;
            }
            if (effectiveType.isMapType()) {
                validateMapLiteral((MapExpr) value, (MapType) effectiveType, false,
                        allowNestedPlaceholders, node);
                return;
            }
        }
        if (value instanceof ArrayExpr && effectiveType.isArrayType()) {
            Type itemType = ((ArrayType) effectiveType).getItemType();
            if (value.getChildren().isEmpty()) {
                validateJSONCompatibleType(effectiveType, "option MAP");
                return;
            }
            for (Expr child : value.getChildren()) {
                validateOptionValueExpression(child, itemType, allowNestedPlaceholders, node);
            }
            return;
        }
        if (effectiveType.isMapType() && validateMapConcatExpression(expression, (MapType) effectiveType,
                allowNestedPlaceholders, node)) {
            return;
        }
        if (value instanceof FunctionCallExpr && effectiveType.isStructType()) {
            String functionName = ((FunctionCallExpr) value).getFunctionName();
            StructType structType = (StructType) effectiveType;
            int valueOffset;
            int fieldStride;
            if (FunctionSet.ROW.equals(functionName)) {
                valueOffset = 0;
                fieldStride = 1;
            } else if (FunctionSet.NAMED_STRUCT.equals(functionName)) {
                valueOffset = 1;
                fieldStride = 2;
            } else {
                valueOffset = -1;
                fieldStride = 0;
            }
            if (valueOffset >= 0) {
                Preconditions.checkState(value.getChildren().size() == structType.getFields().size() * fieldStride);
                for (int i = 0; i < structType.getFields().size(); i++) {
                    Type fieldType = structType.getField(i).getType();
                    Expr fieldValue = value.getChild(valueOffset + i * fieldStride);
                    validateOptionValueExpression(fieldValue, fieldType, allowNestedPlaceholders, node);
                }
                return;
            }
        }
        validateJSONCompatibleType(effectiveType, "option MAP");
        if (effectiveType.isFloatingPointType()) {
            validateFiniteAIConstant(expression, effectiveType, node);
        }
    }

    private static boolean validateMapConcatExpression(Expr expression, MapType effectiveType,
                                                       boolean allowUntypedPlaceholders, FunctionCallExpr node) {
        Expr value = unwrapCasts(expression);
        if (!(value instanceof FunctionCallExpr)
                || !FunctionSet.MAP_CONCAT.equals(((FunctionCallExpr) value).getFunctionName())) {
            return false;
        }
        for (Expr child : value.getChildren()) {
            validateOptionValueExpression(child, effectiveType, allowUntypedPlaceholders, node);
        }
        return true;
    }

    private static boolean isUntypedEmptyMap(Expr expression) {
        return expression instanceof MapExpr && expression.getChildren().isEmpty();
    }

    private static boolean hasExplicitCast(Expr expression) {
        Expr current = expression;
        while (current instanceof CastExpr) {
            if (!((CastExpr) current).isImplicit()) {
                return true;
            }
            current = current.getChild(0);
        }
        return false;
    }

    private static Expr unwrapImplicitCasts(Expr expression) {
        Expr current = expression;
        while (current instanceof CastExpr && ((CastExpr) current).isImplicit()) {
            current = current.getChild(0);
        }
        return current;
    }

    private static void validateJSONCompatibleType(Type type, String scope) {
        if (type.isNull() || type.isBoolean() || type.isNumericType() || type.isStringType() || type.isJsonType()) {
            return;
        }
        if (type.isArrayType()) {
            validateJSONCompatibleType(((ArrayType) type).getItemType(), scope);
            return;
        }
        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            if (isUntypedMapType(mapType)) {
                return;
            }
            if (!mapType.getKeyType().isVarchar()) {
                throw new SemanticException("ai_complete nested MAP keys must be VARCHAR");
            }
            validateJSONCompatibleType(mapType.getValueType(), scope);
            return;
        }
        if (type.isStructType()) {
            for (StructField field : ((StructType) type).getFields()) {
                validateJSONCompatibleType(field.getType(), scope);
            }
            return;
        }
        throw new SemanticException("ai_complete " + scope + " values must be JSON-compatible, but found "
                + type.toSql());
    }

    private static boolean isUntypedMapType(Type type) {
        if (!type.isMapType()) {
            return false;
        }
        MapType mapType = (MapType) type;
        return mapType.getKeyType().isNull() && mapType.getValueType().isNull();
    }

    private static Expr foldAIConstant(Expr expression) {
        return ExprUtils.analyzeAndCastFold(expression.clone());
    }

    private static Expr foldAIConstantAsType(Expr expression, Type targetType) {
        if (expression.getType().equals(targetType)) {
            if (targetType.isComplexType() || expression instanceof LiteralExpr) {
                return expression;
            }
        }
        Expr typedExpression = expression.getType().equals(targetType)
                ? expression : new CastExpr(targetType, expression);
        return foldAnalyzedAIConstant(typedExpression);
    }

    private static Expr foldAnalyzedAIConstant(Expr expression) {
        try {
            ScalarOperator scalarOperator = rewriteAnalyzedAIConstant(expression);
            return ScalarOperatorToExpr.buildExprIgnoreSlot(scalarOperator,
                    new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        } catch (UnsupportedException e) {
            return expression;
        }
    }

    private static void validateFiniteAIConstant(Expr expression, Type targetType, FunctionCallExpr node) {
        Expr typedExpression = expression.getType().equals(targetType)
                ? expression : new CastExpr(targetType, expression);
        try {
            ScalarOperator scalarOperator = rewriteAnalyzedAIConstant(typedExpression);
            if (scalarOperator instanceof ConstantOperator) {
                ConstantOperator constant = (ConstantOperator) scalarOperator;
                if (!constant.isNull() && !Double.isFinite(constant.getDouble())) {
                    throwNonFiniteAIOption(node);
                }
                return;
            }

            // Constant folding deliberately leaves invalid casts and overflowing floating-point calls
            // unchanged. Distinguish non-finite string spellings from ordinary failed casts, which
            // evaluate to NULL, and reject failed FE-evaluable floating-point calls conservatively.
            if (scalarOperator instanceof CastOperator && scalarOperator.getChild(0) instanceof ConstantOperator) {
                ConstantOperator child = (ConstantOperator) scalarOperator.getChild(0);
                if (!child.isNull() && child.getType().isStringType()) {
                    try {
                        String value = child.getType().isVarchar() ? child.getVarchar() : child.getChar();
                        if (!Double.isFinite(Double.parseDouble(value.trim()))) {
                            throwNonFiniteAIOption(node);
                        }
                    } catch (NumberFormatException ignored) {
                        // Invalid numeric text follows normal CAST semantics and evaluates to NULL.
                    }
                }
                return;
            }
            if (scalarOperator instanceof CallOperator && scalarOperator.getChildren().stream()
                    .allMatch(ScalarOperator::isConstantRef)
                    && ScalarOperatorEvaluator.INSTANCE.isFEConstantFunction((CallOperator) scalarOperator)
                    && isFunctionConstantFoldingEnabled()) {
                throwNonFiniteAIOption(node);
            }
        } catch (UnsupportedException ignored) {
            // BE validates values that FE cannot fold before serializing the HTTP request.
        }
    }

    private static boolean isFunctionConstantFoldingEnabled() {
        ConnectContext context = ConnectContext.get();
        return context == null || !context.getSessionVariable().isDisableFunctionFoldConstants();
    }

    private static ScalarOperator rewriteAnalyzedAIConstant(Expr expression) throws UnsupportedException {
        ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expression.clone());
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        return rewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
    }

    private static void throwNonFiniteAIOption(FunctionCallExpr node) {
        throw new SemanticException("ai_complete option MAP numeric values must be finite", node.getPos());
    }

    private static Expr unwrapCasts(Expr expression) {
        Expr current = expression;
        while (current instanceof CastExpr) {
            current = current.getChild(0);
        }
        return current;
    }

}
