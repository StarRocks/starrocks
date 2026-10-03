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

package com.starrocks.connector.index;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Recognizes connector-indexable scalar predicates and vector TopN expressions. */
public final class IndexAnalyzer {
    private static final Set<BinaryType> EQUALITY_TYPES = Set.of(BinaryType.EQ, BinaryType.NE);
    private static final Set<BinaryType> RANGE_TYPES =
            Set.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE);

    private final ConnectorIndexMetadata metadata;

    public IndexAnalyzer(ConnectorIndexMetadata metadata) {
        this.metadata = metadata;
    }

    /** Returns the supported conjuncts without removing them from the original scan predicate. */
    public ScalarOperator getIndexPredicate(ScalarOperator predicate) {
        IndexCondition condition = getIndexCondition(predicate);
        return condition == null ? null : condition.getPredicate();
    }

    /** Returns the supported conjuncts together with the concrete per-column index choices. */
    public IndexCondition getIndexCondition(ScalarOperator predicate) {
        if (predicate == null || metadata.isEmpty()) {
            return null;
        }
        List<ScalarOperator> supported = Utils.extractConjuncts(predicate).stream()
                .filter(this::supportsScalarPredicate)
                .collect(Collectors.toList());
        if (supported.isEmpty()) {
            return null;
        }

        Map<String, ConnectorIndexType> requiredIndexes = new LinkedHashMap<>();
        for (ScalarOperator conjunct : supported) {
            IndexUse use = getIndexUse(conjunct);
            if (use == null) {
                continue;
            }
            requiredIndexes.merge(use.columnName, use.indexType,
                    (left, right) -> left == ConnectorIndexType.RANGE || right == ConnectorIndexType.RANGE
                            ? ConnectorIndexType.RANGE : left);
        }
        return new IndexCondition(Utils.compoundAnd(supported), requiredIndexes);
    }

    /**
     * Returns whether an ORDER BY expression has a compatible vector index and direction.
     * L2 distance is ordered ascending; cosine similarity and inner product are ordered descending.
     */
    public boolean supportsVectorTopN(ScalarOperator scoreExpression, boolean ascending) {
        ScalarOperator unwrapped = unwrapFloatingPointCast(scoreExpression);
        if (!(unwrapped instanceof CallOperator)) {
            return false;
        }

        CallOperator call = (CallOperator) unwrapped;
        String function = call.getFnName();
        boolean supportedDirection;
        if (FunctionSet.APPROX_L2_DISTANCE.equalsIgnoreCase(function)) {
            supportedDirection = ascending;
        } else if (FunctionSet.APPROX_COSINE_SIMILARITY.equalsIgnoreCase(function)
                || FunctionSet.APPROX_INNER_PRODUCT.equalsIgnoreCase(function)) {
            supportedDirection = !ascending;
        } else {
            return false;
        }
        if (!supportedDirection || call.getChildren().size() != 2) {
            return false;
        }

        ScalarOperator first = call.getChild(0);
        ScalarOperator second = call.getChild(1);
        if (first instanceof ColumnRefOperator && isFloatArrayLiteral(second)) {
            return metadata.supports(((ColumnRefOperator) first).getName(), ConnectorIndexType.VECTOR);
        }
        return second instanceof ColumnRefOperator && isFloatArrayLiteral(first)
                && metadata.supports(((ColumnRefOperator) second).getName(), ConnectorIndexType.VECTOR);
    }

    private boolean supportsScalarPredicate(ScalarOperator predicate) {
        return getIndexUse(predicate) != null;
    }

    private IndexUse getIndexUse(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            return getBinaryIndexUse((BinaryPredicateOperator) predicate);
        }
        if (predicate instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) predicate;
            if (in.isSubquery() || !(in.getChild(0) instanceof ColumnRefOperator)
                    || !in.allValuesMatch(IndexAnalyzer::isNonNullScalarLiteral) || in.hasAnyNullValues()) {
                return null;
            }
            ColumnRefOperator column = (ColumnRefOperator) in.getChild(0);
            return supportedScalarType(column) ? equalityIndexUse(column.getName()) : null;
        }
        if (predicate instanceof IsNullPredicateOperator) {
            ScalarOperator child = predicate.getChild(0);
            if (!(child instanceof ColumnRefOperator) || !supportedScalarType((ColumnRefOperator) child)) {
                return null;
            }
            return equalityIndexUse(((ColumnRefOperator) child).getName());
        }
        if (predicate instanceof CallOperator) {
            CallOperator call = (CallOperator) predicate;
            boolean supported = FunctionSet.STARTS_WITH.equalsIgnoreCase(call.getFnName())
                    && call.getChildren().size() == 2
                    && call.getChild(0) instanceof ColumnRefOperator
                    && call.getChild(0).getType().isStringType()
                    && isNonNullScalarLiteral(call.getChild(1))
                    && metadata.supports(((ColumnRefOperator) call.getChild(0)).getName(),
                    ConnectorIndexType.RANGE);
            return supported ? new IndexUse(((ColumnRefOperator) call.getChild(0)).getName(),
                    ConnectorIndexType.RANGE) : null;
        }
        return null;
    }

    private IndexUse getBinaryIndexUse(BinaryPredicateOperator predicate) {
        ScalarOperator first = predicate.getChild(0);
        ScalarOperator second = predicate.getChild(1);
        ColumnRefOperator column;
        if (first instanceof ColumnRefOperator && isNonNullScalarLiteral(second)) {
            column = (ColumnRefOperator) first;
        } else if (second instanceof ColumnRefOperator && isNonNullScalarLiteral(first)) {
            column = (ColumnRefOperator) second;
        } else {
            return null;
        }
        if (!supportedScalarType(column)) {
            return null;
        }

        if (EQUALITY_TYPES.contains(predicate.getBinaryType())) {
            return equalityIndexUse(column.getName());
        }
        return RANGE_TYPES.contains(predicate.getBinaryType())
                && metadata.supports(column.getName(), ConnectorIndexType.RANGE)
                ? new IndexUse(column.getName(), ConnectorIndexType.RANGE) : null;
    }

    private IndexUse equalityIndexUse(String columnName) {
        if (metadata.supports(columnName, ConnectorIndexType.BITMAP)) {
            return new IndexUse(columnName, ConnectorIndexType.BITMAP);
        }
        return metadata.supports(columnName, ConnectorIndexType.RANGE)
                ? new IndexUse(columnName, ConnectorIndexType.RANGE) : null;
    }

    private static boolean supportedScalarType(ColumnRefOperator column) {
        return column.getType().isBoolean() || column.getType().isTinyint() || column.getType().isSmallint()
                || column.getType().isInt() || column.getType().isBigint()
                || column.getType().isStringType();
    }

    private static ScalarOperator unwrapFloatingPointCast(ScalarOperator expression) {
        ScalarOperator current = expression;
        while (current instanceof CastOperator && current.getType().isFloatingPointType()) {
            current = current.getChild(0);
        }
        return current;
    }

    private static boolean isLiteral(ScalarOperator expression) {
        if (expression instanceof ConstantOperator) {
            return true;
        }
        if (expression instanceof CastOperator) {
            return isLiteral(expression.getChild(0));
        }
        return expression instanceof ArrayOperator && expression.getChildren().stream().allMatch(IndexAnalyzer::isLiteral);
    }

    private static boolean isFloatArrayLiteral(ScalarOperator expression) {
        ScalarOperator unwrapped = expression;
        while (unwrapped instanceof CastOperator) {
            unwrapped = unwrapped.getChild(0);
        }
        return unwrapped instanceof ArrayOperator && unwrapped.getType() instanceof ArrayType
                && ((ArrayType) unwrapped.getType()).getItemType().isFloatingPointType()
                && isLiteral(unwrapped);
    }

    private static boolean isNonNullScalarLiteral(ScalarOperator expression) {
        if (expression == null || !isSupportedScalarLiteralType(expression.getType())) {
            return false;
        }
        if (expression instanceof CastOperator) {
            ScalarOperator child = expression.getChild(0);
            return isJsonLiteralPreservingCast(child.getType(), expression.getType())
                    && isNonNullScalarLiteral(child);
        }
        return expression instanceof ConstantOperator && !((ConstantOperator) expression).isNull();
    }

    private static boolean isSupportedScalarLiteralType(Type type) {
        return type.isBoolean() || type.isTinyint() || type.isSmallint() || type.isInt() || type.isBigint()
                || type.isStringType();
    }

    private static int integerTypeRank(Type type) {
        if (type.isTinyint()) {
            return 1;
        }
        if (type.isSmallint()) {
            return 2;
        }
        if (type.isInt()) {
            return 3;
        }
        if (type.isBigint()) {
            return 4;
        }
        return -1;
    }

    private static boolean isJsonLiteralPreservingCast(Type from, Type to) {
        if (from.equals(to)) {
            return true;
        }
        int fromIntegerRank = integerTypeRank(from);
        return fromIntegerRank > 0 && integerTypeRank(to) >= fromIntegerRank;
    }

    private static final class IndexUse {
        private final String columnName;
        private final ConnectorIndexType indexType;

        private IndexUse(String columnName, ConnectorIndexType indexType) {
            this.columnName = columnName;
            this.indexType = indexType;
        }
    }
}
