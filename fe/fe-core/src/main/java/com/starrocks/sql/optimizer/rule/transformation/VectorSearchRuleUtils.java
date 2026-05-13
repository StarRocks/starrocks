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
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.common.VectorIndexParams;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;

import java.util.List;
import java.util.Optional;

import static com.starrocks.sql.ast.expression.BinaryType.GE;
import static com.starrocks.sql.ast.expression.BinaryType.LE;

/**
 * Shared utilities for vector search rewrite rules (both OLAP and external tables).
 */
public class VectorSearchRuleUtils {

    private VectorSearchRuleUtils() {
    }

    /**
     * Whether the scalar operator is a constant array of float.
     * Matches `ArrayOperator(type=ArrayType(numeric))` or
     * `CastOperator(child=ArrayOperator, type=ArrayType(float))`.
     */
    public static boolean isConstantArrayFloat(ScalarOperator scalarOperator) {
        if (!scalarOperator.isConstant()) {
            return false;
        }

        if (scalarOperator instanceof CastOperator) {
            if (!scalarOperator.getType().isArrayType()) {
                return false;
            }
            ArrayType arrayType = (ArrayType) scalarOperator.getType();
            if (!arrayType.getItemType().isFloatingPointType()) {
                return false;
            }
            return scalarOperator.getChildren().stream().allMatch(VectorSearchRuleUtils::isConstantArrayFloat);
        } else if (scalarOperator instanceof ArrayOperator) {
            if (!scalarOperator.getType().isArrayType()) {
                return false;
            }
            ArrayType innerArrayType = (ArrayType) scalarOperator.getType();
            return innerArrayType.getItemType().isNumericType();
        } else {
            return false;
        }
    }

    /**
     * Recursively extract constant values from a scalar tree into {@code vectorQuery}.
     */
    public static void extractValuesFromConstantArray(ScalarOperator scalarOperator, List<String> vectorQuery) {
        if (scalarOperator instanceof ColumnRefOperator) {
            return;
        }
        if (scalarOperator instanceof ConstantOperator) {
            vectorQuery.add(String.valueOf(((ConstantOperator) scalarOperator).getValue()));
            return;
        }
        for (ScalarOperator child : scalarOperator.getChildren()) {
            extractValuesFromConstantArray(child, vectorQuery);
        }
    }

    /**
     * Check if {@code operator} matches the given vector function call, allowing
     * a cast-to-float wrapper.
     */
    public static boolean matchesVectorFuncCall(CallOperator vectorFuncCallOperator, ScalarOperator operator) {
        if (operator instanceof CastOperator) {
            CastOperator castOperator = (CastOperator) operator;
            return castOperator.getType().isFloatingPointType() &&
                    matchesVectorFuncCall(vectorFuncCallOperator, castOperator.getChild(0));
        }
        if (operator instanceof CallOperator) {
            return vectorFuncCallOperator.equals(operator);
        }
        return false;
    }

    /**
     * Extract the vector range value from predicates (e.g., distance <= 10).
     * Only supports binary predicates and AND compound predicates.
     */
    public static Optional<Double> extractVectorRange(ScalarOperator predicate, VectorFuncInfo info) {
        if (predicate instanceof BinaryPredicateOperator) {
            return parseVectorRangeFromBinaryPredicate(predicate, info);
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
            if (!compoundPredicate.isAnd()) {
                return Optional.empty();
            }
            Optional<Double> value = Optional.empty();
            for (ScalarOperator child : predicate.getChildren()) {
                Optional<Double> childValue = parseVectorRangeFromBinaryPredicate(child, info);
                if (childValue.isEmpty()) {
                    return Optional.empty();
                }
                if (value.isEmpty()) {
                    value = childValue;
                } else {
                    if (info.isAscending) {
                        value = Optional.of(Math.min(value.get(), childValue.get()));
                    } else {
                        value = Optional.of(Math.max(value.get(), childValue.get()));
                    }
                }
            }
            return value;
        }
        return Optional.empty();
    }

    public static Optional<Double> parseVectorRangeFromBinaryPredicate(ScalarOperator predicate, VectorFuncInfo info) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryType binaryType = ((BinaryPredicateOperator) predicate).getBinaryType();
            ScalarOperator lhs = predicate.getChild(0);
            ScalarOperator rhs = predicate.getChild(1);

            if (rhs instanceof ConstantOperator && matchesVectorFuncCall(info.vectorFuncCallOperator, lhs) &&
                    (((binaryType.equals(LE)) && info.isAscending) || ((binaryType.equals(GE)) && !info.isAscending))) {
                return Optional.of((double) ((ConstantOperator) rhs).getValue());
            } else if (lhs instanceof ConstantOperator && matchesVectorFuncCall(info.vectorFuncCallOperator, rhs) &&
                    (((binaryType.equals(GE)) && info.isAscending) || ((binaryType.equals(LE)) && !info.isAscending))) {
                return Optional.of((double) ((ConstantOperator) lhs).getValue());
            }
        }
        return Optional.empty();
    }

    /**
     * Replace occurrences of the vector function call in the operator tree
     * with a column ref pointing to the distance column.
     */
    public static ScalarOperator rewriteScalarOperatorByDistanceColumn(
            ScalarOperator scalarOperator, VectorFuncInfo info, ColumnRefOperator distanceColRef) {
        if (scalarOperator.equals(info.vectorFuncCallOperator)) {
            return distanceColRef;
        }
        for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
            ScalarOperator child = scalarOperator.getChild(i);
            scalarOperator.setChild(i, rewriteScalarOperatorByDistanceColumn(child, info, distanceColRef));
        }
        return scalarOperator;
    }

    /**
     * Table-type-agnostic information about a vector function call in a query.
     * OLAP rules carry an {@code Index} reference; Paimon rules leave it null.
     */
    public static class VectorFuncInfo {
        public final ColumnRefOperator inColumnRef;
        public final ColumnRefOperator outColumnRef;
        public final CallOperator vectorFuncCallOperator;
        public final VectorIndexParams.MetricsType metricType;
        public final List<String> vectorQuery;
        public final boolean isAscending;

        public VectorFuncInfo(ColumnRefOperator inColumnRef,
                              ColumnRefOperator outColumnRef,
                              CallOperator vectorFuncCallOperator,
                              VectorIndexParams.MetricsType metricType,
                              List<String> vectorQuery,
                              boolean isAscending) {
            this.inColumnRef = inColumnRef;
            this.outColumnRef = outColumnRef;
            this.vectorFuncCallOperator = vectorFuncCallOperator;
            this.metricType = metricType;
            this.vectorQuery = vectorQuery;
            this.isAscending = isAscending;
        }
    }
}
