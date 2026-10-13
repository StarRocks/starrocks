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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.BinaryType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Versioned, compact serialization for scalar predicates evaluated by connector index providers.
 *
 * <p>This is deliberately narrower than general expression serialization. An unsupported literal
 * or expression makes distributed evaluation fall back to SDK planning. BE validates the payload
 * again before touching an index. Compact keys keep each shard's request small.
 */
public final class ScalarOperatorSerializer {
    public static final String ARGUMENTS = "a";
    public static final String BINARY_TYPE = "b";
    public static final String CHILDREN = "c";
    public static final String COMPOUND_TYPE = "ct";
    public static final String FN_NAME = "f";
    public static final String NAME = "n";
    public static final String NEGATED = "ng";
    public static final String OPERATOR_TYPE = "o";
    public static final String TYPE = "t";
    public static final String VALUE = "v";

    private ScalarOperatorSerializer() {
    }

    public static Map<String, Object> toJson(ScalarOperator operator) {
        if (operator instanceof BinaryPredicateOperator) {
            return serializeBinary((BinaryPredicateOperator) operator);
        }
        if (operator instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) operator;
            Map<String, Object> result = node("cp");
            result.put(COMPOUND_TYPE, compound.getCompoundType().name());
            result.put(CHILDREN, serializeChildren(compound.getChildren()));
            return result;
        }
        if (operator instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) operator;
            Map<String, Object> result = node("ip");
            result.put(NEGATED, in.isNotIn());
            result.put(CHILDREN, serializeChildren(in.getChildren()));
            return result;
        }
        if (operator instanceof IsNullPredicateOperator) {
            IsNullPredicateOperator isNull = (IsNullPredicateOperator) operator;
            Map<String, Object> result = node("isn");
            result.put(NEGATED, isNull.isNotNull());
            result.put(CHILDREN, serializeChildren(isNull.getChildren()));
            return result;
        }
        if (operator instanceof CastOperator) {
            // CastOperator is a CallOperator too. Check it before generic calls, and do
            // not relabel literals without applying SQL conversion semantics.
            throw new IllegalArgumentException("Fold literal casts before connector index serialization");
        }
        if (operator instanceof CallOperator) {
            CallOperator call = (CallOperator) operator;
            Map<String, Object> result = node("ca");
            result.put(FN_NAME, call.getFnName());
            result.put(ARGUMENTS, serializeChildren(call.getChildren()));
            return result;
        }
        if (operator instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) operator;
            if (constant.isNull()) {
                throw new IllegalArgumentException("Null literals must be represented by IS NULL");
            }
            Map<String, Object> result = node("co");
            result.put(TYPE, constant.getType().toTypeString());
            result.put(VALUE, constant.getValue());
            return result;
        }
        if (operator instanceof ColumnRefOperator) {
            ColumnRefOperator column = (ColumnRefOperator) operator;
            Map<String, Object> result = node("cr");
            result.put(TYPE, column.getType().toTypeString());
            result.put(NAME, column.getName());
            return result;
        }
        throw new IllegalArgumentException("Unsupported connector index expression: " + operator.getClass());
    }

    private static Map<String, Object> serializeBinary(BinaryPredicateOperator binary) {
        ScalarOperator left = binary.getChild(0);
        ScalarOperator right = binary.getChild(1);
        BinaryType type = binary.getBinaryType();
        if (!(left instanceof ColumnRefOperator) && right instanceof ColumnRefOperator) {
            ScalarOperator swapped = left;
            left = right;
            right = swapped;
            type = type.commutative();
        }
        Map<String, Object> result = node("b");
        result.put(BINARY_TYPE, type.name());
        result.put(CHILDREN, List.of(toJson(left), toJson(right)));
        return result;
    }

    private static List<Map<String, Object>> serializeChildren(List<ScalarOperator> children) {
        List<Map<String, Object>> result = new ArrayList<>(children.size());
        for (ScalarOperator child : children) {
            result.add(toJson(child));
        }
        return result;
    }

    private static Map<String, Object> node(String operatorType) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(OPERATOR_TYPE, operatorType);
        return result;
    }
}
