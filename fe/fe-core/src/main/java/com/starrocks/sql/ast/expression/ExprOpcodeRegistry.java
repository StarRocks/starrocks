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

package com.starrocks.sql.ast.expression;

import com.starrocks.thrift.TExprOpcode;

import java.util.EnumMap;
import java.util.Objects;

/**
 * Central registry that maintains the mapping between front-end expression metadata
 * and the corresponding thrift {@link TExprOpcode}. This decouples expression
 * classes from thrift-specific details.
 */
public final class ExprOpcodeRegistry {
    private static final EnumMap<BinaryType, TExprOpcode> BINARY_OPCODES = new EnumMap<>(BinaryType.class);
    private static final EnumMap<ArithmeticExpr.Operator, TExprOpcode> ARITHMETIC_OPCODES =
            new EnumMap<>(ArithmeticExpr.Operator.class);
    private static final EnumMap<MatchExpr.MatchOperator, TExprOpcode> MATCH_OPCODES =
            new EnumMap<>(MatchExpr.MatchOperator.class);

    static {
        BINARY_OPCODES.put(BinaryType.EQ, TExprOpcode.EQ);
        BINARY_OPCODES.put(BinaryType.NE, TExprOpcode.NE);
        BINARY_OPCODES.put(BinaryType.LE, TExprOpcode.LE);
        BINARY_OPCODES.put(BinaryType.GE, TExprOpcode.GE);
        BINARY_OPCODES.put(BinaryType.LT, TExprOpcode.LT);
        BINARY_OPCODES.put(BinaryType.GT, TExprOpcode.GT);
        BINARY_OPCODES.put(BinaryType.EQ_FOR_NULL, TExprOpcode.EQ_FOR_NULL);

        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.MULTIPLY, TExprOpcode.MULTIPLY);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.DIVIDE, TExprOpcode.DIVIDE);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.MOD, TExprOpcode.MOD);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.INT_DIVIDE, TExprOpcode.INT_DIVIDE);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.ADD, TExprOpcode.ADD);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.SUBTRACT, TExprOpcode.SUBTRACT);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BITAND, TExprOpcode.BITAND);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BITOR, TExprOpcode.BITOR);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BITXOR, TExprOpcode.BITXOR);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BITNOT, TExprOpcode.BITNOT);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.FACTORIAL, TExprOpcode.FACTORIAL);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BIT_SHIFT_LEFT, TExprOpcode.BIT_SHIFT_LEFT);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT, TExprOpcode.BIT_SHIFT_RIGHT);
        ARITHMETIC_OPCODES.put(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL,
                TExprOpcode.BIT_SHIFT_RIGHT_LOGICAL);

        MATCH_OPCODES.put(MatchExpr.MatchOperator.MATCH, TExprOpcode.MATCH);
        MATCH_OPCODES.put(MatchExpr.MatchOperator.MATCH_ANY, TExprOpcode.MATCH_ANY);
        MATCH_OPCODES.put(MatchExpr.MatchOperator.MATCH_ALL, TExprOpcode.MATCH_ALL);
    }

    private ExprOpcodeRegistry() {
    }

    public static TExprOpcode getBinaryOpcode(BinaryType type) {
        return BINARY_OPCODES.getOrDefault(type, TExprOpcode.INVALID_OPCODE);
    }

    public static TExprOpcode getArithmeticOpcode(ArithmeticExpr.Operator operator) {
        return ARITHMETIC_OPCODES.getOrDefault(operator, TExprOpcode.INVALID_OPCODE);
    }

    public static TExprOpcode getMatchOpcode(MatchExpr.MatchOperator operator) {
        return MATCH_OPCODES.getOrDefault(operator, TExprOpcode.INVALID_OPCODE);
    }

    public static TExprOpcode getInPredicateOpcode(boolean isNotIn) {
        return isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
    }

    public static TExprOpcode getCastOpcode() {
        return TExprOpcode.CAST;
    }

    /**
     * Resolve opcode for a specific expression instance. The default value is
     * {@link TExprOpcode#INVALID_OPCODE} when there is no mapping required.
     */
    public static TExprOpcode getExprOpcode(Expr expr) {
        Objects.requireNonNull(expr, "expr cannot be null");
        if (expr instanceof BinaryPredicate) {
            return getBinaryOpcode(((BinaryPredicate) expr).getOp());
        } else if (expr instanceof InPredicate) {
            return getInPredicateOpcode(((InPredicate) expr).isNotIn());
        } else if (expr instanceof ArithmeticExpr) {
            return getArithmeticOpcode(((ArithmeticExpr) expr).getOp());
        } else if (expr instanceof CastExpr) {
            return getCastOpcode();
        } else if (expr instanceof MatchExpr) {
            return getMatchOpcode(((MatchExpr) expr).getMatchOperator());
        }
        return TExprOpcode.INVALID_OPCODE;
    }
}
