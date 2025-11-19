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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ArithmeticExpr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.ScalarType;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ArithmeticExpr extends Expr {
    private static final Map<String, Operator> SUPPORT_FUNCTIONS = ImmutableMap.<String, Operator>builder()
            .put(Operator.MULTIPLY.getName(), Operator.MULTIPLY)
            .put(Operator.DIVIDE.getName(), Operator.DIVIDE)
            .put(Operator.MOD.getName(), Operator.MOD)
            .put(Operator.INT_DIVIDE.getName(), Operator.INT_DIVIDE)
            .put(Operator.ADD.getName(), Operator.ADD)
            .put(Operator.SUBTRACT.getName(), Operator.SUBTRACT)
            .put(Operator.BITAND.getName(), Operator.BITAND)
            .put(Operator.BITOR.getName(), Operator.BITOR)
            .put(Operator.BITXOR.getName(), Operator.BITXOR)
            .put(Operator.BIT_SHIFT_LEFT.getName(), Operator.BIT_SHIFT_LEFT)
            .put(Operator.BIT_SHIFT_RIGHT.getName(), Operator.BIT_SHIFT_RIGHT)
            .put(Operator.BIT_SHIFT_RIGHT_LOGICAL.getName(), Operator.BIT_SHIFT_RIGHT_LOGICAL)
            .build();

    public static Set<String> DECIMAL_SCALE_ADJUST_OPERATOR_SET = ImmutableSet.<String>builder()
            .add(Operator.ADD.name)
            .add(Operator.SUBTRACT.name)
            .add(Operator.MULTIPLY.name)
            .add(Operator.DIVIDE.name)
            .add(Operator.MOD.name)
            .add(Operator.INT_DIVIDE.name)
            .build();

    private final Operator op;

    public enum OperatorPosition {
        BINARY_INFIX,
        UNARY_PREFIX,
        UNARY_POSTFIX,
    }

    public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
        this(op, e1, e2, NodePosition.ZERO);
    }

    public ArithmeticExpr(Operator op, Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
                op == Operator.BITNOT && e2 == null || op != Operator.BITNOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
    }

    /**
     * Copy c'tor used in clone().
     */
    protected ArithmeticExpr(ArithmeticExpr other) {
        super(other);
        this.op = other.op;
    }

    public static boolean isArithmeticExpr(String functionName) {
        return SUPPORT_FUNCTIONS.containsKey(functionName.toLowerCase());
    }

    public static Operator getArithmeticOperator(String functionName) {
        return SUPPORT_FUNCTIONS.get(functionName.toLowerCase());
    }

    @Override
    public String toString() {
        return ExprToSql.toSql(this);
    }

    @Override
    public Expr clone() {
        return new ArithmeticExpr(this);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }

        return op == ((ArithmeticExpr) obj).op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }

    @Override
    public boolean isNullable() {
        if (op == Operator.DIVIDE || op == Operator.INT_DIVIDE || op == Operator.MOD) {
            return true;
        }
        return children.stream().anyMatch(e -> e.isNullable() || e.getType().isDecimalV3());
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitArithmeticExpr(this, context);
    }

    public Operator getOp() {
        return op;
    }

    public enum Operator {
        MULTIPLY("*", "multiply", OperatorPosition.BINARY_INFIX, true),
        DIVIDE("/", "divide", OperatorPosition.BINARY_INFIX, true),
        MOD("%", "mod", OperatorPosition.BINARY_INFIX, false),
        INT_DIVIDE("DIV", "int_divide", OperatorPosition.BINARY_INFIX, true),
        ADD("+", "add", OperatorPosition.BINARY_INFIX, true),
        SUBTRACT("-", "subtract", OperatorPosition.BINARY_INFIX, true),
        BITAND("&", "bitand", OperatorPosition.BINARY_INFIX, false),
        BITOR("|", "bitor", OperatorPosition.BINARY_INFIX, false),
        BITXOR("^", "bitxor", OperatorPosition.BINARY_INFIX, false),
        BITNOT("~", "bitnot", OperatorPosition.UNARY_PREFIX, false),
        FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX, true),
        BIT_SHIFT_LEFT("BITSHIFTLEFT", "bitShiftLeft", OperatorPosition.BINARY_INFIX, false),
        BIT_SHIFT_RIGHT("BITSHIFTRIGHT", "bitShiftRight", OperatorPosition.BINARY_INFIX, false),
        BIT_SHIFT_RIGHT_LOGICAL("BITSHIFTRIGHTLOGICAL", "bitShiftRightLogical", OperatorPosition.BINARY_INFIX,
                false);

        private final String description;
        private final String name;
        private final OperatorPosition pos;
        private final boolean monotonic;

        Operator(String description, String name, OperatorPosition pos, boolean monotonic) {
            this.description = description;
            this.name = name;
            this.pos = pos;
            this.monotonic = monotonic;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public OperatorPosition getPos() {
            return pos;
        }

        public boolean isBinary() {
            return pos == OperatorPosition.BINARY_INFIX;
        }

        public boolean isMonotonic() {
            return monotonic;
        }
    }

    public static class TypeTriple {
        public ScalarType returnType;
        public ScalarType lhsTargetType;
        public ScalarType rhsTargetType;
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }
}
