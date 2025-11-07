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
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

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

    // cast int128 into decimal128(38, 0).
    // cast int64(including narrower integer types) into decimal64(18, 0).
    // cast float32 into decimal64(18,6).
    // cast float64 into decimal128(38,9).
    // other types, throw an error to indicates an explicit cast is required
    private static Type nonDecimalToDecimal(Type type) {
        Preconditions.checkState(!type.isDecimalV3(), "Type of rhs may not be DecimalV3");
        if (type.isLargeIntType()) {
            return DecimalType.DECIMAL128_INT;
        } else if (type.isBigint()) {
            return DecimalType.DECIMAL64_INT;
        } else if (type.isIntegerType() || type.isBoolean()) {
            return DecimalType.DECIMAL32_INT;
        } else if (type.isDecimalV2()) {
            return DecimalType.DEFAULT_DECIMAL128;
        } else if (type.isNull() || type.isFloatingPointType() || type.isStringType()) {
            return DecimalType.DECIMAL_ZERO;
        } else {
            Preconditions.checkState(false,
                    "Implicit casting for decimal arithmetic operations only support integer/float/boolean/null");
            return InvalidType.INVALID;
        }
    }

    // For decimal addition, to avoid overflow, we adopt this scaling strategy:
    // as much as possible to ensure correctness
    // result precision is maximum integer part width + maximum fractional part width + 1
    // This can be fully guaranteed correctness in case of sufficient precision
    public static void getAddSubReturnTypeOfDecimal(TypeTriple triple, ScalarType lhsType, ScalarType rhsType) {
        final int lhsPrecision = lhsType.getPrecision();
        final int rhsPrecision = rhsType.getPrecision();
        final int lhsScale = lhsType.getScalarScale();
        final int rhsScale = rhsType.getScalarScale();

        int maxRetPrecision = 38;
        // TODO(stephen): support auto scale up decimal precision
        if (triple.lhsTargetType.isDecimal256() || triple.rhsTargetType.isDecimal256()) {
            maxRetPrecision = 76;
        }
        // decimal(p1, s1) + decimal(p2, s2)
        // result type = decimal(max(p1 - s1, p2 - s2) + max(s1, s2) + 1, max(s1, s2))
        int maxIntLength = Math.max(lhsPrecision - lhsScale, rhsPrecision - rhsScale);
        int retPrecision = maxIntLength + Math.max(lhsScale, rhsScale) + 1;
        int retScale = Math.max(lhsScale, rhsScale);
        // precision
        retPrecision = Math.min(retPrecision, maxRetPrecision);
        PrimitiveType decimalType = PrimitiveType.getDecimalPrimitiveType(retPrecision);
        decimalType = PrimitiveType.getWiderDecimalV3Type(decimalType, lhsType.getPrimitiveType());
        decimalType = PrimitiveType.getWiderDecimalV3Type(decimalType, rhsType.getPrimitiveType());

        triple.lhsTargetType = TypeFactory.createDecimalV3Type(decimalType, retPrecision, lhsScale);
        triple.rhsTargetType = TypeFactory.createDecimalV3Type(decimalType, retPrecision, rhsScale);
        triple.returnType = TypeFactory.createDecimalV3Type(decimalType, retPrecision, retScale);
    }

    public static TypeTriple getReturnTypeOfDecimal(Operator op, ScalarType lhsType, ScalarType rhsType)
            throws SemanticException {
        Preconditions.checkState(lhsType.isDecimalV3() && rhsType.isDecimalV3(),
                "Types of lhs and rhs must be DecimalV3");
        final PrimitiveType lhsPtype = lhsType.getPrimitiveType();
        final PrimitiveType rhsPtype = rhsType.getPrimitiveType();
        final int lhsPrecision = lhsType.getPrecision();
        final int rhsPrecision = rhsType.getPrecision();
        final int lhsScale = lhsType.getScalarScale();
        final int rhsScale = rhsType.getScalarScale();

        // get the wider decimal type
        PrimitiveType widerType = PrimitiveType.getWiderDecimalV3Type(lhsPtype, rhsPtype);
        // compute arithmetic expr use decimal64 for both decimal32 and decimal64
        widerType = PrimitiveType.getWiderDecimalV3Type(widerType, PrimitiveType.DECIMAL64);
        int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(widerType);

        TypeTriple result = new TypeTriple();
        result.lhsTargetType = TypeFactory.createDecimalV3Type(widerType, maxPrecision, lhsScale);
        result.rhsTargetType = TypeFactory.createDecimalV3Type(widerType, maxPrecision, rhsScale);
        int returnScale = 0;
        int returnPrecision = 0;
        switch (op) {
            case ADD:
            case SUBTRACT:
                getAddSubReturnTypeOfDecimal(result, lhsType, rhsType);
                return result;
            case MOD:
                returnScale = Math.max(lhsScale, rhsScale);
                break;
            case MULTIPLY:
                returnScale = lhsScale + rhsScale;
                returnPrecision = lhsPrecision + rhsPrecision;
                PrimitiveType defaultMaxDecimalType = PrimitiveType.DECIMAL128;
                // TODO(stephen): support auto scale up decimal precision
                if (result.lhsTargetType.isDecimal256() || result.rhsTargetType.isDecimal256()) {
                    defaultMaxDecimalType = PrimitiveType.DECIMAL256;
                }
                final int maxDecimalPrecision = PrimitiveType.getMaxPrecisionOfDecimal(defaultMaxDecimalType);
                if (returnPrecision <= maxDecimalPrecision) {
                    // returnPrecision <= 38, result never overflows, use the narrowest decimal type that can holds the result.
                    // for examples:
                    // decimal32(4,3) * decimal32(4,3) => decimal32(8,6);
                    // decimal64(15,3) * decimal32(9,4) => decimal128(24,7).
                    PrimitiveType commonPtype =
                            TypeFactory.createDecimalV3NarrowestType(returnPrecision, returnScale).getPrimitiveType();
                    // TODO(stephen): support auto scale up decimal precision
                    if (defaultMaxDecimalType == PrimitiveType.DECIMAL128 && commonPtype == PrimitiveType.DECIMAL256) {
                        commonPtype = PrimitiveType.DECIMAL128;
                    }

                    // a common type shall never be narrower than type of lhs and rhs
                    commonPtype = PrimitiveType.getWiderDecimalV3Type(commonPtype, lhsPtype);
                    commonPtype = PrimitiveType.getWiderDecimalV3Type(commonPtype, rhsPtype);
                    result.returnType = TypeFactory.createDecimalV3Type(commonPtype, returnPrecision, returnScale);
                    result.lhsTargetType = TypeFactory.createDecimalV3Type(commonPtype, lhsPrecision, lhsScale);
                    result.rhsTargetType = TypeFactory.createDecimalV3Type(commonPtype, rhsPrecision, rhsScale);
                    return result;
                } else if (returnScale <= maxDecimalPrecision) {
                    ConnectContext connectContext = ConnectContext.get();
                    if (connectContext != null && connectContext.getSessionVariable().isDecimalOverflowToDouble()) {
                        // Convert to double when precision overflow and session variable is enabled
                        result.returnType = FloatType.DOUBLE;
                        result.lhsTargetType = FloatType.DOUBLE;
                        result.rhsTargetType = FloatType.DOUBLE;
                        return result;
                    }
                    // returnPrecision > maxDecimalPrecision(38 or 76) and returnScale <= maxDecimalPrecision,
                    // the multiplication is computable but the result maybe overflow,
                    // so use decimal128 or decimal256 arithmetic and adopt maximum decimal precision(38) or precision(76)
                    // as precision of the result.
                    // for examples:
                    // decimal128(23,5) * decimal64(18,4) => decimal128(38, 9).
                    // TODO(stephen): support auto scale up decimal precision
                    result.returnType =
                            TypeFactory.createDecimalV3Type(defaultMaxDecimalType, maxDecimalPrecision, returnScale);
                    result.lhsTargetType =
                            TypeFactory.createDecimalV3Type(defaultMaxDecimalType, lhsPrecision, lhsScale);
                    result.rhsTargetType =
                            TypeFactory.createDecimalV3Type(defaultMaxDecimalType, rhsPrecision, rhsScale);
                    return result;
                } else {
                    // returnScale > 38, so it is cannot be represented as decimal.
                    throw new SemanticException(
                            String.format(
                                    "Return scale(%d) exceeds maximum value(%d), please cast decimal type to low-precision one",
                                    returnScale, maxDecimalPrecision));
                }

            case INT_DIVIDE:
            case DIVIDE:
                if (lhsScale <= 6) {
                    returnScale = lhsScale + 6;
                } else if (lhsScale <= 12) {
                    returnScale = 12;
                } else {
                    returnScale = lhsScale;
                }
                widerType = PrimitiveType.DECIMAL128;
                // TODO(stephen): support auto scale up decimal precision
                if (result.lhsTargetType.isDecimal256() || result.rhsTargetType.isDecimal256()) {
                    widerType = PrimitiveType.DECIMAL256;
                }

                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(widerType);
                result.lhsTargetType = TypeFactory.createDecimalV3Type(widerType, maxPrecision, lhsScale);
                result.rhsTargetType = TypeFactory.createDecimalV3Type(widerType, maxPrecision, rhsScale);
                int adjustedScale = returnScale + rhsScale;
                if (adjustedScale > maxPrecision) {
                    throw new SemanticException(
                            String.format(
                                    "Dividend fails to adjust scale to %d that exceeds maximum value(%d)",
                                    adjustedScale,
                                    maxPrecision));
                }
                break;
            case BITAND:
            case BITOR:
            case BITXOR:
            case BIT_SHIFT_LEFT:
            case BIT_SHIFT_RIGHT:
            case BIT_SHIFT_RIGHT_LOGICAL:
                result.lhsTargetType = IntegerType.BIGINT;
                result.rhsTargetType = IntegerType.BIGINT;
                result.returnType = IntegerType.BIGINT;
                return result;
            default:
                Preconditions.checkState(false, "DecimalV3 only support operators: +-*/%&|^");
        }
        result.returnType = op == Operator.INT_DIVIDE ? IntegerType.BIGINT :
                TypeFactory.createDecimalV3Type(widerType, maxPrecision, returnScale);
        return result;
    }

    private TypeTriple rewriteDecimalDecimalOperation() throws AnalysisException {
        final Type lhsOriginType = getChild(0).type;
        final Type rhsOriginType = getChild(1).type;

        Type lhsTargetType = lhsOriginType;
        Type rhsTargetType = rhsOriginType;

        if (!lhsTargetType.isDecimalV3()) {
            lhsTargetType = nonDecimalToDecimal(lhsTargetType);
        }
        if (!rhsTargetType.isDecimalV3()) {
            rhsTargetType = nonDecimalToDecimal(rhsTargetType);
        }
        return getReturnTypeOfDecimal(op, (ScalarType) lhsTargetType, (ScalarType) rhsTargetType);
    }

    private TypeTriple rewriteDecimalFloatingPointOperation() throws AnalysisException {
        TypeTriple typeTriple = new TypeTriple();
        typeTriple.lhsTargetType = FloatType.DOUBLE;
        typeTriple.rhsTargetType = FloatType.DOUBLE;
        typeTriple.returnType = FloatType.DOUBLE;
        return typeTriple;
    }

    private boolean hasFloatingPointOrStringType() {
        Type lhsType = getChild(0).type;
        Type rhsType = getChild(1).type;
        return lhsType.isFloatingPointType() || lhsType.isStringType() || rhsType.isFloatingPointType() ||
                rhsType.isStringType();
    }

    private boolean resultTypeIsBigInt() {
        switch (op) {
            case BITAND:
            case BITOR:
            case BITXOR:
            case BITNOT:
            case INT_DIVIDE:
            case BIT_SHIFT_LEFT:
            case BIT_SHIFT_RIGHT:
            case BIT_SHIFT_RIGHT_LOGICAL:
                return true;
            default:
                return false;
        }
    }

    public TypeTriple rewriteDecimalOperation() throws AnalysisException {
        if (hasFloatingPointOrStringType() && !resultTypeIsBigInt()) {
            return rewriteDecimalFloatingPointOperation();
        } else {
            return rewriteDecimalDecimalOperation();
        }
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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
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
