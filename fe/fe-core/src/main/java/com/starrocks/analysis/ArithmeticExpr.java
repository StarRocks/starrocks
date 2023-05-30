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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;

import java.util.Arrays;
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

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getNumericTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.MULTIPLY.getName(), Lists.newArrayList(t, t), t));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.ADD.getName(), Lists.newArrayList(t, t), t));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.SUBTRACT.getName(), Lists.newArrayList(t, t), t));
        }
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
                Type.DOUBLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMALV2, Type.DECIMALV2),
                Type.DECIMALV2));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL32, Type.DECIMAL32),
                Type.DECIMAL32));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL64, Type.DECIMAL64),
                Type.DECIMAL64));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL128, Type.DECIMAL128),
                Type.DECIMAL128));

        // MOD(), FACTORIAL(), BITAND(), BITOR(), BITXOR(), and BITNOT() are registered as
        // builtins, see starrocks_functions.py
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), t));
        }
        for (Type t : Arrays.asList(Type.DECIMAL32, Type.DECIMAL64, Type.DECIMAL128)) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), Type.BIGINT));
        }
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.BIT_SHIFT_LEFT.getName(), Lists.newArrayList(t, Type.BIGINT), t));
        }
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.BIT_SHIFT_RIGHT.getName(), Lists.newArrayList(t, Type.BIGINT), t));
        }
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.BIT_SHIFT_RIGHT_LOGICAL.getName(), Lists.newArrayList(t, Type.BIGINT), t));
        }
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
            return Type.DECIMAL128_INT;
        } else if (type.isBigint()) {
            return Type.DECIMAL64_INT;
        } else if (type.isIntegerType() || type.isBoolean()) {
            return Type.DECIMAL32_INT;
        } else if (type.isDecimalV2()) {
            return Type.DEFAULT_DECIMAL128;
        } else if (type.isNull() || type.isFloatingPointType() || type.isStringType()) {
            return Type.DECIMAL_ZERO;
        } else {
            Preconditions.checkState(false,
                    "Implicit casting for decimal arithmetic operations only support integer/float/boolean/null");
            return Type.INVALID;
        }
    }

    // For decimal addition, to avoid overflow, we adopt this scaling strategy:
    // as much as possible to ensure correctness
    // result precision is maximum integer part width + maximum fractional part width + 1
    // This can be fully guaranteed correctness in case of sufficient precision
    public static void getAddReturnTypeOfDecimal(TypeTriple triple, ScalarType lhsType, ScalarType rhsType) {
        final int lhsPrecision = lhsType.getPrecision();
        final int rhsPrecision = rhsType.getPrecision();
        final int lhsScale = lhsType.getScalarScale();
        final int rhsScale = rhsType.getScalarScale();

        // decimal(p1, s1) + decimal(p2, s2)
        // result type = decimal(max(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2)) + 1
        int maxIntLength = Math.max(lhsPrecision - lhsScale, rhsPrecision - rhsScale);
        int retPrecision = maxIntLength + Math.max(lhsScale, rhsScale) + 1;
        int retScale = Math.max(lhsScale, rhsScale);
        // precision
        retPrecision = Math.min(retPrecision, 38);
        PrimitiveType decimalType = PrimitiveType.getDecimalPrimitiveType(retPrecision);
        decimalType = PrimitiveType.getWiderDecimalV3Type(decimalType, lhsType.getPrimitiveType());
        decimalType = PrimitiveType.getWiderDecimalV3Type(decimalType, rhsType.getPrimitiveType());

        triple.lhsTargetType = ScalarType.createDecimalV3Type(decimalType, retPrecision, lhsScale);
        triple.rhsTargetType = ScalarType.createDecimalV3Type(decimalType, retPrecision, rhsScale);
        triple.returnType = ScalarType.createDecimalV3Type(decimalType, retPrecision, retScale);
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
        result.lhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, lhsScale);
        result.rhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, rhsScale);
        int returnScale = 0;
        int returnPrecision = 0;
        switch (op) {
            case ADD:
                getAddReturnTypeOfDecimal(result, lhsType, rhsType);
                return result;
            case SUBTRACT:
            case MOD:
                returnScale = Math.max(lhsScale, rhsScale);
                break;
            case MULTIPLY:
                returnScale = lhsScale + rhsScale;
                returnPrecision = lhsPrecision + rhsPrecision;
                final int maxDecimalPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                if (returnPrecision <= maxDecimalPrecision) {
                    // returnPrecision <= 38, result never overflows, use the narrowest decimal type that can holds the result.
                    // for examples:
                    // decimal32(4,3) * decimal32(4,3) => decimal32(8,6);
                    // decimal64(15,3) * decimal32(9,4) => decimal128(24,7).
                    PrimitiveType commonPtype =
                            ScalarType.createDecimalV3NarrowestType(returnPrecision, returnScale).getPrimitiveType();
                    // a common type shall never be narrower than type of lhs and rhs
                    commonPtype = PrimitiveType.getWiderDecimalV3Type(commonPtype, lhsPtype);
                    commonPtype = PrimitiveType.getWiderDecimalV3Type(commonPtype, rhsPtype);
                    result.returnType = ScalarType.createDecimalV3Type(commonPtype, returnPrecision, returnScale);
                    result.lhsTargetType = ScalarType.createDecimalV3Type(commonPtype, lhsPrecision, lhsScale);
                    result.rhsTargetType = ScalarType.createDecimalV3Type(commonPtype, rhsPrecision, rhsScale);
                    return result;
                } else if (returnScale <= maxDecimalPrecision) {
                    // returnPrecision > 38 and returnScale <= 38, the multiplication is computable but the result maybe
                    // overflow, so use decimal128 arithmetic and adopt maximum decimal precision(38) as precision of
                    // the result.
                    // for examples:
                    // decimal128(23,5) * decimal64(18,4) => decimal128(38, 9).
                    result.returnType =
                            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, maxDecimalPrecision, returnScale);
                    result.lhsTargetType =
                            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, lhsPrecision, lhsScale);
                    result.rhsTargetType =
                            ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, rhsPrecision, rhsScale);
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
                maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(widerType);
                result.lhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, lhsScale);
                result.rhsTargetType = ScalarType.createDecimalV3Type(widerType, maxPrecision, rhsScale);
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
                result.lhsTargetType = ScalarType.BIGINT;
                result.rhsTargetType = ScalarType.BIGINT;
                result.returnType = ScalarType.BIGINT;
                return result;
            default:
                Preconditions.checkState(false, "DecimalV3 only support operators: +-*/%&|^");
        }
        result.returnType = op == Operator.INT_DIVIDE ? ScalarType.BIGINT :
                ScalarType.createDecimalV3Type(widerType, maxPrecision, returnScale);
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
        typeTriple.lhsTargetType = Type.DOUBLE;
        typeTriple.rhsTargetType = Type.DOUBLE;
        typeTriple.returnType = Type.DOUBLE;
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
        return toSql();
    }

    @Override
    public Expr clone() {
        return new ArithmeticExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toSql();
        } else {
            return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
        }
    }

    @Override
    protected String explainImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).explain();
        } else {
            return getChild(0).explain() + " " + op.toString() + " " + getChild(1).explain();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        msg.setOpcode(op.getOpcode());
        msg.setOutput_column(outputColumn);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((ArithmeticExpr) obj).opcode == opcode;
    }

    public static Type getCommonType(Type t1, Type t2) {
        PrimitiveType pt1 = t1.getNumResultType().getPrimitiveType();
        PrimitiveType pt2 = t2.getNumResultType().getPrimitiveType();

        if (pt1 == PrimitiveType.DOUBLE || pt2 == PrimitiveType.DOUBLE) {
            return Type.DOUBLE;
        } else if (pt1.isDecimalV3Type() || pt2.isDecimalV3Type()) {
            return ScalarType.getAssigmentCompatibleTypeOfDecimalV3((ScalarType) t1, (ScalarType) t2);
        } else if (pt1 == PrimitiveType.DECIMALV2 || pt2 == PrimitiveType.DECIMALV2) {
            return Type.DECIMALV2;
        } else if (pt1 == PrimitiveType.LARGEINT || pt2 == PrimitiveType.LARGEINT) {
            return Type.LARGEINT;
        } else if (pt1 == PrimitiveType.BIGINT || pt2 == PrimitiveType.BIGINT) {
            return Type.BIGINT;
        } else if ((PrimitiveType.TINYINT.ordinal() <= pt1.ordinal() &&
                pt1.ordinal() <= PrimitiveType.INT.ordinal()) &&
                (PrimitiveType.TINYINT.ordinal() <= pt2.ordinal() &&
                        pt2.ordinal() <= PrimitiveType.INT.ordinal())) {
            return (pt1.ordinal() > pt2.ordinal()) ? t1 : t2;
        } else if (PrimitiveType.TINYINT.ordinal() <= pt1.ordinal() &&
                pt1.ordinal() <= PrimitiveType.INT.ordinal()) {
            // when t2 is INVALID TYPE:
            return t1;
        } else if (PrimitiveType.TINYINT.ordinal() <= pt2.ordinal() &&
                pt2.ordinal() <= PrimitiveType.INT.ordinal()) {
            // when t1 is INVALID TYPE:
            return t2;
        } else {
            return Type.INVALID;
        }
    }

    public static Type getBiggerType(Type t) {
        switch (t.getNumResultType().getPrimitiveType()) {
            case TINYINT:
                return Type.SMALLINT;
            case SMALLINT:
                return Type.INT;
            case INT:
            case BIGINT:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case DOUBLE:
                return Type.DOUBLE;
            case DECIMALV2:
                return Type.DECIMALV2;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return t;
            default:
                return Type.INVALID;
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
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
        return visitor.visitArithmeticExpr(this, context);
    }

    public Operator getOp() {
        return op;
    }

    public enum Operator {
        MULTIPLY("*", "multiply", OperatorPosition.BINARY_INFIX, TExprOpcode.MULTIPLY, true),
        DIVIDE("/", "divide", OperatorPosition.BINARY_INFIX, TExprOpcode.DIVIDE, true),
        MOD("%", "mod", OperatorPosition.BINARY_INFIX, TExprOpcode.MOD, false),
        INT_DIVIDE("DIV", "int_divide", OperatorPosition.BINARY_INFIX, TExprOpcode.INT_DIVIDE, true),
        ADD("+", "add", OperatorPosition.BINARY_INFIX, TExprOpcode.ADD, true),
        SUBTRACT("-", "subtract", OperatorPosition.BINARY_INFIX, TExprOpcode.SUBTRACT, true),
        BITAND("&", "bitand", OperatorPosition.BINARY_INFIX, TExprOpcode.BITAND, false),
        BITOR("|", "bitor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITOR, false),
        BITXOR("^", "bitxor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITXOR, false),
        BITNOT("~", "bitnot", OperatorPosition.UNARY_PREFIX, TExprOpcode.BITNOT, false),
        FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX, TExprOpcode.FACTORIAL, true),
        BIT_SHIFT_LEFT("BITSHIFTLEFT", "bitShiftLeft", OperatorPosition.BINARY_INFIX, TExprOpcode.BIT_SHIFT_LEFT,
                false),
        BIT_SHIFT_RIGHT("BITSHIFTRIGHT", "bitShiftRight", OperatorPosition.BINARY_INFIX, TExprOpcode.BIT_SHIFT_RIGHT,
                false),
        BIT_SHIFT_RIGHT_LOGICAL("BITSHIFTRIGHTLOGICAL", "bitShiftRightLogical", OperatorPosition.BINARY_INFIX,
                TExprOpcode.BIT_SHIFT_RIGHT_LOGICAL, false);

        private final String description;
        private final String name;
        private final OperatorPosition pos;
        private final TExprOpcode opcode;
        private final boolean monotonic;

        Operator(String description, String name, OperatorPosition pos, TExprOpcode opcode, boolean monotonic) {
            this.description = description;
            this.name = name;
            this.pos = pos;
            this.opcode = opcode;
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

        public TExprOpcode getOpcode() {
            return opcode;
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
