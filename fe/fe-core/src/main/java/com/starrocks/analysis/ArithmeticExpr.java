// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class ArithmeticExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(ArithmeticExpr.class);
    private final Operator op;

    public enum OperatorPosition {
        BINARY_INFIX,
        UNARY_PREFIX,
        UNARY_POSTFIX,
    }

    public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
        super();
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

    public static TypeTriple getReturnTypeOfDecimal(Operator op, ScalarType lhsType, ScalarType rhsType)
            throws AnalysisException {
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
                    throw new AnalysisException(
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
                    throw new AnalysisException(
                            String.format(
                                    "Dividend fails to adjust scale to %d that exceeds maximum value(%d)",
                                    adjustedScale,
                                    maxPrecision));
                }
                break;
            case BITAND:
            case BITOR:
            case BITXOR:
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

    private void rewriteDecimalDecimalOperation() throws AnalysisException {
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
        TypeTriple triple = getReturnTypeOfDecimal(op, (ScalarType) lhsTargetType, (ScalarType) rhsTargetType);
        if (!triple.lhsTargetType.equals(lhsOriginType)) {
            Preconditions.checkState(triple.lhsTargetType.isValid());
            castChild(triple.lhsTargetType, 0);
        }
        if (!triple.rhsTargetType.equals(rhsOriginType)) {
            Preconditions.checkState(triple.rhsTargetType.isValid());
            castChild(triple.rhsTargetType, 1);
        }
        type = triple.returnType;
    }

    private void rewriteDecimalFloatingPointOperation() throws AnalysisException {
        Type lhsType = getChild(0).type;
        Type rhsType = getChild(1).type;
        Type resultType = Type.DOUBLE;
        if (!resultType.equals(lhsType)) {
            castChild(resultType, 0);
        }
        if (!resultType.equals(rhsType)) {
            castChild(resultType, 1);
        }
        this.type = resultType;
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
                return true;
            default:
                return false;
        }
    }

    public void rewriteDecimalOperation() throws AnalysisException {
        if (hasFloatingPointOrStringType() && !resultTypeIsBigInt()) {
            rewriteDecimalFloatingPointOperation();
        } else {
            rewriteDecimalDecimalOperation();
        }
    }

    private void rewriteNonDecimalOperation() throws AnalysisException {
        Type t1 = getChild(0).getType().getNumResultType();
        Type t2 = getChild(1).getType().getNumResultType();
        // Find result type of this operator
        Type commonType = Type.INVALID;
        switch (op) {
            case MULTIPLY:
            case ADD:
            case SUBTRACT:
            case MOD:
                // numeric ops must be promoted to highest-resolution type
                // (otherwise we can't guarantee that a <op> b won't overflow/underflow)
                commonType = getCommonType(t1, t2);
                break;
            case DIVIDE:
                commonType = getCommonType(t1, t2);
                if (commonType.getPrimitiveType() == PrimitiveType.BIGINT
                        || commonType.getPrimitiveType() == PrimitiveType.LARGEINT) {
                    commonType = Type.DOUBLE;
                }
                break;
            case INT_DIVIDE:
            case BITAND:
            case BITOR:
            case BITXOR:
                // Must be bigint
                commonType = Type.BIGINT;
                break;
            default:
                // the programmer forgot to deal with a case
                Preconditions.checkState(false,
                        "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
                break;
        }
        if (getChild(0).getType().isNull() && getChild(1).getType().isNull()) {
            commonType = Type.NULL;
        }
        type = castBinaryOp(commonType);
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
    public String toDigestImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toDigest();
        } else {
            return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
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

    @Override
    public void computeOutputColumn(Analyzer analyzer) {
        super.computeOutputColumn(analyzer);

        List<TupleId> tupleIds = Lists.newArrayList();
        getIds(tupleIds, null);
        Preconditions.checkArgument(tupleIds.size() == 1);
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
        // bitnot is the only unary op, deal with it here
        if (op == Operator.BITNOT) {
            type = Type.BIGINT;
            if (getChild(0).getType().getPrimitiveType() != PrimitiveType.BIGINT) {
                castChild(type, 0);
            }
            fn = getBuiltinFunction(
                    analyzer, op.getName(), collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
            if (fn == null) {
                Preconditions.checkState(false, String.format("No match for op with operand types", toSql()));
            }
            return;
        }

        analyzeSubqueryInChildren();
        // if children has subquery, it will be rewritten and reanalyzed in the future.
        if (contains(Subquery.class)) {
            return;
        }

        Type t1 = getChild(0).getType();
        Type t2 = getChild(1).getType();
        if (t1.isDecimalV3() || t2.isDecimalV3()) {
            rewriteDecimalOperation();
        } else {
            rewriteNonDecimalOperation();
        }
        String fnName = op.name;
        fn = getBuiltinFunction(analyzer, fnName, collectChildReturnTypes(),
                Function.CompareMode.IS_INDISTINGUISHABLE);
        if (fn == null) {
            Preconditions.checkState(false, String.format(
                    "No match for '%s' with operand types %s and %s", toSql(), t1, t2));
        }
    }

    public void analyzeSubqueryInChildren() throws AnalysisException {
        for (Expr child : children) {
            if (child instanceof Subquery) {
                Subquery subquery = (Subquery) child;
                if (!subquery.returnsScalarColumn()) {
                    String msg = "Subquery of arithmetic expr must return a single column: " + child.toSql();
                    throw new AnalysisException(msg);
                }
                /**
                 * Situation: The expr is a binary predicate and the type of subquery is not scalar type.
                 * Add assert: The stmt of subquery is added an assert condition (return error if row count > 1).
                 * Input params:
                 *     expr: 0.9*(select k1 from t2)
                 *     subquery stmt: select k1 from t2
                 * Output params:
                 *     new expr: 0.9 * (select k1 from t2 (assert row count: return error if row count > 1 ))
                 *     subquery stmt: select k1 from t2 (assert row count: return error if row count > 1 )
                 */
                if (!subquery.getType().isScalarType()) {
                    subquery.getStatement().setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.LE);
                }
            }
        }
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
        FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX, TExprOpcode.FACTORIAL, true);

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

        public boolean isUnary() {
            return pos == OperatorPosition.UNARY_PREFIX
                    || pos == OperatorPosition.UNARY_POSTFIX;
        }

        public boolean isBinary() {
            return pos == OperatorPosition.BINARY_INFIX;
        }

        public boolean isMonotonic() {
            return monotonic;
        }
    }

    public static class TypeTriple {
        ScalarType returnType;
        ScalarType lhsTargetType;
        ScalarType rhsTargetType;
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }
}
