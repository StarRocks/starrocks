// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/BinaryPredicate.java

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
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.analyzer.ExprVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Most predicates with two operands..
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class BinaryPredicate extends Predicate implements Writable {
    private static final Logger LOG = LogManager.getLogger(BinaryPredicate.class);

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_RANGE_PREDICATE =
            arg -> arg.getOp() == Operator.LT
                    || arg.getOp() == Operator.LE
                    || arg.getOp() == Operator.GT
                    || arg.getOp() == Operator.GE;

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_PREDICATE =
            arg -> arg.getOp() == Operator.EQ;

    // true if this BinaryPredicate is inferred from slot equivalences, false otherwise.
    private boolean isInferred_ = false;

    public enum Operator {
        EQ("=", "eq", TExprOpcode.EQ, true),
        NE("!=", "ne", TExprOpcode.NE, true),
        LE("<=", "le", TExprOpcode.LE, true),
        GE(">=", "ge", TExprOpcode.GE, true),
        LT("<", "lt", TExprOpcode.LT, true),
        GT(">", "gt", TExprOpcode.GT, true),
        EQ_FOR_NULL("<=>", "eq_for_null", TExprOpcode.EQ_FOR_NULL, true);

        private final String description;
        private final String name;
        private final TExprOpcode opcode;
        private final boolean monotonic;

        Operator(String description,
                 String name,
                 TExprOpcode opcode,
                 boolean monotonic) {
            this.description = description;
            this.name = name;
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

        public TExprOpcode getOpcode() {
            return opcode;
        }

        public Operator commutative() {
            switch (this) {
                case EQ:
                    return this;
                case NE:
                    return this;
                case LE:
                    return GE;
                case GE:
                    return LE;
                case LT:
                    return GT;
                case GT:
                    return LE;
                case EQ_FOR_NULL:
                    return this;
            }
            return null;
        }

        public Operator converse() {
            switch (this) {
                case EQ:
                    return EQ;
                case NE:
                    return NE;
                case LE:
                    return GE;
                case GE:
                    return LE;
                case LT:
                    return GT;
                case GT:
                    return LT;
                case EQ_FOR_NULL:
                    return EQ_FOR_NULL;
                // case DISTINCT_FROM: return DISTINCT_FROM;
                // case NOT_DISTINCT: return NOT_DISTINCT;
                // case NULL_MATCHING_EQ:
                // throw new IllegalStateException("Not implemented");
                default:
                    throw new IllegalStateException("Invalid operator");
            }
        }

        public boolean isEquivalence() {
            return this == EQ || this == EQ_FOR_NULL;
        }

        public boolean isMonotonic() {
            return monotonic;
        }

        public boolean isUnequivalence() {
            return this == NE;
        }
    }

    private Operator op;
    // check if left is slot and right isnot slot.
    private Boolean slotIsleft = null;

    // for restoring
    public BinaryPredicate() {
        super();
    }

    public BinaryPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        this.opcode = op.opcode;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
        slotIsleft = other.slotIsleft;
        isInferred_ = other.isInferred_;
    }

    public boolean isInferred() {
        return isInferred_;
    }

    public void setIsInferred() {
        isInferred_ = true;
    }

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull() || t.isPseudoType()) {
                continue; // NULL is handled through type promotion.
            }
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.EQ.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.NE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.LE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.GE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.LT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.GT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.EQ_FOR_NULL.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
        }
    }

    @Override
    public Expr clone() {
        return new BinaryPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public Expr negate() {
        Operator newOp = null;
        switch (op) {
            case EQ:
                newOp = Operator.NE;
                break;
            case NE:
                newOp = Operator.EQ;
                break;
            case LT:
                newOp = Operator.GE;
                break;
            case LE:
                newOp = Operator.GT;
                break;
            case GE:
                newOp = Operator.LT;
                break;
            case GT:
                newOp = Operator.LE;
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }
        return new BinaryPredicate(newOp, getChild(0), getChild(1));
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((BinaryPredicate) obj).opcode == this.opcode;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    public String explainImpl() {
        return getChild(0).explain() + " " + op.toString() + " " + getChild(1).explain();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(opcode);
        msg.setVector_opcode(vectorOpcode);
        msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
    }

    @Override
    public void vectorizedAnalyze(Analyzer analyzer) {
        super.vectorizedAnalyze(analyzer);
        Function match = null;

        try {
            match = getBuiltinFunction(analyzer, op.name, collectChildReturnTypes(),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } catch (AnalysisException e) {
            Preconditions.checkState(false);
        }
        Preconditions.checkState(match != null);
        Preconditions.checkState(match.getReturnType().getPrimitiveType() == PrimitiveType.BOOLEAN);
        //todo(dhc): should add oppCode
        //this.vectorOpcode = match.opcode;
        LOG.debug(debugString() + " opcode: " + vectorOpcode);
    }

    private static boolean canCompareDate(PrimitiveType t1, PrimitiveType t2) {
        if (t1.isDateType()) {
            return t2.isDateType() || t2.isStringType();
        } else if (t2.isDateType()) {
            return t1.isStringType();
        } else {
            return false;
        }
    }

    public static Type getCmpType(Type type1, Type type2) {
        PrimitiveType t1 = type1.getResultType().getPrimitiveType();
        PrimitiveType t2 = type2.getResultType().getPrimitiveType();

        if (canCompareDate(type1.getPrimitiveType(), type2.getPrimitiveType())) {
            return Type.DATETIME;
        }

        // Following logical is compatible with MySQL:
        //    Cast to DOUBLE by default, because DOUBLE has the largest range of values.
        if (t1 == PrimitiveType.VARCHAR && t2 == PrimitiveType.VARCHAR) {
            return Type.VARCHAR;
        }
        if (t1 == PrimitiveType.BIGINT && t2 == PrimitiveType.BIGINT) {
            return Type.getAssignmentCompatibleType(type1, type2, false);
        }
        if (t1.isDecimalV3Type()) {
            return Type.getAssignmentCompatibleType(type1, type2, false);
        }
        if (t2.isDecimalV3Type()) {
            return Type.getAssignmentCompatibleType(type1, type2, false);
        }
        if ((t1 == PrimitiveType.BIGINT || t1 == PrimitiveType.DECIMALV2)
                && (t2 == PrimitiveType.BIGINT || t2 == PrimitiveType.DECIMALV2)) {
            return Type.DECIMALV2;
        }
        if ((t1 == PrimitiveType.BIGINT || t1 == PrimitiveType.LARGEINT)
                && (t2 == PrimitiveType.BIGINT || t2 == PrimitiveType.LARGEINT)) {
            return Type.LARGEINT;
        }

        return Type.DOUBLE;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);

        for (Expr expr : children) {
            if (expr instanceof Subquery) {
                Subquery subquery = (Subquery) expr;
                if (!subquery.returnsScalarColumn()) {
                    String msg = "Subquery of binary predicate must return a single column: " + expr.toSql();
                    throw new AnalysisException(msg);
                }
                /**
                 * Situation: The expr is a binary predicate and the type of subquery is not scalar type.
                 * Add assert: The stmt of subquery is added an assert condition (return error if row count > 1).
                 * Input params:
                 *     expr: k1=(select k1 from t2)
                 *     subquery stmt: select k1 from t2
                 * Output params:
                 *     new expr: k1 = (select k1 from t2 (assert row count: return error if row count > 1 ))
                 *     subquery stmt: select k1 from t2 (assert row count: return error if row count > 1 )
                 */
                if (!subquery.getType().isScalarType()) {
                    subquery.getStatement().setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.LE);
                }
            }
        }

        // if children has subquery, it will be rewritten and reanalyzed in the future.
        if (contains(Subquery.class)) {
            return;
        }

        // decimal_col = "string literal" is rewritten into decimal_col = cast("string literal" as decimal);
        if (getChild(0).getType().isDecimalV3() && getChild(1) instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) getChild(1);
            setChild(1, stringLiteral.castToNontypedNumericLiteral());
        }

        // "string literal" = decimal_col is rewritten into cast("string literal" as decimal) = decimal_col;
        if (getChild(1).getType().isDecimalV3() && getChild(0) instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) getChild(0);
            setChild(0, stringLiteral.castToNontypedNumericLiteral());
        }

        Type cmpType = getCmpType(getChild(0).getType(), getChild(1).getType());

        if (getChild(0).getType().isDate() && getChild(1) instanceof LiteralExpr
                && isValidDate((LiteralExpr) getChild(1))) {
            cmpType = Type.DATE;
        } else if (getChild(1).getType().isDate() && getChild(0) instanceof LiteralExpr
                && isValidDate((LiteralExpr) getChild(0))) {
            cmpType = Type.DATE;
        } else if (getChild(0) instanceof SlotRef &&
                getChild(0).getType().getPrimitiveType() == PrimitiveType.DECIMAL32 &&
                getChild(1) instanceof LiteralExpr
                && isValidDecimal((LiteralExpr) (getChild(1)), getChild(0).getType())) {
            cmpType = getChild(0).getType();
        } else if (getChild(1) instanceof SlotRef &&
                getChild(1).getType().getPrimitiveType() == PrimitiveType.DECIMAL32 &&
                getChild(0) instanceof LiteralExpr
                && isValidDecimal((LiteralExpr) (getChild(0)), getChild(1).getType())) {
            cmpType = getChild(1).getType();
        }

        // Ignore return value because type is always bool for predicates.
        castBinaryOp(cmpType);

        this.opcode = op.getOpcode();
        String opName = op.getName();
        fn = getBuiltinFunction(analyzer, opName, collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
        Preconditions.checkArgument(fn != null, String.format("Unsupported binary predicate %s", toSql()));

        // determine selectivity
        Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
        if (op == Operator.EQ && isSingleColumnPredicate(slotRefRef,
                null) && slotRefRef.getRef().getNumDistinctValues() > 0) {
            Preconditions.checkState(slotRefRef.getRef() != null);
            selectivity = 1.0 / slotRefRef.getRef().getNumDistinctValues();
            selectivity = Math.max(0, Math.min(1, selectivity));
        } else {
            // TODO: improve using histograms, once they show up
            selectivity = Expr.DEFAULT_SELECTIVITY;
        }
    }

    public boolean isValidDate(LiteralExpr expr) {
        try {
            new DateLiteral(expr.getStringValue(), Type.DATE);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    public boolean isValidDecimal(LiteralExpr expr, Type type) {
        try {
            new DecimalLiteral(expr.getStringValue(), type);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    /**
     * If predicate is of the form "<slotref> <op> <expr>", returns expr,
     * otherwise returns null. Slotref may be wrapped in a CastExpr.
     */
    public Expr getSlotBinding(SlotId id) {
        SlotRef slotRef = null;
        // check left operand
        if (getChild(0) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(0);
        } else if (getChild(0) instanceof CastExpr && getChild(0).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(0)).canHashPartition()) {
                slotRef = (SlotRef) getChild(0).getChild(0);
            }
        }
        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = true;
            return getChild(1);
        }

        // check right operand
        if (getChild(1) instanceof SlotRef) {
            slotRef = (SlotRef) getChild(1);
        } else if (getChild(1) instanceof CastExpr && getChild(1).getChild(0) instanceof SlotRef) {
            if (((CastExpr) getChild(1)).canHashPartition()) {
                slotRef = (SlotRef) getChild(1).getChild(0);
            }
        }

        if (slotRef != null && slotRef.getSlotId() == id) {
            slotIsleft = false;
            return getChild(0);
        }

        return null;
    }

    /**
     * If e is an equality predicate between two slots that only require implicit
     * casts, returns those two slots; otherwise returns null.
     */
    public static Pair<SlotId, SlotId> getEqSlots(Expr e) {
        if (!(e instanceof BinaryPredicate)) {
            return null;
        }
        return ((BinaryPredicate) e).getEqSlots();
    }

    /**
     * If this is an equality predicate between two slots that only require implicit
     * casts, returns those two slots; otherwise returns null.
     */
    @Override
    public Pair<SlotId, SlotId> getEqSlots() {
        if (op != Operator.EQ) {
            return null;
        }
        SlotRef lhs = getChild(0).unwrapSlotRef(true);
        if (lhs == null) {
            return null;
        }
        SlotRef rhs = getChild(1).unwrapSlotRef(true);
        if (rhs == null) {
            return null;
        }
        return new Pair<>(lhs.getSlotId(), rhs.getSlotId());
    }

    public boolean slotIsLeft() {
        Preconditions.checkState(slotIsleft != null);
        return slotIsleft;
    }

    /*
     * the follow persistence code is only for TableFamilyDeleteInfo.
     * Maybe useless
     */
    @Override
    public void write(DataOutput out) throws IOException {
        boolean isWritable = true;
        Expr left = this.getChild(0);
        if (!(left instanceof SlotRef)) {
            isWritable = false;
        }

        Expr right = this.getChild(1);
        if (!(right instanceof StringLiteral)) {
            isWritable = false;
        }

        if (isWritable) {
            out.writeInt(1);
            // write op
            Text.writeString(out, op.name());
            // write left
            Text.writeString(out, ((SlotRef) left).getColumnName());
            // write right
            Text.writeString(out, ((StringLiteral) right).getStringValue());
        } else {
            out.writeInt(0);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int isWritable = in.readInt();
        if (isWritable == 0) {
            return;
        }

        // read op
        Operator op = Operator.valueOf(Text.readString(in));
        // read left
        SlotRef left = new SlotRef(null, Text.readString(in));
        // read right
        StringLiteral right = new StringLiteral(Text.readString(in));

        this.op = op;
        this.addChild(left);
        this.addChild(right);
    }

    public static BinaryPredicate read(DataInput in) throws IOException {
        BinaryPredicate binaryPredicate = new BinaryPredicate();
        binaryPredicate.readFields(in);
        return binaryPredicate;
    }

    @Override
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr leftChildValue = getChild(0);
        final Expr rightChildValue = getChild(1);
        if (!(leftChildValue instanceof LiteralExpr)
                || !(rightChildValue instanceof LiteralExpr)) {
            return this;
        }
        return compareLiteral((LiteralExpr) leftChildValue, (LiteralExpr) rightChildValue);
    }

    private Expr compareLiteral(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        final boolean isFirstNull = (first instanceof NullLiteral);
        final boolean isSecondNull = (second instanceof NullLiteral);
        if (op == Operator.EQ_FOR_NULL) {
            if (isFirstNull && isSecondNull) {
                return new BoolLiteral(true);
            } else if (isFirstNull || isSecondNull) {
                return new BoolLiteral(false);
            }
        } else {
            if (isFirstNull || isSecondNull) {
                return new NullLiteral();
            }
        }

        final int compareResult = first.compareLiteral(second);
        switch (op) {
            case EQ:
            case EQ_FOR_NULL:
                return new BoolLiteral(compareResult == 0);
            case GE:
                return new BoolLiteral(compareResult == 1 || compareResult == 0);
            case GT:
                return new BoolLiteral(compareResult == 1);
            case LE:
                return new BoolLiteral(compareResult == -1 || compareResult == 0);
            case LT:
                return new BoolLiteral(compareResult == -1);
            case NE:
                return new BoolLiteral(compareResult != 0);
            default:
                Preconditions.checkState(false, "No defined binary operator.");
        }
        return this;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public boolean isVectorized() {
        for (Expr expr : children) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    public boolean isNullable() {
        return op != Operator.EQ_FOR_NULL;
    }

    @Override
    public boolean isStrictPredicate() {
        if (op == Operator.EQ_FOR_NULL) {
            return false;
        }
        // To exclude 1 = 1;
        return getChild(0).unwrapSlotRef() != null || getChild(1).unwrapSlotRef() != null;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }
}
