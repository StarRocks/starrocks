// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BinaryPredicateOperator extends PredicateOperator {
    private static final Map<BinaryType, BinaryType> BINARY_COMMUTATIVE_MAP =
            ImmutableMap.<BinaryType, BinaryType>builder()
                    .put(BinaryType.EQ, BinaryType.EQ)
                    .put(BinaryType.NE, BinaryType.NE)
                    .put(BinaryType.LE, BinaryType.GE)
                    .put(BinaryType.LT, BinaryType.GT)
                    .put(BinaryType.GE, BinaryType.LE)
                    .put(BinaryType.GT, BinaryType.LT)
                    .put(BinaryType.EQ_FOR_NULL, BinaryType.EQ_FOR_NULL)
                    .build();

    private static final Map<BinaryType, BinaryType> BINARY_NEGATIVE_MAP =
            ImmutableMap.<BinaryType, BinaryType>builder()
                    .put(BinaryType.EQ, BinaryType.NE)
                    .put(BinaryType.NE, BinaryType.EQ)
                    .put(BinaryType.LE, BinaryType.GT)
                    .put(BinaryType.LT, BinaryType.GE)
                    .put(BinaryType.GE, BinaryType.LT)
                    .put(BinaryType.GT, BinaryType.LE)
                    .put(BinaryType.EQ_FOR_NULL, BinaryType.NE)
                    .build();

    private final BinaryType type;

    public BinaryPredicateOperator(BinaryType type, ScalarOperator... arguments) {
        super(OperatorType.BINARY, arguments);
        this.type = type;
        Preconditions.checkState(arguments.length == 2);
    }

    public BinaryPredicateOperator(BinaryType type, List<ScalarOperator> arguments) {
        super(OperatorType.BINARY, arguments);
        this.type = type;
        Preconditions.checkState(arguments.size() == 2);
    }

    public BinaryType getBinaryType() {
        return type;
    }

    public void swap() {
        ScalarOperator c0 = getChild(0);
        ScalarOperator c1 = getChild(1);
        setChild(0, c1);
        setChild(1, c0);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    public enum BinaryType {
        EQ("="),
        NE("!="),
        LE("<="),
        GE(">="),
        LT("<"),
        GT(">"),
        EQ_FOR_NULL("<=>");

        private final String type;

        BinaryType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        public boolean isEqual() {
            return type.equals(EQ.type);
        }

        public boolean isNotEqual() {
            return type.equals(NE.type);
        }

        public boolean isEquivalence() {
            return this == EQ || this == EQ_FOR_NULL;
        }

        public boolean isUnequivalence() {
            return this == NE;
        }

        public boolean isNotRangeComparison() {
            return isEquivalence() || isUnequivalence();
        }

        public boolean isRange() {
            return type.equals(LT.type)
                    || type.equals(LE.type)
                    || type.equals(GT.type)
                    || type.equals(GE.type);
        }
    }

    public BinaryPredicateOperator commutative() {
        return new BinaryPredicateOperator(BINARY_COMMUTATIVE_MAP.get(this.getBinaryType()),
                this.getChild(1),
                this.getChild(0));
    }

    public BinaryPredicateOperator negative() {
        return new BinaryPredicateOperator(BINARY_NEGATIVE_MAP.get(this.getBinaryType()), this.getChildren());
    }

    @Override
    public String toString() {
        return getChild(0).toString() + " " + type.toString() + " " + getChild(1).toString();
    }

    @Override
    public String debugString() {
        return getChild(0).debugString() + " " + type.toString() + " " + getChild(1).debugString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BinaryPredicateOperator that = (BinaryPredicateOperator) o;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type);
    }

    @Override
    public boolean isNullable() {
        return !this.type.equals(BinaryType.EQ_FOR_NULL) && super.isNullable();
    }
}
