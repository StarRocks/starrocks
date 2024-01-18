// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;

import java.util.Objects;

public class CastOperator extends CallOperator {
    private final boolean isImplicit;

    public CastOperator(Type returnType, ScalarOperator args) {
        super("cast", returnType, Lists.newArrayList(args));
        this.isImplicit = false;
    }

    public CastOperator(Type returnType, ScalarOperator args, boolean isImplicit) {
        super("cast", returnType, Lists.newArrayList(args));
        this.isImplicit = isImplicit;
    }

    public Type fromType() {
        return getChild(0).getType();
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public String toString() {
        if (getType().isDecimalOfAnyVersion()) {
            return "cast(" + getChild(0).toString() + " as " + getType() + ")";
        } else {
            return "cast(" + getChild(0).toString() + " as " + getType().toSql() + ")";
        }
    }

    @Override
    public String debugString() {
        return "cast(" + getChild(0).debugString() + " as " + getType().toSql() + ")";
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
        CastOperator that = (CastOperator) o;
        return isImplicit == that.isImplicit && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isImplicit, type);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCastOperator(this, context);
    }
}
