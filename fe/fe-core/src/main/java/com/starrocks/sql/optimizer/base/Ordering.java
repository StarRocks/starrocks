// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Objects;

public class Ordering {
    private final ColumnRefOperator columnRef;
    private final boolean isAsc;
    private final boolean isNullFirst;

    public Ordering(ColumnRefOperator columnRef, boolean isAsc, boolean isNullFirst) {
        this.columnRef = columnRef;
        this.isAsc = isAsc;
        this.isNullFirst = isNullFirst;
    }

    public ColumnRefOperator getColumnRef() {
        return columnRef;
    }

    public boolean isAscending() {
        return isAsc;
    }

    public boolean isNullsFirst() {
        return isNullFirst;
    }

    public boolean matches(Ordering rhs) {
        return columnRef.equals(rhs.columnRef) &&
                isAsc == rhs.isAsc &&
                isNullFirst == rhs.isNullFirst;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRef, isAsc, isNullFirst);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }

        if (!(object instanceof Ordering)) {
            return false;
        }

        return matches((Ordering) object);
    }

    @Override
    public String toString() {
        return columnRef.toString() + (isAsc ? " ASC" : " DESC") + (isNullFirst ? " NULLS FIRST" : " NULLS LAST");
    }
}
