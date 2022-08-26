// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
    public String toString() {
        return columnRef.toString() + (isAsc ? " ASC" : " DESC") + (isNullFirst ? " NULLS FIRST" : " NULLS LAST");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Ordering ordering = (Ordering) o;
        return isAsc == ordering.isAsc && isNullFirst == ordering.isNullFirst &&
                Objects.equals(columnRef, ordering.columnRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRef, isAsc, isNullFirst);
    }
}
