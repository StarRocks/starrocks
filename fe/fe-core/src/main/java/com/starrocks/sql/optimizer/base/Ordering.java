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
