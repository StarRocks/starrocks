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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Objects;

public class IsNullPredicateOperator extends PredicateOperator {
    private final boolean isNotNull;

    public IsNullPredicateOperator(ScalarOperator arguments) {
        super(OperatorType.IS_NULL, arguments);
        this.isNotNull = false;
    }

    public IsNullPredicateOperator(boolean isNotNull, ScalarOperator arguments) {
        super(OperatorType.IS_NULL, arguments);
        this.isNotNull = isNotNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public String toString() {
        if (!isNotNull) {
            return getChild(0).toString() + " IS NULL";
        } else {
            return getChild(0).toString() + " IS NOT NULL";
        }
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitIsNullPredicate(this, context);
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
        IsNullPredicateOperator that = (IsNullPredicateOperator) o;
        return isNotNull == that.isNotNull;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotNull);
    }
}
