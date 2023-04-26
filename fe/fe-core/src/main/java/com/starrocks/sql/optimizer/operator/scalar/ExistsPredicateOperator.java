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

import java.util.List;
import java.util.Objects;

public class ExistsPredicateOperator extends PredicateOperator {
    private final boolean isNotExists;

    public ExistsPredicateOperator(boolean isNotExists, ScalarOperator... arguments) {
        super(OperatorType.EXISTS, arguments);
        this.isNotExists = isNotExists;
    }

    public ExistsPredicateOperator(boolean isNotExists, List<ScalarOperator> arguments) {
        super(OperatorType.EXISTS, arguments);
        this.isNotExists = isNotExists;
    }

    public boolean isNotExists() {
        return isNotExists;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitExistsPredicate(this, context);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        if (isNotExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).toString());
        return strBuilder.toString();
    }

    @Override
    public String debugString() {
        StringBuilder strBuilder = new StringBuilder();
        if (isNotExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).debugString());
        return strBuilder.toString();
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
        ExistsPredicateOperator that = (ExistsPredicateOperator) o;
        return isNotExists == that.isNotExists;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotExists);
    }
}
