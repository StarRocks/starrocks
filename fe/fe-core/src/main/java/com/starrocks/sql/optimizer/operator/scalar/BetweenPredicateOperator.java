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

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class BetweenPredicateOperator extends PredicateOperator {

    private final boolean notBetween;

    public BetweenPredicateOperator(boolean notBetween, ScalarOperator... arguments) {
        super(OperatorType.BETWEEN, arguments);
        this.notBetween = notBetween;
        Preconditions.checkState(arguments.length == 3);
    }

    public BetweenPredicateOperator(boolean notBetween, List<ScalarOperator> arguments) {
        super(OperatorType.BETWEEN, arguments);
        this.notBetween = notBetween;
        Preconditions.checkState(arguments != null && arguments.size() == 3);
    }

    public boolean isNotBetween() {
        return notBetween;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).toString()).append(" ");

        if (isNotBetween()) {
            sb.append("NOT ");
        }

        sb.append("BETWEEN ");
        sb.append(getChild(1)).append(" AND ").append(getChild(2));
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).debugString()).append(" ");

        if (isNotBetween()) {
            sb.append("NOT ");
        }

        sb.append("BETWEEN ");
        sb.append(getChild(1)).append(" AND ").append(getChild(2));
        return sb.toString();
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
        BetweenPredicateOperator that = (BetweenPredicateOperator) o;
        return notBetween == that.notBetween;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), notBetween);
    }
}
