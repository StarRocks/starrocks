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

public class LikePredicateOperator extends PredicateOperator {
    private final LikeType likeType;

    public LikePredicateOperator(ScalarOperator... arguments) {
        super(OperatorType.LIKE, arguments);
        this.likeType = LikeType.LIKE;
        Preconditions.checkState(arguments.length == 2);
    }

    public LikePredicateOperator(LikeType likeType, ScalarOperator... arguments) {
        super(OperatorType.LIKE, arguments);
        this.likeType = likeType;
        Preconditions.checkState(arguments.length == 2);
    }

    public LikePredicateOperator(LikeType likeType, List<ScalarOperator> arguments) {
        super(OperatorType.LIKE, arguments);
        this.likeType = likeType;
        Preconditions.checkState(arguments != null && arguments.size() == 2);
    }

    public enum LikeType {
        LIKE,
        REGEXP
    }

    public boolean isRegexp() {
        return LikeType.REGEXP.equals(this.likeType);
    }

    public LikeType getLikeType() {
        return likeType;
    }

    @Override
    public String toString() {
        if (LikeType.LIKE.equals(likeType)) {
            return getChild(0).toString() + " LIKE " + getChild(1).toString();
        }

        return getChild(0).toString() + " REGEXP " + getChild(1).toString();
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLikePredicateOperator(this, context);
    }

    @Override
    public String debugString() {
        if (LikeType.LIKE.equals(likeType)) {
            return getChild(0).debugString() + " LIKE " + getChild(1).debugString();
        }

        return getChild(0).debugString() + " REGEXP " + getChild(1).debugString();
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
        LikePredicateOperator that = (LikePredicateOperator) o;
        return likeType == that.likeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), likeType);
    }
}
