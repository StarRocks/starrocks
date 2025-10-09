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

import com.starrocks.catalog.Type;

import java.util.List;
import java.util.Objects;

public class LargeInPredicateOperator extends InPredicateOperator {
    private final String rawConstantList;
    private final int constantCount;
    private final Type constantType;

    public LargeInPredicateOperator(String rawConstantList,
                                   int constantCount, boolean isNotIn, Type constantType,
                                   List<ScalarOperator> children) {
        super(isNotIn, children.toArray(new ScalarOperator[0]));
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
        this.constantType = constantType;
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    public Type getConstantType() {
        return constantType;
    }

    public ScalarOperator getCompareExpr() {
        return getChild(0);
    }

    @Override
    public String toString() {
        String inClause = isNotIn() ? " NOT IN " : " IN ";
        if (constantCount > 100) {
            return getCompareExpr() + inClause + "(<" + constantCount + " values>)";
        } else {
            return getCompareExpr() + inClause + "(" + rawConstantList + ")";
        }
    }

    @Override
    public boolean equalsSelf(Object obj) {
        if (!super.equalsSelf(obj)) {
            return false;
        }
        LargeInPredicateOperator that = (LargeInPredicateOperator) obj;
        return constantCount == that.constantCount &&
               Objects.equals(rawConstantList, that.rawConstantList) &&
               Objects.equals(constantType, that.constantType);
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(super.hashCodeSelf(), rawConstantList, constantCount, constantType);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }


    @Override
    public boolean allValuesMatch(java.util.function.Predicate<? super ScalarOperator> lambda) {
        return false;
    }

    @Override
    public boolean hasAnyNullValues() {
        return false;
    }
}
