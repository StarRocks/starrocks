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

import java.util.ArrayList;

public class HashCachedCompoundPredicateOperator extends PredicateOperator {
    public CompoundPredicateOperator operator;
    private int hashValue = 0;

    public HashCachedCompoundPredicateOperator(CompoundPredicateOperator operator) {
        super(OperatorType.COMPOUND, new ArrayList<>());
        this.operator = operator;
    }

    public int getHashValue() {
        return hashValue;
    }

    public void setHashValue(int hashValue) {
        this.hashValue = hashValue;
    }

    public CompoundPredicateOperator getOperator() {
        return operator;
    }

    @Override
    public int hashCode() {
        if (this.getHashValue() != 0) {
            return this.getHashValue();
        }

        this.setHashValue(operator.hashCode());
        return this.getHashValue();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashCachedCompoundPredicateOperator that = (HashCachedCompoundPredicateOperator) o;
        return this.getOperator() == that.getOperator();
    }
}
