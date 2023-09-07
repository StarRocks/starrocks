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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

// A class for range predicate in mv rewrite used to simplify range predicates compensation
// The range could be just ranges for one column, or for compound 'and' or 'or'.
// eg:
// a > 10 and a < 10000 will  generate ColumnRangePredicate a-> (10, 1000)
// a > 10 and b < 1000 will generate AndRangePredicate: a -> (10, +∞) and b -> (-∞, 1000)
// a > 10 or b < 1000 will generate OrRangePredicate: a -> (10, +∞) or b -> (-∞, 1000)
public class RangePredicate {
    protected List<RangePredicate> childPredicates;

    public RangePredicate(List<RangePredicate> childPredicates) {
        this.childPredicates = childPredicates;
    }

    public RangePredicate() {
    }

    public List<RangePredicate> getChildPredicates() {
        return childPredicates;
    }

    // check whether this range predicate encloses other
    // eg: a > 10 enclose a > 20
    // a > 10 and a < 100 does not enclose a> 1 and a < 1000
    public boolean enclose(RangePredicate other) {
        return false;
    }

    public <T extends RangePredicate> T cast() {
        return (T) this;
    }

    // convert RangePredicate to ScalarOperator
    public ScalarOperator toScalarOperator() {
        return null;
    }

    // simplify this range predicate against other
    // eg:
    // this: a < 10, other: a < 10, will return ConstantOperator.TRUE
    // this: a < 10, other: a < 20, will return a < 10
    public ScalarOperator simplify(RangePredicate other) {
        return null;
    }
}
