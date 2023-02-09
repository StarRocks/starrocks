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

import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

public class AggregateRewriteChecker {
    private final List<ScalarOperator> targetAggregates;
    private boolean distinct;

    public AggregateRewriteChecker(List<ScalarOperator> targetAggregates) {
        this.targetAggregates = targetAggregates;
        this.distinct = false;
    }

    // true if all matched, or false
    public boolean check(List<ScalarOperator> srcAggregates) {
        AggregateCheckVisitor visitor = new AggregateCheckVisitor();
        for (ScalarOperator agg : srcAggregates) {
            boolean matched = agg.accept(visitor, null);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    public boolean hasDistinct() {
        return distinct;
    }

    private class AggregateCheckVisitor extends ScalarOperatorVisitor<Boolean, Void> {
        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            // Aggregate must be CallOperator
            return isMatched(scalarOperator);
        }

        @Override
        public Boolean visitCall(CallOperator callOperator, Void context) {
            // Aggregate must be CallOperator
            if (callOperator.isDistinct()) {
                distinct = true;
            }
            return isMatched(callOperator);
        }

        boolean isMatched(ScalarOperator scalarOperator) {
            return targetAggregates.contains(scalarOperator);
        }
    }
}
