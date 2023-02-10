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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.BitSet;
import java.util.List;

// TODO: now depends on equal
// This will be extended to rollup(like date_trunc)
public class GroupKeyChecker {
    private final List<ScalarOperator> mvGroupKeys;
    private final BitSet matchingBits;

    public GroupKeyChecker(List<ScalarOperator> mvGroupKeys) {
        this.mvGroupKeys = mvGroupKeys;
        this.matchingBits = new BitSet(mvGroupKeys.size());
    }

    public boolean isRollup() {
        return matchingBits.cardinality() < mvGroupKeys.size();
    }

    // true if matched, or false
    public boolean check(List<ScalarOperator> groupKeys) {
        GroupKeyCheckVisitor visitor = new GroupKeyCheckVisitor();
        for (ScalarOperator key : groupKeys) {
            boolean matched = key.accept(visitor, null);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private class GroupKeyCheckVisitor extends ScalarOperatorVisitor<Boolean, Object> {
        @Override
        public Boolean visit(ScalarOperator scalarOperator, Object context) {
            for (int i = 0; i < mvGroupKeys.size(); i++) {
                if (mvGroupKeys.get(i).equals(scalarOperator)) {
                    matchingBits.set(i);
                    return true;
                }
            }
            return false;
        }
    }
}
