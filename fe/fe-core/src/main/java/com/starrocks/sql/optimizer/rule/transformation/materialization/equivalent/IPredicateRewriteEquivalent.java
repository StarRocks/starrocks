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

package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

/**
 * Two expression can be equivalent even they are not the same expressions especially for partition datetime functions,
 * example1:
 *   a. time_slice(dt, INTERVAL 1 HOUR) >= '2023-11-21 00:00:00'
 *   b. dt >= '2023-11-21 00:00:00'
 * example2:
 *   a. date_trunc('hour', dt) >= '2023-11-21 00:00:00'
 *   b. dt >= '2023-11-21 00:00:00'
 *
 * {@code IRewriteEquivalent} is used to check whether different expression are equivalent or not.
 */
public abstract class IPredicateRewriteEquivalent implements IRewriteEquivalent {
    public RewriteEquivalentType getRewriteEquivalentType() {
        return RewriteEquivalentType.PREDICATE;
    }

    /**
     * @param operator : The input scalar operator eg:date_trunc/time_slice expression
     * @param constantOperator : const operator of the binary predicate's right child
     * @return : The input operator can be replaced by the {@code IRewriteEquivalent}
     */
    abstract boolean isEquivalent(ScalarOperator operator, ConstantOperator constantOperator);
}
