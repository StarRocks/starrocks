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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

/**
 * Two expression can be equivalent even they are not the same expressions especially for partition datetime functions,
 *  <p>
 * example1:
 *   a. time_slice(dt, INTERVAL 1 HOUR) >= '2023-11-21 00:00:00'
 *   b. dt >= '2023-11-21 00:00:00'
 * example2:
 *   a. date_trunc('hour', dt) >= '2023-11-21 00:00:00'
 *   b. dt >= '2023-11-21 00:00:00'
 *  </p>
 * {@code IRewriteEquivalent} is used to check whether different expression are equivalent or not.
 */

public interface IRewriteEquivalent {
    /**
     * Different {@code RewriteEquivalentType} will be used to rewrite different scalar operators.
     * eg:
     *  `PREDICATE` will be used to rewrite for `BinaryPredicateOperator`;
     *  `CallOperator` will be used to rewrite for `BinaryPredicateOperator`;
     */
    enum RewriteEquivalentType {
        PREDICATE,
        AGGREGATE
    }

    class RewriteEquivalentContext {
        // input or newInput should always have an equivalent, record it in the prepare stage.
        // eg:
        //  reference : time_slice(dt, 1 hour interval) = '2023-11-23 00:00:00'
        //  newInput  : dt = '2023-11-23 00:00:00',
        // then equivalent is `dt`
        private final ScalarOperator equivalent;
        private final ScalarOperator input;

        private ColumnRefOperator replace;

        public RewriteEquivalentContext(ScalarOperator equivalent, ScalarOperator input) {
            this.equivalent = equivalent;
            this.input = input;
        }

        public ScalarOperator getEquivalent() {
            return equivalent;
        }

        public ColumnRefOperator getReplace() {
            return replace;
        }

        public void setReplace(ColumnRefOperator replace) {
            this.replace = replace;
        }

        public ScalarOperator getInput() {
            return input;
        }
    }

    RewriteEquivalentType getRewriteEquivalentType();

    /**
     * Check input scalar operator can be used for equivalent compare.
     * @param input : scalar operator that can be used for generating other equivalents.
     * @return      : true if it can be used, otherwise false.
     */
    RewriteEquivalentContext prepare(ScalarOperator input);

    /**
     * Rewrite newInput if it is equivalent with input by using input's column ref mapping replace.
     * @param input         : original scalar operator.
     * @param replace       : original scalar operator's mapping column ref.
     * @param newInput      : the new scalar operator to rewrite.
     * @return              : return a new scalar operator that is rewritten by newInput and replace.
     */
    ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                           EquivalentShuttleContext shuttleContext,
                           ColumnRefOperator replace,
                           ScalarOperator newInput);
}
