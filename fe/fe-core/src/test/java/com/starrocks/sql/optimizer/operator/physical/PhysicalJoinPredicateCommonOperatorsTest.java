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

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;

// ScalarOperatorsReuseRule may extract a common sub-expression out of a physical operator's predicate into
// predicateCommonOperators; the predicate then references columns that are defined only there. Every physical join
// constructor must carry predicateCommonOperators together with predicate, so that a rebuilt join (e.g.
// JoinTuningGuide, SkewShuffleJoinEliminationRule) does not keep the predicate while dropping the columns it depends
// on, which would fail InputDependenciesChecker. These tests lock that invariant for all three join subclasses.
class PhysicalJoinPredicateCommonOperatorsTest {

    private static Map<ColumnRefOperator, ScalarOperator> commonOperators() {
        ColumnRefOperator reuseCol = new ColumnRefOperator(100, IntegerType.BIGINT, "cse", true);
        return Map.of(reuseCol, ConstantOperator.createBigint(1L));
    }

    private static ScalarOperator predicate() {
        ColumnRefOperator reuseCol = new ColumnRefOperator(100, IntegerType.BIGINT, "cse", true);
        return new BinaryPredicateOperator(BinaryType.EQ, reuseCol, ConstantOperator.createBigint(1L));
    }

    @Test
    void hashJoinCarriesPredicateCommonOperators() {
        Map<ColumnRefOperator, ScalarOperator> common = commonOperators();
        PhysicalHashJoinOperator join = new PhysicalHashJoinOperator(
                JoinOperator.INNER_JOIN, null, "", Operator.DEFAULT_LIMIT, predicate(), common, null, null, null);
        assertSame(common, join.getPredicateCommonOperators());
    }

    @Test
    void nestLoopJoinCarriesPredicateCommonOperators() {
        Map<ColumnRefOperator, ScalarOperator> common = commonOperators();
        PhysicalNestLoopJoinOperator join = new PhysicalNestLoopJoinOperator(
                JoinOperator.INNER_JOIN, null, "", Operator.DEFAULT_LIMIT, predicate(), common, null);
        assertSame(common, join.getPredicateCommonOperators());
    }

    @Test
    void mergeJoinCarriesPredicateCommonOperators() {
        Map<ColumnRefOperator, ScalarOperator> common = commonOperators();
        PhysicalMergeJoinOperator join = new PhysicalMergeJoinOperator(
                JoinOperator.INNER_JOIN, null, "", Operator.DEFAULT_LIMIT, predicate(), common, null);
        assertSame(common, join.getPredicateCommonOperators());
    }
}
