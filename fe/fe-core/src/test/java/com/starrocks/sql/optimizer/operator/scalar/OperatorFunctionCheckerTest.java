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

import com.starrocks.common.Pair;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OperatorFunctionCheckerTest {
    private static CastOperator cast(Type from, Type to) {
        return new CastOperator(to, new ColumnRefOperator(1, from, "c1", true));
    }

    /**
     * A cast between strings and numbers reorders values: '99845' sorts after '998425506019' while
     * 99845 is far below 998425506019. Mapping a range predicate through it prunes partitions that
     * hold matching rows, so it is not monotonic.
     */
    @Test
    public void testNonOrderPreservingCastIsNotMonotonic() {
        Pair<Boolean, String> result =
                OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(VarcharType.VARCHAR, IntegerType.BIGINT));
        assertFalse(result.first);
        assertTrue(result.second.contains("cast"), result.second);

        assertFalse(OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(IntegerType.BIGINT, IntegerType.INT)).first,
                "a narrowing integer cast wraps or saturates");
    }

    @Test
    public void testOrderPreservingCastIsMonotonic() {
        assertTrue(OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(IntegerType.INT, IntegerType.BIGINT)).first);
        assertTrue(OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(DateType.DATE, DateType.DATETIME)).first);
        assertTrue(OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(DateType.DATETIME, DateType.DATE)).first);
        assertTrue(OperatorFunctionChecker.onlyContainMonotonicFunctions(cast(IntegerType.BIGINT, IntegerType.BIGINT)).first);
    }

    /**
     * The order a cast puts values in is a question about monotonicity alone. Equality maps soundly
     * through any deterministic function, and the callers license the equality rewrite off the
     * FE-constant check (see ListPartitionPruner.deduceExtraConjuncts, which gates on FE-constant
     * first and only demands monotonicity for the non-equality case). So the FE-constant check must
     * accept a cast the monotonicity check rejects -- otherwise `c1 = '99845'` stops deducing
     * `c2 = 99845` for a generated column `c2 AS cast(c1 as bigint)` and every partition is scanned,
     * and an MV retention condition containing such a cast is rejected outright by
     * PartitionSelector.validateRetentionConditionPredicate.
     */
    @Test
    public void testFEConstantCheckAcceptsAnyCast() {
        assertTrue(OperatorFunctionChecker.onlyContainFEConstantFunctions(cast(VarcharType.VARCHAR, IntegerType.BIGINT)).first);
        assertTrue(OperatorFunctionChecker.onlyContainFEConstantFunctions(cast(IntegerType.BIGINT, IntegerType.INT)).first);
        assertTrue(OperatorFunctionChecker.onlyContainFEConstantFunctions(cast(VarcharType.VARCHAR, DateType.DATE)).first);
    }
}
