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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class MvColumnRefSubstitutorTest {

    @Test
    void substituteWidensSumAfterLeafSwap() {
        // Original: sum(SMALLINT k3). MV column: BIGINT mv_sum_k3.
        // After substitution + re-derive: sum's argType must be BIGINT.
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.SMALLINT, "k3", true);
        ColumnRefOperator mvSumK3 = new ColumnRefOperator(101, IntegerType.BIGINT, "mv_sum_k3", true);
        Function origSum = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(origSum, "sum(SMALLINT) builtin must exist");
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), origSum);

        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(k3, mvSumK3);
        Optional<ScalarOperator> out = MvColumnRefSubstitutor.substitute(sum, map);

        Assertions.assertTrue(out.isPresent(), "substitute should succeed for sum(SMALLINT) -> sum(BIGINT)");
        CallOperator newSum = (CallOperator) out.get();
        Assertions.assertEquals(IntegerType.BIGINT, newSum.getFunction().getArgs()[0]);
        Assertions.assertEquals(IntegerType.BIGINT, newSum.getType());
    }

    @Test
    void substituteReturnsEmptyWhenReDeriveFails() {
        // Bogus function name → resolveFunction fails → TypeReDeriveException → Optional.empty().
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.SMALLINT, "k3", true);
        ColumnRefOperator mv = new ColumnRefOperator(101, IntegerType.BIGINT, "mv", true);
        CallOperator bogus = new CallOperator("not_a_real_function_xyz_999", IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), null);
        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(k3, mv);

        Optional<ScalarOperator> out = MvColumnRefSubstitutor.substitute(bogus, map);
        Assertions.assertFalse(out.isPresent(), "substitute should return empty when re-derive throws");
    }

    @Test
    void substituteAndSyncOutputUpdatesOutputRefType() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.SMALLINT, "k3", true);
        ColumnRefOperator mvSumK3 = new ColumnRefOperator(101, IntegerType.BIGINT, "mv_sum_k3", true);
        ColumnRefOperator outRef = new ColumnRefOperator(200, IntegerType.SMALLINT, "out_sum", true);
        Function origSum = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), origSum);

        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(k3, mvSumK3);
        Optional<ScalarOperator> out = MvColumnRefSubstitutor.substituteAndSyncOutput(outRef, sum, map);

        Assertions.assertTrue(out.isPresent());
        Assertions.assertEquals(IntegerType.BIGINT, outRef.getType(),
                "output ref type must follow rewritten expression");
    }
}
