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

package com.starrocks.qe;

import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.plan.AIInputTokenEstimate;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AIQueryAdmissionTest {
    private ExecPlan plan(AIInputTokenEstimate estimate, long limit) {
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getAIInputTokenEstimate()).thenReturn(estimate);
        when(plan.getAIInputTokenLimit()).thenReturn(limit);
        return plan;
    }

    @Test
    public void testNoAIAndDisabledBudgetDoNotReject() {
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(null));
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(plan(AIInputTokenEstimate.none(), -1)));
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(plan(AIInputTokenEstimate.unknown("missing statistics"), 0)));
    }

    @Test
    public void testExactBudgetAndZeroEstimatePass() {
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.TEN), 10)));
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.ZERO), 1)));
    }

    @Test
    public void testOverBudgetRejectsWithoutRetryErrorCode() {
        StarRocksException failure = Assertions.assertThrows(StarRocksException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.valueOf(11)), 10)));
        Assertions.assertTrue(failure.getMessage().contains("exceeds"));
        Assertions.assertEquals(InternalErrorCode.INTERNAL_ERR, failure.getInternalErrorCode());
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, null, null, mock(QueryStatement.class));
        Assertions.assertSame(failure, Assertions.assertThrows(StarRocksException.class,
                () -> ExecuteExceptionHandler.handle(failure, retryContext)));
    }

    @Test
    public void testUnknownRejectsEnabledBudget() {
        StarRocksException failure = Assertions.assertThrows(StarRocksException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.unknown("missing statistics"), 10)));
        Assertions.assertTrue(failure.getMessage().contains("unknown"));
        Assertions.assertTrue(failure.getMessage().contains("missing statistics"));
    }

    @Test
    public void testNegativeBudgetRejectsAI() {
        Assertions.assertThrows(StarRocksException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.ZERO), -1)));
    }

    @Test
    public void testEstimateAboveLongRangeCannotWrap() {
        Assertions.assertThrows(StarRocksException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)), Long.MAX_VALUE)));
    }
}
