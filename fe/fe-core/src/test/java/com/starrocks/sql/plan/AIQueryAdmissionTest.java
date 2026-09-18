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

package com.starrocks.sql.plan;

import com.starrocks.qe.ExecuteExceptionHandler;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AIQueryAdmissionTest {
    private ExecPlan plan(AIInputTokenEstimate estimate) {
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getAIInputTokenEstimate()).thenReturn(estimate);
        return plan;
    }

    @Test
    public void testNoAIAndDisabledBudgetDoNotReject() {
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(plan(AIInputTokenEstimate.none()), 1));
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(plan(AIInputTokenEstimate.unknown("missing statistics")), 0));
    }

    @Test
    public void testExactBudgetAndZeroEstimatePass() {
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.TEN)), 10));
        Assertions.assertDoesNotThrow(() -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.ZERO)), 1));
    }

    @Test
    public void testOverBudgetRejectsWithoutRetryErrorCode() {
        StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.valueOf(11))), 10));
        Assertions.assertTrue(failure.getMessage().contains("Exceeded"));
        Assertions.assertEquals(ErrorType.USER_ERROR, failure.getType());
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, null, null, mock(QueryStatement.class));
        Assertions.assertSame(failure, Assertions.assertThrows(StarRocksPlannerException.class,
                () -> ExecuteExceptionHandler.handle(failure, retryContext)));
    }

    @Test
    public void testUnknownRejectsEnabledBudget() {
        StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.unknown("missing statistics")), 10));
        Assertions.assertTrue(failure.getMessage().contains("UNKNOWN"));
        Assertions.assertTrue(failure.getMessage().contains("missing statistics"));
    }

    @Test
    public void testEstimateAboveLongRangeCannotWrap() {
        Assertions.assertThrows(StarRocksPlannerException.class, () -> AIQueryAdmission.check(
                plan(AIInputTokenEstimate.estimated(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE))), Long.MAX_VALUE));
    }
}
