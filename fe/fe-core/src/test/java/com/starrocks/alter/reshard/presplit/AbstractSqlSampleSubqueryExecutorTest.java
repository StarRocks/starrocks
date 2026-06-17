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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractSqlSampleSubqueryExecutorTest {

    /**
     * Regression guard: {@code ConnectContext.setCurrentWarehouseId} delegates to
     * {@code setCurrentWarehouse}, which REPLACES the session-variable object with a fresh
     * warehouse-defaulted one (re-applying only tracked SET variables). The pre-submit-budget
     * {@code query_timeout} is applied via a direct setter (not a tracked SET), so it must be set
     * AFTER the warehouse switch or it is silently dropped — letting an over-budget sample run to
     * the warehouse/default timeout and block the load past the pre-submit budget.
     */
    @Test
    void queryTimeoutIsAppliedAfterWarehouseSwitchSoItSurvives() {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(4242L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 137);

        // The warehouse switch (which swaps the session variable) MUST precede the timeout setter.
        InOrder inOrder = inOrder(context, sessionVariable);
        inOrder.verify(context).setCurrentWarehouseId(4242L);
        inOrder.verify(sessionVariable).setQueryTimeoutS(137);
    }

    @Test
    void nonPositiveQueryTimeoutLeavesSessionTimeoutUntouched() {
        ConnectContext context = mock(ConnectContext.class);
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = mock(ComputeResource.class);
        when(computeResource.getWarehouseId()).thenReturn(7L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                context, computeResource, /*queryTimeoutSeconds=*/ 0);

        verify(context).setCurrentWarehouseId(7L);
        verify(sessionVariable, never()).setQueryTimeoutS(anyInt());
    }
}
