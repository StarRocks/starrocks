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

package com.starrocks.load.loadv2;

import com.starrocks.alter.reshard.presplit.BrokerLoadPreSplitHook;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Direct coverage for {@link BrokerLoadJob#firePreSplitHooks} — the
 * package-private hook-firing helper extracted from {@code createLoadingTask}
 * so the persisted-opt-out + bindScope + per-table delegation can be
 * exercised without standing up the full {@code BrokerLoadJob} fixture.
 */
public class BrokerLoadJobPreSplitFiringTest {

    @Test
    public void testEmptyInputsSkipsEverything() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Database db = Mockito.mock(Database.class);
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);

        try (MockedStatic<BrokerLoadPreSplitHook> hookStatic =
                     Mockito.mockStatic(BrokerLoadPreSplitHook.class)) {
            BrokerLoadJob.firePreSplitHooks(
                    context, db, brokerDesc, computeResource, List.of(), Map.of());

            hookStatic.verifyNoInteractions();
            verifyNoInteractions(context);
        }
    }

    @Test
    public void testSingleInputFiresHookWithoutPersistedOptOut() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        ConnectContext.ScopeGuard scopeGuard = Mockito.mock(ConnectContext.ScopeGuard.class);
        Mockito.when(context.bindScope()).thenReturn(scopeGuard);

        Database db = Mockito.mock(Database.class);
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        OlapTable targetTable = Mockito.mock(OlapTable.class);
        List<BrokerFileGroup> fileGroups = List.of(Mockito.mock(BrokerFileGroup.class));
        List<List<TBrokerFileStatus>> fileStatuses = List.of(List.of());
        BrokerLoadJob.PreSplitHookInput input =
                new BrokerLoadJob.PreSplitHookInput(targetTable, fileGroups, fileStatuses);

        try (MockedStatic<BrokerLoadPreSplitHook> hookStatic =
                     Mockito.mockStatic(BrokerLoadPreSplitHook.class)) {
            BrokerLoadJob.firePreSplitHooks(
                    context, db, brokerDesc, computeResource, List.of(input), Map.of());

            // No persisted opt-out → session variable left untouched.
            verify(sessionVariable, never()).setEnableTabletPreSplit(Mockito.anyBoolean());
            // Hook fired once, against the load's bound context.
            verify(context, times(1)).bindScope();
            hookStatic.verify(() -> BrokerLoadPreSplitHook.maybeRunPreSplit(
                    eq(context), eq(db), eq(targetTable), eq(brokerDesc),
                    eq(fileGroups), eq(fileStatuses), eq(computeResource)));
        }
    }

    @Test
    public void testPersistedOptOutFalseAppliedBeforeHookFires() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        ConnectContext.ScopeGuard scopeGuard = Mockito.mock(ConnectContext.ScopeGuard.class);
        Mockito.when(context.bindScope()).thenReturn(scopeGuard);

        Database db = Mockito.mock(Database.class);
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        BrokerLoadJob.PreSplitHookInput input = new BrokerLoadJob.PreSplitHookInput(
                Mockito.mock(OlapTable.class), List.of(), List.of());

        try (MockedStatic<BrokerLoadPreSplitHook> hookStatic =
                     Mockito.mockStatic(BrokerLoadPreSplitHook.class)) {
            BrokerLoadJob.firePreSplitHooks(
                    context, db, brokerDesc, computeResource,
                    List.of(input),
                    Map.of(SessionVariable.ENABLE_TABLET_PRE_SPLIT, "false"));

            // Persisted opt-out should be re-applied to the recreated context
            // so the load's submit-time SET survives FE failover.
            verify(sessionVariable).setEnableTabletPreSplit(false);
            hookStatic.verify(() -> BrokerLoadPreSplitHook.maybeRunPreSplit(
                    any(), any(), any(), any(), any(), any(), any()));
        }
    }

    @Test
    public void testMultipleInputsFireHookPerTable() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        ConnectContext.ScopeGuard scopeGuard = Mockito.mock(ConnectContext.ScopeGuard.class);
        Mockito.when(context.bindScope()).thenReturn(scopeGuard);

        Database db = Mockito.mock(Database.class);
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        BrokerLoadJob.PreSplitHookInput firstInput = new BrokerLoadJob.PreSplitHookInput(
                Mockito.mock(OlapTable.class), List.of(), List.of());
        BrokerLoadJob.PreSplitHookInput secondInput = new BrokerLoadJob.PreSplitHookInput(
                Mockito.mock(OlapTable.class), List.of(), List.of());

        try (MockedStatic<BrokerLoadPreSplitHook> hookStatic =
                     Mockito.mockStatic(BrokerLoadPreSplitHook.class)) {
            BrokerLoadJob.firePreSplitHooks(
                    context, db, brokerDesc, computeResource,
                    List.of(firstInput, secondInput), Map.of());

            hookStatic.verify(() -> BrokerLoadPreSplitHook.maybeRunPreSplit(
                    any(), any(), any(), any(), any(), any(), any()), times(2));
        }
    }
}
