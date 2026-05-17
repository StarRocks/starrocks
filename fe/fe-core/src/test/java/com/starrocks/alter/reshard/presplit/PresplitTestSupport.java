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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;

/**
 * Shared test fixtures for the presplit package. Centralizes the
 * cross-test-class helpers so the contract changes in one place when the
 * underlying types evolve (e.g., when {@link ScanContext} stops being an
 * empty marker).
 */
final class PresplitTestSupport {

    static final ScanContext DUMMY_CONTEXT = new ScanContext() { };

    private PresplitTestSupport() {
    }

    static Column bigintColumn(String name) {
        return new Column(name, IntegerType.BIGINT);
    }

    static Column varcharColumn(String name) {
        return new Column(name, VarcharType.VARCHAR);
    }

    static Tuple bigintTuple(long value) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(value))));
    }

    static List<Variant> bigintRow(long value) {
        return List.of(Variant.of(IntegerType.BIGINT, Long.toString(value)));
    }

    static Tuple compositeTuple(String tenant, long position) {
        return new Tuple(List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position))));
    }

    static List<Variant> compositeRow(String tenant, long position) {
        return List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position)));
    }

    /**
     * Wraps {@code invocation} with a {@code MockedStatic} so a hook test can
     * assert the hook never reached
     * {@link TabletPreSplitCoordinator#submitAsynchronously}. "No throw" alone
     * is too weak — every hook swallows internal throws by design.
     */
    static void assertHookDoesNotDelegate(HookInvocation invocation) throws Exception {
        try (MockedStatic<TabletPreSplitCoordinator> coordinator =
                     Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            invocation.run();
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    /** Functional-interface signature for {@link #assertHookDoesNotDelegate} lambdas. */
    @FunctionalInterface
    interface HookInvocation {
        void run() throws Exception;
    }
}
