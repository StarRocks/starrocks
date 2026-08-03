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

package com.starrocks.qe.scheduler.dag;

import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllAtOnceExecutionScheduleTest {

    @Test
    public void testTracerContextUnpinsConnectContextOnPooledWorker() throws Exception {
        AllAtOnceExecutionSchedule schedule = new AllAtOnceExecutionSchedule();
        ConnectContext queryContext = new ConnectContext();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // Simulates a query-deploy worker: no ConnectContext before the task runs.
            Future<Boolean> cleanedUp = executor.submit(() -> {
                assertNull(ConnectContext.get());
                try (AllAtOnceExecutionSchedule.TracerContext ignored =
                        schedule.new TracerContext(Tracers.get(), queryContext)) {
                    assertSame(queryContext, ConnectContext.get());
                }
                // Before the fix the query context stayed pinned in this worker's ThreadLocal.
                return ConnectContext.get() == null;
            });
            assertTrue(cleanedUp.get(), "worker ThreadLocal must not retain the query's ConnectContext");

            // A later task on the same (previously clean) worker must also leave no residue.
            Future<Boolean> secondRun = executor.submit(() -> {
                try (AllAtOnceExecutionSchedule.TracerContext ignored =
                        schedule.new TracerContext(Tracers.get(), queryContext)) {
                    assertSame(queryContext, ConnectContext.get());
                }
                return ConnectContext.get() == null;
            });
            assertTrue(secondRun.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTracerContextRestoresPreviousConnectContext() {
        AllAtOnceExecutionSchedule schedule = new AllAtOnceExecutionSchedule();
        ConnectContext outerContext = new ConnectContext();
        ConnectContext innerContext = new ConnectContext();
        ConnectContext.set(outerContext);
        try {
            try (AllAtOnceExecutionSchedule.TracerContext ignored =
                    schedule.new TracerContext(Tracers.get(), innerContext)) {
                assertSame(innerContext, ConnectContext.get());
            }
            assertSame(outerContext, ConnectContext.get());
        } finally {
            ConnectContext.remove();
        }
    }
}
