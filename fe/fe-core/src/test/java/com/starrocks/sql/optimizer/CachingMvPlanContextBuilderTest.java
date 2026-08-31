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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

public class CachingMvPlanContextBuilderTest extends PlanTestBase {
    private static final String MV_PLAN_CACHE_THREAD_PREFIX = "mv-plan-cache-";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    /**
     * The future returned by submitAsyncTask is meant to be used as an inter-layer barrier on the FE
     * startup path: all activation tasks of one MV layer must be awaited before the next layer starts.
     * This test pins down the contract of that future when the submitted task throws, since the barrier
     * side has to be written against it.
     * <p>
     * Real behaviour, asserted below: it is plain {@link CompletableFuture#supplyAsync} semantics, so a
     * throwing task completes the future EXCEPTIONALLY. The whenComplete() logging stage inside
     * submitAsyncTask does not swallow the failure. A barrier therefore must not use a bare
     * join()/allOf().join() - it has to go through exceptionally()/handle() first.
     * <p>
     * Note this is about submitAsyncTask itself: today's only caller
     * (MaterializedView#checkAndSetActive) wraps its whole task body in try/catch(Throwable), so in
     * production the future completes normally even when activation fails.
     */
    @Test
    public void testSubmitAsyncTaskReturnsFuture() {
        // happy path: the future completes normally and the task runs on the MV plan cache pool
        AtomicReference<String> taskThreadName = new AtomicReference<>();
        CompletableFuture<?> okFuture = CachingMvPlanContextBuilder.submitAsyncTask("ut-ok-task", () -> {
            taskThreadName.set(Thread.currentThread().getName());
            return null;
        });
        Assertions.assertNull(okFuture.join());
        Assertions.assertFalse(okFuture.isCompletedExceptionally());
        Assertions.assertTrue(taskThreadName.get().startsWith(MV_PLAN_CACHE_THREAD_PREFIX),
                "async task should run on the mv plan cache pool, but ran on " + taskThreadName.get());

        // failure path: a throwing task completes the future exceptionally
        CompletableFuture<?> failedFuture = CachingMvPlanContextBuilder.submitAsyncTask("ut-throwing-task", () -> {
            throw new IllegalStateException("ut-injected-failure");
        });
        CompletionException e = Assertions.assertThrows(CompletionException.class, failedFuture::join);
        Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
        Assertions.assertTrue(failedFuture.isCompletedExceptionally());
    }
}
