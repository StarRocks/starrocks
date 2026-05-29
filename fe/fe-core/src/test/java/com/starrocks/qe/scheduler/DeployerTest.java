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

package com.starrocks.qe.scheduler;

import com.starrocks.common.ThreadPoolManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeployerTest {

    private ThreadPoolExecutor executor;

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testResolveMaxThreadPoolSizeFallsBackToCpuCores() {
        // -1 means "use cpu cores"; the helper guarantees at least cpuCores threads.
        int cpuCores = ThreadPoolManager.cpuCores();
        assertEquals(cpuCores, Deployer.resolveMaxThreadPoolSize(-1));
        assertEquals(cpuCores, Deployer.resolveMaxThreadPoolSize(0));
        assertEquals(Math.max(cpuCores, 1024), Deployer.resolveMaxThreadPoolSize(1024));
    }

    @Test
    public void testClampMinThreadPoolSize() {
        assertEquals(1, Deployer.clampMinThreadPoolSize(-5, 8));
        assertEquals(1, Deployer.clampMinThreadPoolSize(0, 8));
        assertEquals(4, Deployer.clampMinThreadPoolSize(4, 8));
        assertEquals(8, Deployer.clampMinThreadPoolSize(8, 8));
        // Configured min above max gets clamped to max so setCorePoolSize never exceeds it.
        assertEquals(8, Deployer.clampMinThreadPoolSize(100, 8));
    }

    @Test
    public void testResizeExecutorExpand() {
        executor = newExecutor(2, 4);

        Deployer.resizeExecutor(executor, 6, 12);

        assertEquals(12, executor.getMaximumPoolSize());
        assertEquals(6, executor.getCorePoolSize());
    }

    @Test
    public void testResizeExecutorShrink() {
        executor = newExecutor(4, 8);

        Deployer.resizeExecutor(executor, 1, 2);

        assertEquals(2, executor.getMaximumPoolSize());
        assertEquals(1, executor.getCorePoolSize());
    }

    @Test
    public void testResizeExecutorShrinkBelowCurrentCore() {
        // Current core (4) is larger than the new max (2). Must lower core first.
        executor = newExecutor(4, 8);

        Deployer.resizeExecutor(executor, 2, 2);

        assertEquals(2, executor.getMaximumPoolSize());
        assertEquals(2, executor.getCorePoolSize());
    }

    @Test
    public void testResizeExecutorOnlyMinChanges() {
        executor = newExecutor(2, 8);

        Deployer.resizeExecutor(executor, 5, 8);

        assertEquals(8, executor.getMaximumPoolSize());
        assertEquals(5, executor.getCorePoolSize());
    }

    @Test
    public void testResizeExecutorNoChange() {
        executor = newExecutor(2, 8);

        Deployer.resizeExecutor(executor, 2, 8);

        assertEquals(8, executor.getMaximumPoolSize());
        assertEquals(2, executor.getCorePoolSize());
    }

    private static ThreadPoolExecutor newExecutor(int core, int max) {
        return new ThreadPoolExecutor(core, max, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(16));
    }
}