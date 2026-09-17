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

package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskRunIdentityFailureTest {
    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testContextConstructionFailureCompletesAsFailed() throws Exception {
        TaskRun run = new TaskRun() {
            @Override
            public Constants.TaskRunState executeTaskRun() {
                new ConnectContext().setThreadLocalInfo();
                throw new SemanticException("Task execution identity is no longer valid");
            }
        };
        run.setTask(new Task("identity_failure"));
        run.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        TaskRunExecutor executor = new TaskRunExecutor();
        try {
            assertTrue(executor.executeTaskRun(run));
            assertEquals(Constants.TaskRunState.FAILED, run.getFuture().get(10, TimeUnit.SECONDS));
            assertTrue(run.getStatus().getErrorMessage().contains("Task execution identity is no longer valid"));
            assertTrue(run.getStatus().getFinishTime() > 0);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10000));
        }
    }
}
