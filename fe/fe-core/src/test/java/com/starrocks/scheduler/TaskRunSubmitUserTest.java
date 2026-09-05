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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class TaskRunSubmitUserTest {

    private static void mockConnectContext(ConnectContext context) {
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return context;
            }
        };
    }

    private static ConnectContext contextWithUser(String user) {
        ConnectContext context = new ConnectContext();
        context.setQualifiedUser(user);
        return context;
    }

    private static TaskRun taskRunWith(boolean isManual) {
        Task task = new Task("submit_user_test_task");
        ExecuteOption option = new ExecuteOption(task);
        option.setManual(isManual);
        return TaskRunBuilder.newBuilder(task).setExecuteOption(option).build();
    }

    @Test
    public void manualSubmitRecordsSessionUser() {
        mockConnectContext(contextWithUser("bob"));
        TaskRun taskRun = taskRunWith(true);
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        Assertions.assertEquals("bob", taskRun.getStatus().getSubmitUser());
    }

    @Test
    public void automaticRefreshRecordsSystemEvenWithLiveContext() {
        // An on-base-table-change refresh runs on the triggering DML's thread, so ConnectContext is non-null;
        // it must still be attributed to the scheduler, not the incidental DML user.
        mockConnectContext(contextWithUser("bob"));
        TaskRun taskRun = taskRunWith(false);
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        Assertions.assertEquals(TaskRun.SUBMIT_USER_SYSTEM, taskRun.getStatus().getSubmitUser());
    }

    @Test
    public void manualSubmitFallsBackToSystemWithoutContext() {
        mockConnectContext(null);
        TaskRun taskRun = taskRunWith(true);
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        Assertions.assertEquals(TaskRun.SUBMIT_USER_SYSTEM, taskRun.getStatus().getSubmitUser());
    }

    @Test
    public void batchFollowUpInheritsLeaderSubmitterOverContext() {
        mockConnectContext(contextWithUser("not-the-submitter"));
        Task task = new Task("submit_user_test_task");
        ExecuteOption option = new ExecuteOption(task);
        option.setSubmitUser("bob");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).setExecuteOption(option).build();
        taskRun.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        Assertions.assertEquals("bob", taskRun.getStatus().getSubmitUser());
    }
}
