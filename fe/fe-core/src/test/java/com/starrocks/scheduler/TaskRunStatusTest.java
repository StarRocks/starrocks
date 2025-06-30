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

import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import org.junit.Assert;
import org.junit.Test;

public class TaskRunStatusTest {

    @Test
    public void getLastRefreshStateReturnsStateWhenNotMVTask() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.PENDING);
        taskRunStatus.setSource(Constants.TaskSource.CTAS);

        Assert.assertEquals(Constants.TaskRunState.PENDING, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsStateWhenRefreshFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.FAILED);
        taskRunStatus.setSource(Constants.TaskSource.MV);

        Assert.assertEquals(Constants.TaskRunState.FAILED, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsRunningWhenStateIsSuccessAndNotFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.SUCCESS);
        taskRunStatus.setSource(Constants.TaskSource.MV);
        taskRunStatus.setMvTaskRunExtraMessage(new MVTaskRunExtraMessage());
        taskRunStatus.getMvTaskRunExtraMessage().setNextPartitionStart("2023-01-01");

        Assert.assertEquals(Constants.TaskRunState.RUNNING, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsStateWhenStateIsNotSuccessAndNotFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.PENDING);
        taskRunStatus.setSource(Constants.TaskSource.MV);
        taskRunStatus.setMvTaskRunExtraMessage(new MVTaskRunExtraMessage());
        taskRunStatus.getMvTaskRunExtraMessage().setNextPartitionStart("2023-01-01");
        Assert.assertEquals(Constants.TaskRunState.PENDING, taskRunStatus.getLastRefreshState());
    }
}