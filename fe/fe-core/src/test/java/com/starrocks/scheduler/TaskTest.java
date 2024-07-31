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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

public class TaskTest {

    @Test
    public void testDeserialize() {
        Task task = GsonUtils.GSON.fromJson("{}", Task.class);
        Assert.assertEquals(Constants.TaskSource.CTAS, task.getSource());
        Assert.assertEquals(AuthenticationMgr.ROOT_USER, task.getCreateUser());
        Assert.assertEquals(Constants.TaskState.UNKNOWN, task.getState());
        Assert.assertEquals(Constants.TaskType.MANUAL, task.getType());
    }

    @Test
    public void testTaskRunState() {
        Assert.assertFalse(Constants.TaskRunState.PENDING.isFinishState());
        Assert.assertFalse(Constants.TaskRunState.RUNNING.isFinishState());
        Assert.assertTrue(Constants.TaskRunState.FAILED.isFinishState());
        Assert.assertTrue(Constants.TaskRunState.SUCCESS.isFinishState());
    }

    @Test
    public void testConstantTaskState() {
        // whether it's a finished state
        Assert.assertEquals(true, Constants.TaskRunState.FAILED.isFinishState());
        Assert.assertEquals(true, Constants.TaskRunState.MERGED.isFinishState());
        Assert.assertEquals(true, Constants.TaskRunState.SUCCESS.isFinishState());
        Assert.assertEquals(false, Constants.TaskRunState.PENDING.isFinishState());
        Assert.assertEquals(false, Constants.TaskRunState.RUNNING.isFinishState());
        // whether it's a success state
        Assert.assertEquals(false, Constants.TaskRunState.FAILED.isSuccessState());
        Assert.assertEquals(true, Constants.TaskRunState.MERGED.isSuccessState());
        Assert.assertEquals(true, Constants.TaskRunState.SUCCESS.isSuccessState());
        Assert.assertEquals(false, Constants.TaskRunState.PENDING.isSuccessState());
        Assert.assertEquals(false, Constants.TaskRunState.RUNNING.isSuccessState());
    }
}