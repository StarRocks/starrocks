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
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TaskTest {

    @Test
    public void testDeserialize() {
        Task task = GsonUtils.GSON.fromJson("{}", Task.class);
        Assertions.assertEquals(Constants.TaskSource.CTAS, task.getSource());
        Assertions.assertEquals(AuthenticationMgr.ROOT_USER, task.getCreateUser());
        Assertions.assertEquals(Constants.TaskState.UNKNOWN, task.getState());
        Assertions.assertEquals(Constants.TaskType.MANUAL, task.getType());
    }

    @Test
    public void testTaskRunState() {
        Assertions.assertFalse(Constants.TaskRunState.PENDING.isFinishState());
        Assertions.assertFalse(Constants.TaskRunState.RUNNING.isFinishState());
        Assertions.assertTrue(Constants.TaskRunState.FAILED.isFinishState());
        Assertions.assertTrue(Constants.TaskRunState.SUCCESS.isFinishState());
    }

    @Test
    public void testGetWarehouseName() {
        Task task = new Task();
        Assertions.assertEquals(task.getWarehouseName(), WarehouseManager.DEFAULT_WAREHOUSE_NAME);

        Map<String, String> properties = new HashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, "aaa");
        task.setProperties(properties);
        Assertions.assertEquals(task.getWarehouseName(), "aaa");
    }

    public void testConstantTaskState() {
        // whether it's a finished state
        Assertions.assertEquals(true, Constants.TaskRunState.FAILED.isFinishState());
        Assertions.assertEquals(true, Constants.TaskRunState.MERGED.isFinishState());
        Assertions.assertEquals(true, Constants.TaskRunState.SUCCESS.isFinishState());
        Assertions.assertEquals(false, Constants.TaskRunState.PENDING.isFinishState());
        Assertions.assertEquals(false, Constants.TaskRunState.RUNNING.isFinishState());
        // whether it's a success state
        Assertions.assertEquals(false, Constants.TaskRunState.FAILED.isSuccessState());
        Assertions.assertEquals(true, Constants.TaskRunState.MERGED.isSuccessState());
        Assertions.assertEquals(true, Constants.TaskRunState.SUCCESS.isSuccessState());
        Assertions.assertEquals(false, Constants.TaskRunState.PENDING.isSuccessState());
        Assertions.assertEquals(false, Constants.TaskRunState.RUNNING.isSuccessState());
    }
}
