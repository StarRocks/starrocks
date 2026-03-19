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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.WarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
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

    @Test
    public void testGetWarehouseNameForMvTask() {
        // Mock warehouse environment
        UtFrameUtils.mockInitWarehouseEnv();

        // Create an MV and set a specific warehouse
        MaterializedView mv = new MaterializedView();
        mv.setId(10001L);
        mv.setName("test_mv");
        mv.setTableProperty(new TableProperty());
        mv.setWarehouseId(1000L);

        // Create MV task
        Task mvTask = TaskBuilder.buildMvTask(mv, "test_db");

        // The task should not have warehouse in properties (after our fix)
        Assertions.assertNull(mvTask.getProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));

        // But getWarehouseName() should return the default warehouse since MV is not in GlobalStateMgr
        // In real scenario, it would fetch from MV's warehouse
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME, mvTask.getWarehouseName());
    }

    @Test
    public void testMvTaskWithoutWarehouseProperty() {
        // Mock warehouse environment
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setId(10002L);
        mv.setName("test_mv2");
        mv.setTableProperty(new TableProperty());

        // Create MV task - should not have warehouse property
        Task mvTask = TaskBuilder.buildMvTask(mv, "test_db");

        // Verify no warehouse property is stored
        Assertions.assertNull(mvTask.getProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));

        // Verify it's an MV task
        Assertions.assertEquals(Constants.TaskSource.MV, mvTask.getSource());
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
