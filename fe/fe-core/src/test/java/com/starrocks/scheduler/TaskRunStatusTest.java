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

import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TaskRunStatusTest {

    @Test
    public void testGetWarehouseNameReturnsDefaultWhenPropertiesNull() {
        TaskRunStatus status = new TaskRunStatus();
        // properties is null by default
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME, status.getWarehouseName());
    }

    @Test
    public void testGetWarehouseNameReturnsDefaultWhenWarehouseKeyAbsent() {
        TaskRunStatus status = new TaskRunStatus();
        status.setProperties(new HashMap<>());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME, status.getWarehouseName());
    }

    @Test
    public void testGetWarehouseNameReturnsValueFromProperties() {
        TaskRunStatus status = new TaskRunStatus();
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, "my_warehouse");
        status.setProperties(props);
        Assertions.assertEquals("my_warehouse", status.getWarehouseName());
    }

    @Test
    public void getLastRefreshStateReturnsStateWhenNotMVTask() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.PENDING);
        taskRunStatus.setSource(Constants.TaskSource.CTAS);

        Assertions.assertEquals(Constants.TaskRunState.PENDING, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsStateWhenRefreshFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.FAILED);
        taskRunStatus.setSource(Constants.TaskSource.MV);

        Assertions.assertEquals(Constants.TaskRunState.FAILED, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsRunningWhenStateIsSuccessAndNotFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.SUCCESS);
        taskRunStatus.setSource(Constants.TaskSource.MV);
        taskRunStatus.setMvTaskRunExtraMessage(new MVTaskRunExtraMessage());
        taskRunStatus.getMvTaskRunExtraMessage().setNextPartitionStart("2023-01-01");

        Assertions.assertEquals(Constants.TaskRunState.RUNNING, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void getLastRefreshStateReturnsStateWhenStateIsNotSuccessAndNotFinished() {
        TaskRunStatus taskRunStatus = new TaskRunStatus();
        taskRunStatus.setState(Constants.TaskRunState.PENDING);
        taskRunStatus.setSource(Constants.TaskSource.MV);
        taskRunStatus.setMvTaskRunExtraMessage(new MVTaskRunExtraMessage());
        taskRunStatus.getMvTaskRunExtraMessage().setNextPartitionStart("2023-01-01");
        Assertions.assertEquals(Constants.TaskRunState.PENDING, taskRunStatus.getLastRefreshState());
    }

    @Test
    public void testSourceIsUnknownWhenAbsentFromPersistedJson() {
        // Runs persisted before the source field existed have no "source" key and must load as UNKNOWN,
        // so task_runs surfaces them as UNKNOWN rather than a misleading CTAS.
        String legacyJson = "{\"queryId\":\"q1\",\"taskName\":\"t1\",\"state\":\"SUCCESS\"}";
        TaskRunStatus status = GsonUtils.GSON.fromJson(legacyJson, TaskRunStatus.class);
        Assertions.assertEquals(Constants.TaskSource.UNKNOWN, status.getSource());
    }

    @Test
    public void getLastRefreshStateTreatsUnknownSourceAsNonMv() {
        // A legacy run carries an UNKNOWN source; getLastRefreshState must treat it as a non-MV run.
        TaskRunStatus status = new TaskRunStatus();
        status.setState(Constants.TaskRunState.SUCCESS);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, status.getLastRefreshState());
    }
}