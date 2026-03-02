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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TaskBuilderTest {

    @Test
    public void testTaskBuilderForMv() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("aa.bb.cc");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        Task task = TaskBuilder.buildMvTask(mv, "test");
        Assertions.assertEquals("insert overwrite `aa.bb.cc` select * from table1", task.getDefinition());
    }

    @Test
    public void testBuildMvTaskDoesNotPersistWarehouse() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("test.mv");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        // Set a specific warehouse id
        mv.setWarehouseId(1000L);

        Task task = TaskBuilder.buildMvTask(mv, "test");

        // Verify the task does NOT contain warehouse property since it may be changed at runtime
        // via "ALTER MATERIALIZED VIEW SET WAREHOUSE". The warehouse should be fetched from MV
        // in refreshTaskProperties instead.
        Assertions.assertNull(task.getProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE),
                "Warehouse property should not be persisted in task since it can be changed at runtime");

        // Verify MV_ID is still set
        Assertions.assertEquals(String.valueOf(mv.getId()), task.getProperties().get(TaskRun.MV_ID));
    }

    @Test
    public void testRebuildMvTaskPreservesProperties() {
        // mock the warehouse of MaterializedView for creating task
        UtFrameUtils.mockInitWarehouseEnv();

        MaterializedView mv = new MaterializedView();
        mv.setName("test.mv");
        mv.setViewDefineSql("select * from table1");
        mv.setTableProperty(new TableProperty());
        mv.setWarehouseId(1000L);

        // Simulate previous task properties that might contain old warehouse
        java.util.Map<String, String> previousTaskProperties = new java.util.HashMap<>();
        previousTaskProperties.put("custom_property", "custom_value");
        // Note: old warehouse property might exist from before the fix
        previousTaskProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, "old_warehouse");

        Task previousTask = new Task("mv-" + mv.getId());
        previousTask.setProperties(previousTaskProperties);

        Task newTask = TaskBuilder.rebuildMvTask(mv, "test", previousTaskProperties, previousTask);

        // Verify the new task contains the MV_ID
        Assertions.assertEquals(String.valueOf(mv.getId()), newTask.getProperties().get(TaskRun.MV_ID));
        // Verify custom properties are preserved
        Assertions.assertEquals("custom_value", newTask.getProperties().get("custom_property"));
        // Note: rebuildMvTask uses previousTaskProperties as-is, so old warehouse might still exist
        // but refreshTaskProperties will override it with the current MV warehouse at runtime
    }
}
