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
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals("insert overwrite `aa.bb.cc` select * from table1", task.getDefinition());
    }
}
