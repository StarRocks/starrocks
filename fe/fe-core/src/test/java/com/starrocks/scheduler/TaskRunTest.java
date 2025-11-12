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

import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskRunTest {

    private TaskRun taskRun;

    @BeforeEach
    public void setUp() {
        taskRun = new TaskRun();
    }

    @Test
    public void testDefaultTimeout() {
        taskRun.setProperties(null);
        assertEquals(Config.task_runs_timeout_second, taskRun.getExecuteTimeoutS());
    }

    @Test
    public void testQueryTimeoutProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(SessionVariable.QUERY_TIMEOUT, "120");
        taskRun.setProperties(props);
        assertEquals(Math.max(120, Config.task_runs_timeout_second), taskRun.getExecuteTimeoutS());
    }

    @Test
    public void testInsertTimeoutProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(SessionVariable.INSERT_TIMEOUT, "200");
        taskRun.setProperties(props);
        assertEquals(Math.max(200, Config.task_runs_timeout_second), taskRun.getExecuteTimeoutS());
    }

    @Test
    public void testInvalidTimeoutValue() {
        Map<String, String> props = new HashMap<>();
        props.put(SessionVariable.QUERY_TIMEOUT, "invalid");
        taskRun.setProperties(props);
        assertEquals(Config.task_runs_timeout_second, taskRun.getExecuteTimeoutS());
    }

    @Test
    public void testNegativeTimeoutValue() {
        Map<String, String> props = new HashMap<>();
        props.put(SessionVariable.QUERY_TIMEOUT, "-10");
        taskRun.setProperties(props);
        assertEquals(Config.task_runs_timeout_second, taskRun.getExecuteTimeoutS());
    }
}