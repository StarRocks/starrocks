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

package com.starrocks.qe.scheduler;

import com.starrocks.common.conf.Config;
import com.starrocks.common.logging.Log4jConfig;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionDAGTest extends SchedulerTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.qe.CoordinatorPreprocessor"};
        Log4jConfig.initLogging();

        SchedulerTestBase.beforeClass();
    }

    @Test
    public void testValidateExecutionDAG() {
        String sql = "select /*+SET_VAR(single_node_exec_plan=true)*/ " +
                "t1.l_orderkey from lineitem t1 join lineitem t2 using(l_orderkey)";
        Assert.assertThrows("This sql plan has multi result sinks", StarRocksPlannerException.class, () -> startScheduling(sql));
    }
}
