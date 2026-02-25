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

import com.starrocks.common.Config;
import com.starrocks.common.Log4jConfig;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExecutionDAGTest extends SchedulerTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.qe.CoordinatorPreprocessor"};
        Log4jConfig.initLogging();

        SchedulerTestBase.beforeClass();
    }

    @Test
    public void testValidateExecutionDAG() {
        String sql = "select /*+SET_VAR(single_node_exec_plan=true)*/ " +
                "t1.l_orderkey from lineitem t1 join lineitem t2 using(l_orderkey)";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> startScheduling(sql),
                "This sql plan has multi result sinks");
    }

    @Test
    public void testSkewJoin() throws Exception {
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinOptimizeV2(true);
        String sql = "select count(1) from lineitem t1 JOIN [skew|t1.l_orderkey(1,2,3)] lineitem t2 using(l_orderkey)";
        DefaultCoordinator scheduler = startScheduling(sql);

        Assertions.assertTrue(scheduler.getExecStatus().ok());
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinOptimizeV2(false);
    }
}
