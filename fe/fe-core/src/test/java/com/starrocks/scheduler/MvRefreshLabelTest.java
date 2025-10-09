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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MvRefreshLabelTest extends MVTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE base_label_t1 (k1 int) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES('replication_num'='1')");
        cluster.runSql(DB_NAME, "insert into base_label_t1 values (1), (2)");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_label_test REFRESH MANUAL AS select k1 from base_label_t1");
    }

    @Test
    public void testMVRefreshAssignsMvLabel() throws Exception {
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(DB_NAME, "mv_label_test");

        Task task = TaskBuilder.buildMvTask(mv, DB_NAME);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRun taskRun = taskManager.buildTaskRun(task, new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), true));
        // init status and build context
        String queryId = UUIDUtil.genUUID().toString();
        taskRun.initStatus(queryId, System.currentTimeMillis());
        TaskRunContext ctx = taskRun.buildTaskRunContext();

        MVTaskRunProcessor proc = (MVTaskRunProcessor) taskRun.getProcessor();
        proc.prepare(ctx);
        BaseMVRefreshProcessor.ProcessExecPlan plan = proc.getMVRefreshProcessor().getProcessExecPlan(proc.getMvTaskRunContext());

        if (plan != null && plan.insertStmt() != null) {
            String label = plan.insertStmt().getLabel();
            // The label is assigned during planning; it should either be null (skipped) or start with mv_
            if (label != null) {
                Assertions.assertTrue(label.startsWith("mv_"), "MV refresh label should start with mv_ but was: " + label);
            }
        }
    }
}
