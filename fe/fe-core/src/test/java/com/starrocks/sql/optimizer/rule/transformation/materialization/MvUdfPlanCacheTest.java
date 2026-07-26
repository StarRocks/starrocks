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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MvPlanContextBuilder;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MvUdfPlanCacheTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        Config.enable_udf = true;
        FunctionName fnName = FunctionName.createFnName("udf_upper");
        fnName.setAsGlobalFunction();
        Type[] argTypes = {VarcharType.VARCHAR};
        ScalarFunction udfUpper = new ScalarFunction(fnName, argTypes, VarcharType.VARCHAR, false);
        GlobalFunctionMgr.assignIdToUserDefinedFunction(udfUpper);
        GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(udfUpper);

        starRocksAssert.withTable("CREATE TABLE example_table (col1 VARCHAR(10)) "
                + "ENGINE = olap DUPLICATE KEY(col1) DISTRIBUTED BY HASH(col1) "
                + "PROPERTIES('replication_num' = '1')");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW udf_bug_test "
                + "REFRESH ASYNC AS SELECT udf_upper(col1) AS result FROM example_table");
        // built-in function control case
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW builtin_ok_mv "
                + "REFRESH ASYNC AS SELECT upper(col1) AS result FROM example_table");
        // dedicated MV whose refresh task is dropped in the task-missing test, leaving the shared MVs untouched
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW udf_missing_task_mv "
                + "REFRESH ASYNC AS SELECT udf_upper(col1) AS result FROM example_table");
    }

    @Test
    public void testPlanBuildWithGlobalUdfWhenNoCurrentUserShouldNotFail() {
        MaterializedView mv = getMv("udf_bug_test");

        connectContext.setThreadLocalInfo();
        UserIdentity savedIdentity = connectContext.getCurrentUserIdentity();
        try {
            connectContext.setCurrentUserIdentity(null);
            List<MvPlanContext> plans = MvPlanContextBuilder.getPlanContext(mv, true);
            Assertions.assertNotNull(plans);
            Assertions.assertFalse(plans.isEmpty(),
                    "MV plan for a UDF-referencing MV must build even without a session user");
            Assertions.assertTrue(plans.get(0).isValidMvPlan(),
                    "MV plan for a UDF-referencing MV must be valid");
        } finally {
            connectContext.setCurrentUserIdentity(savedIdentity);
        }
    }

    @Test
    public void testAsyncPlanCacheWithGlobalUdfIsPopulated() {
        MaterializedView mv = getMv("udf_bug_test");
        // Force a cache miss so the value is (re)loaded on the background pool (null user) — the real #76240
        // path — instead of returning an entry a prior test may already have populated.
        CachingMvPlanContextBuilder.getInstance().evictMaterializedViewCache(mv);
        List<MvPlanContext> plans = CachingMvPlanContextBuilder.getInstance().getOrLoadPlanContext(mv, 30000);
        Assertions.assertNotNull(plans);
        Assertions.assertFalse(plans.isEmpty(),
                "async plan cache must hold a plan for a UDF-referencing MV (empty => issue #76240)");
        Assertions.assertTrue(plans.stream().anyMatch(MvPlanContext::isValidMvPlan),
                "async plan cache must hold a VALID plan for a UDF-referencing MV");
    }

    @Test
    public void testPlanBuildWithBuiltinFunctionWhenNoCurrentUserWorks() {
        MaterializedView mv = getMv("builtin_ok_mv");

        connectContext.setThreadLocalInfo();
        UserIdentity savedIdentity = connectContext.getCurrentUserIdentity();
        try {
            connectContext.setCurrentUserIdentity(null);

            List<MvPlanContext> plans = MvPlanContextBuilder.getPlanContext(mv, true);

            Assertions.assertFalse(plans.isEmpty(), "built-in-function MV plan must build without a session user");
            Assertions.assertTrue(plans.get(0).isValidMvPlan());
        } finally {
            connectContext.setCurrentUserIdentity(savedIdentity);
        }
    }

    @Test
    public void testPlanBuildWithGlobalUdfWhenCreatorAuthDisabledBuildsAsRoot() {
        MaterializedView mv = getMv("udf_bug_test");

        connectContext.setThreadLocalInfo();
        UserIdentity savedIdentity = connectContext.getCurrentUserIdentity();
        boolean savedCreatorAuth = Config.mv_use_creator_based_authorization;
        try {
            Config.mv_use_creator_based_authorization = false;
            connectContext.setCurrentUserIdentity(null);

            List<MvPlanContext> plans = MvPlanContextBuilder.getPlanContext(mv, true);

            Assertions.assertFalse(plans.isEmpty(),
                    "UDF MV plan must build under root-based authorization without a session user");
            Assertions.assertTrue(plans.get(0).isValidMvPlan());
        } finally {
            Config.mv_use_creator_based_authorization = savedCreatorAuth;
            connectContext.setCurrentUserIdentity(savedIdentity);
        }
    }

    @Test
    public void testPlanBuildWithGlobalUdfWhenRefreshTaskMissingBuildsAsRoot() {
        MaterializedView mv = getMv("udf_missing_task_mv");
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(mv);
        Assertions.assertNotNull(task, "precondition: refresh task should exist before it is dropped");
        taskManager.dropTasks(Lists.newArrayList(task.getId()));
        Assertions.assertNull(taskManager.getTask(mv), "precondition: refresh task must be gone");

        connectContext.setThreadLocalInfo();
        UserIdentity savedIdentity = connectContext.getCurrentUserIdentity();
        boolean savedCreatorAuth = Config.mv_use_creator_based_authorization;
        try {
            Config.mv_use_creator_based_authorization = true;
            connectContext.setCurrentUserIdentity(null);
            List<MvPlanContext> plans = MvPlanContextBuilder.getPlanContext(mv, true);

            Assertions.assertFalse(plans.isEmpty(),
                    "UDF MV plan must build via the ROOT fallback when the refresh task is missing");
            Assertions.assertTrue(plans.get(0).isValidMvPlan());
        } finally {
            Config.mv_use_creator_based_authorization = savedCreatorAuth;
            connectContext.setCurrentUserIdentity(savedIdentity);
        }
    }

    @Test
    public void testPlanBuildDoesNotElevateCallerSessionIdentity() {
        MaterializedView mv = getMv("udf_bug_test");

        connectContext.setThreadLocalInfo();
        UserIdentity savedIdentity = connectContext.getCurrentUserIdentity();
        String savedQualifiedUser = connectContext.getQualifiedUser();
        try {
            UserIdentity sessionUser = UserIdentity.createAnalyzedUserIdentWithIp("mv_reader", "%");
            connectContext.setCurrentUserIdentity(sessionUser);
            connectContext.setQualifiedUser(sessionUser.getUser());

            MvPlanContextBuilder.getPlanContext(mv, false);

            Assertions.assertEquals(sessionUser, connectContext.getCurrentUserIdentity(),
                    "getPlanContext must not elevate the caller's session identity");
            Assertions.assertEquals("mv_reader", connectContext.getQualifiedUser());
        } finally {
            connectContext.setCurrentUserIdentity(savedIdentity);
            connectContext.setQualifiedUser(savedQualifiedUser);
        }
    }
}
