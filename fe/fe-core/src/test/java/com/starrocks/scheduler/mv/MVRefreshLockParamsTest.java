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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * An external base table has no usable lock identity: BaseTableInfo's external constructor leaves tableId at
 * -1, and the database id comes from the connector. It must therefore never reach LockParams -- otherwise every
 * MV in the FE built on a JDBC base table contends on the same (0, -1) entry. Internal base tables must keep
 * their real entry, because collectBaseTableSnapshotInfos does copyOnlyForQuery on them.
 */
public class MVRefreshLockParamsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.lock_params_tbl\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("CREATE MATERIALIZED VIEW test.lock_params_external_mv\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES ('replication_num' = '1')\n" +
                        "AS SELECT l_orderkey, l_suppkey FROM hive0.partitioned_db.lineitem_par;")
                .withMaterializedView("CREATE MATERIALIZED VIEW test.lock_params_mixed_mv\n" +
                        "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES ('replication_num' = '1')\n" +
                        "AS SELECT t.k2, h.l_suppkey FROM test.lock_params_tbl t " +
                        "JOIN hive0.partitioned_db.lineitem_par h ON t.k2 = h.l_orderkey;");
    }

    private static LockParams collectDatabases(String mvName) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), mvName);
        Assertions.assertNotNull(mv);

        Task task = TaskBuilder.buildMvTask(mv, db.getFullName());
        Map<String, String> properties = task.getProperties();
        properties.put(TaskRun.IS_TEST, "true");

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        MVTaskRunProcessor mvTaskRunProcessor = new MVTaskRunProcessor();
        TaskRunContext taskRunContext = new TaskRunContext();
        taskRunContext.setTaskRun(taskRun);
        taskRunContext.setCtx(connectContext);
        taskRunContext.getCtx().setDatabase("test");
        taskRunContext.setProperties(properties);
        mvTaskRunProcessor.prepare(taskRunContext);

        return mvTaskRunProcessor.getMVRefreshProcessor().collectDatabases();
    }

    @Test
    public void testAllExternalBaseTablesTakeNoLock() throws Exception {
        LockParams lockParams = collectDatabases("lock_params_external_mv");
        Assertions.assertTrue(lockParams.getDbs().isEmpty(), "external base tables must not enter the lock set");
        Assertions.assertTrue(lockParams.getTables().isEmpty(), "external base tables must not enter the lock set");
    }

    @Test
    public void testMixedBaseTablesKeepOnlyTheInternalEntry() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        long internalTableId = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "lock_params_tbl").getId();

        LockParams lockParams = collectDatabases("lock_params_mixed_mv");

        // The internal base table still needs its READ lock: collectBaseTableSnapshotInfos copies it.
        Assertions.assertEquals(Set.of(db.getId()), lockParams.getDbs().keySet());
        Assertions.assertEquals(Set.of(internalTableId), lockParams.getTables().get(db.getId()));
        // The -1 default that external BaseTableInfos carry must never show up as a lock id.
        lockParams.getTables().values().forEach(ids -> Assertions.assertFalse(ids.contains(-1L)));
    }
}
