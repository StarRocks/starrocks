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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.MOCK_PAIMON_CATALOG_NAME;
import static com.starrocks.sql.plan.PlanTestBase.cleanupEphemeralMVs;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorPaimonTest extends MVRefreshTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVRefreshTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MOCK_PAIMON_CATALOG_NAME, temp.newFolder().toURI().toString());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        cleanupEphemeralMVs(starRocksAssert, startSuiteTime);
    }

    @Before
    public void before() {
        startCaseTime = Instant.now().getEpochSecond();
    }

    @After
    public void after() throws Exception {
        cleanupEphemeralMVs(starRocksAssert, startCaseTime);
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    private static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
    }

    @Test
    public void testcreateUnpartitionedPmnMaterializeView() throws Exception {
        //unparitioned
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`paimon_parttbl_mv2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "DISTRIBUTED BY HASH(`pk`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT pk, d  FROM `paimon0`.`pmn_db1`.`unpartitioned_table` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        MaterializedView unpartitionedMaterializedView = ((MaterializedView) testDb.getTable("paimon_parttbl_mv2"));
        triggerRefreshMv(testDb, unpartitionedMaterializedView);

        Collection<Partition> partitions = unpartitionedMaterializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
    }

    @Test
    public void testCreatePartitionedPmnMaterializeView() throws Exception {
        //paritioned
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`paimon_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`pt`)\n" +
                        "DISTRIBUTED BY HASH(`pk`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT pk, pt,d  FROM `paimon0`.`pmn_db1`.`partitioned_table` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView partitionedMaterializedView = ((MaterializedView) testDb.getTable("paimon_parttbl_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(10, partitions.size());
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                partitionedMaterializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap();

        BaseTableInfo baseTableInfo = new BaseTableInfo("paimon0", "pmn_db1", "partitioned_table", "partitioned_table");
        versionMap.get(baseTableInfo).put("pt=2026-11-22", new MaterializedView.BasePartitionInfo(1, 2, -1));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Assert.assertEquals(10, partitionedMaterializedView.getPartitions().size());
        triggerRefreshMv(testDb, partitionedMaterializedView);
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView partitionedMaterializedView)
            throws Exception {
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }
}