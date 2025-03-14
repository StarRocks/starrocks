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

package com.starrocks.planner;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Pair;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Collection;
import java.util.Optional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryCacheAndMVTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView partitionedMaterializedView)
            throws Exception {
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }

    @Test
    public void testCreatePartitionedMVForIceberg() throws Exception {
        String mvName = "iceberg_parttbl_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                        "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(4, partitions.size());

        String query = "SELECT /*+SET_VAR(enable_query_cache=true) */ id, sum(data) " +
                "FROM `iceberg0`.`partitioned_db`.`t1` " +
                "where date = '2020-01-02' " +
                "group by id";

        Pair<String, ExecPlan> planAndExecPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), query);
        Optional<PlanFragment> optFragment = planAndExecPlan.second.getFragments().stream()
                .filter(planFragment -> planFragment.getCacheParam() != null)
                .findFirst();
        Assert.assertTrue(optFragment.isPresent());
        PlanFragment fragment = optFragment.get();
        String expectRange = "[types: [DATE]; keys: [2020-01-02]; ..types: [DATE]; keys: [2020-01-03]; )";
        boolean exists = fragment.getCacheParam().getRegion_map()
                .values().stream().anyMatch(value -> value.equals(expectRange));
        Assert.assertTrue(exists);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIceberg2() throws Exception {
        String mvName = "iceberg_parttbl_mv2";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv2`\n" +
                        "PARTITION BY str2date(`date`, '%Y%m%d')\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(4, partitions.size());

        String query = "SELECT /*+SET_VAR(enable_query_cache=true) */ id, sum(data) " +
                "FROM `iceberg0`.`partitioned_db`.`t1` " +
                "where date = '20200102' " +
                "group by id";

        Pair<String, ExecPlan> planAndExecPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), query);
        Optional<PlanFragment> optFragment = planAndExecPlan.second.getFragments().stream()
                .filter(planFragment -> planFragment.getCacheParam() != null)
                .findFirst();
        Assert.assertTrue(optFragment.isPresent());
        PlanFragment fragment = optFragment.get();
        String expectRange = "[]";
        boolean exists = fragment.getCacheParam().getRegion_map()
                .values().stream().anyMatch(value -> value.equals(expectRange));
        Assert.assertTrue(exists);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform1() throws Exception {
        // test partition by year(ts)
        String mvName = "iceberg_year_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_year_mv1`\n" +
                        "PARTITION BY date_trunc('year', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        String query = "SELECT /*+SET_VAR(enable_query_cache=true) */ id, sum(data) " +
                "FROM `iceberg0`.`partitioned_transforms_db`.`t0_year`" +
                "where ts between '2020-01-01 00:00:00'  and '2021-01-01 00:00:00' " +
                "group by id";

        Pair<String, ExecPlan> planAndExecPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), query);
        Optional<PlanFragment> optFragment = planAndExecPlan.second.getFragments().stream()
                .filter(planFragment -> planFragment.getCacheParam() != null)
                .findFirst();
        Assert.assertTrue(optFragment.isPresent());
        PlanFragment fragment = optFragment.get();
        String expectRange = "[types: [DATETIME]; keys: [2020-01-01 00:00:00]; " +
                "..types: [DATETIME]; keys: [2021-01-01 00:00:00]; )";
        boolean exists = fragment.getCacheParam().getRegion_map()
                .values().stream().anyMatch(value -> value.equals(expectRange));
        Assert.assertTrue(exists);
        starRocksAssert.dropMaterializedView(mvName);
    }
}