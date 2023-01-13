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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.binlog.BinlogManager;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MVMaintenanceJobTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.enable_experimental_mv = true;
        PlanTestBase.beforeClass();
        SessionVariable variable = starRocksAssert.getCtx().getSessionVariable();
        variable.setEnableIncrementalRefreshMv(true);
    }

    @Test
    public void basic() throws Exception {
        MaterializedView view = new MaterializedView();
        view.setId(1024);
        view.setName("view1");
        view.setMaintenancePlan(new ExecPlan());

        List<MaterializedView.BaseTableInfo> baseTableInfos = Lists.newArrayList();
        MaterializedView.BaseTableInfo baseTableInfo1 = new MaterializedView.BaseTableInfo(100L, 1L);
        baseTableInfos.add(baseTableInfo1);
        MaterializedView.BaseTableInfo baseTableInfo2 = new MaterializedView.BaseTableInfo(100L, 2L);
        baseTableInfos.add(baseTableInfo2);
        MaterializedView.BaseTableInfo baseTableInfo3 = new MaterializedView.BaseTableInfo(100L, 2L);
        baseTableInfos.add(baseTableInfo3);

        view.setBaseTableInfos(baseTableInfos);

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        assertFalse(job.isRunnable());

        job.startJob();
        assertTrue(job.isRunnable());
        assertEquals(MVMaintenanceJob.JobState.STARTED, job.getState());

        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public void prepareExec() throws Exception {
            }
        };

        new MockUp<TxnBasedEpochCoordinator>() {
            @Mock
            public void runEpoch(MVEpoch epoch) {
            }
        };
        new MockUp<BinlogManager>() {
            @Mock
            public boolean isBinlogAvailable(long dbId, long tableId) {
                return true;
            }

            @Mock
            public Map<Long, Long> getBinlogAvailableVersion(long dbId, long tableId) {
                return Maps.newHashMap();
            }
        };

        job.onSchedule();
        job.onTransactionPublish();
        assertTrue(job.isRunnable());
        assertEquals(MVMaintenanceJob.JobState.RUN_EPOCH, job.getState());
        assertEquals(MVEpoch.EpochState.READY, job.getEpoch().getState());

        job.onSchedule();
        job.stopJob();
    }

    @Test
    public void serialize() throws IOException {
        MaterializedView view = new MaterializedView();
        view.setId(1024);
        view.setName("view1");
        view.setMaintenancePlan(new ExecPlan());

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        job.write(buffer);
        byte[] bytes = buffer.getData();

        DataInput input = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        MVMaintenanceJob deserialized = MVMaintenanceJob.read(input);
        assertEquals(job, deserialized);

    }

    @Test
    public void restore() throws Exception {
        // Create a job and serialize it
        String sql = "select LO_ORDERDATE, count(LO_LINENUMBER) cnt from lineorder_flat_for_mv group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW inc_mv1 " +
                " distributed by hash(LO_ORDERDATE) " +
                " refresh incremental as " + sql);

        Database currentDb = GlobalStateMgr.getCurrentState().getDb(connectContext.getDatabase());
        MaterializedView view = (MaterializedView) currentDb.getTable("inc_mv1");
        ExecPlan originPlan = view.getMaintenancePlan();
        String expectedPlan =
                "PLAN FRAGMENT 0\n" +
                        " OUTPUT EXPRS:\n" +
                        "  PARTITION: RANDOM\n" +
                        "\n" +
                        "  OLAP TABLE SINK\n" +
                        "    TABLE: inc_mv1\n" +
                        "    TUPLE ID: 2\n" +
                        "    RANDOM\n" +
                        "\n" +
                        "  1:StreamAgg\n" +
                        "  |  output: count(3: LO_LINENUMBER)\n" +
                        "  |  group_by: 1: LO_ORDERDATE\n" +
                        "  |  \n" +
                        "  0:BinlogScanNode\n";
        assertContains(originPlan.getExplainString(StatementBase.ExplainLevel.NORMAL), expectedPlan);

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        job.write(buffer);
        byte[] bytes = buffer.getData();

        DataInput input = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        MVMaintenanceJob deserialized = MVMaintenanceJob.read(input);
        assertEquals(job, deserialized);

        // Try to restore the deserialized job
        assertTrue(deserialized.restore());
        ExecPlan restorePlan = deserialized.getExecPlan();
        String planString = restorePlan.getExplainString(TExplainLevel.NORMAL);
        assertContains(planString, expectedPlan);
    }

    @Test
    public void buildPhysicalTopology() throws Exception {
        String sql = "select count(distinct v5) from t1 join t2";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        assertEquals("AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count)}] group by [[]] having [null]\n" +
                        "    EXCHANGE GATHER\n" +
                        "        AGGREGATE ([DISTINCT_LOCAL] aggregate [{7: count=count(2: v5)}] group by [[]] having [null]\n" +
                        "            AGGREGATE ([DISTINCT_GLOBAL] aggregate [{}] group by [[2: v5]] having [null]\n" +
                        "                EXCHANGE SHUFFLE[2]\n" +
                        "                    AGGREGATE ([LOCAL] aggregate [{}] group by [[2: v5]] having [null]\n" +
                        "                        CROSS JOIN (join-predicate [null] post-join-predicate [null])\n" +
                        "                            SCAN (columns[2: v5] predicate[null])\n" +
                        "                            EXCHANGE BROADCAST\n" +
                        "                                SCAN (columns[4: v7] predicate[null])",
                pair.first);

        String currentDb = connectContext.getDatabase();
        long dbId = GlobalStateMgr.getCurrentState().getDb(currentDb).getId();
        MaterializedView view = new MaterializedView();
        view.setDbId(dbId);
        view.setId(1024);
        view.setName("view1");
        view.setMaintenancePlan(pair.second);

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        job.buildContext(false);
        job.buildPhysicalTopology();

        Map<Long, MVMaintenanceTask> taskMap = job.getTasks();
        System.err.println(taskMap);
        assertEquals(1, taskMap.size());
        MVMaintenanceTask task = taskMap.values().stream().findFirst().get();
        System.err.println(task);
        assertEquals(0, task.getTaskId());

        List<TExecPlanFragmentParams> instances = task.getFragmentInstances();
        assertEquals(4, instances.size());

        TExecPlanFragmentParams firstInstance = instances.get(0);
        System.err.println(firstInstance);
        List<TPlanNode> planNodes = firstInstance.getFragment().getPlan().getNodes();
        assertEquals(5, planNodes.size());
        assertEquals(TPlanNodeType.AGGREGATION_NODE, planNodes.get(0).getNode_type());
        assertEquals(TPlanNodeType.PROJECT_NODE, planNodes.get(1).getNode_type());
        assertEquals(TPlanNodeType.NESTLOOP_JOIN_NODE, planNodes.get(2).getNode_type());
        assertEquals(TPlanNodeType.OLAP_SCAN_NODE, planNodes.get(3).getNode_type());
        assertEquals(TPlanNodeType.EXCHANGE_NODE, planNodes.get(4).getNode_type());
    }

}