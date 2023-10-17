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


package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.PlanFragment;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipelineParallelismTest extends PlanTestBase {
    private MockUp<BackendCoreStat> mockedBackendCoreStat = null;
    private final int parallelExecInstanceNum = 16;
    private final int numHardwareCores = 8;
    private int prevParallelExecInstanceNum = 0;
    private boolean prevEnablePipelineEngine = true;
    private int prevPipelineDop = 0;

    @Before
    public void setUp() {
        mockedBackendCoreStat = new MockUp<BackendCoreStat>() {
            @Mock
            public int getAvgNumOfHardwareCoresOfBe() {
                return numHardwareCores;
            }
        };

        prevParallelExecInstanceNum = connectContext.getSessionVariable().getParallelExecInstanceNum();
        prevEnablePipelineEngine = connectContext.getSessionVariable().isEnablePipelineEngine();
        prevPipelineDop = connectContext.getSessionVariable().getPipelineDop();

        connectContext.getSessionVariable().setParallelExecInstanceNum(parallelExecInstanceNum);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setPipelineDop(0);
        connectContext.getSessionVariable().setEnableAdaptiveSinkDop(false);
    }

    @After
    public void tearDown() {
        mockedBackendCoreStat = null;

        connectContext.getSessionVariable().setParallelExecInstanceNum(prevParallelExecInstanceNum);
        connectContext.getSessionVariable().setEnablePipelineEngine(prevEnablePipelineEngine);
        connectContext.getSessionVariable().setPipelineDop(prevPipelineDop);
    }

    @Test
    public void testSchemaScan() throws Exception {
        ExecPlan plan = getExecPlan("select * from information_schema.columns");
        PlanFragment fragment0 = plan.getFragments().get(0);
        assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "SCAN SCHEMA");
        Assert.assertEquals(1, fragment0.getParallelExecNum());
        Assert.assertEquals(numHardwareCores / 2, fragment0.getPipelineDop());
    }

    @Test
    public void testMetaScan() throws Exception {
        ExecPlan plan = getExecPlan("select * from t0 [_META_]");
        PlanFragment fragment1 = plan.getFragments().get(1);
        assertContains(fragment1.getExplainString(TExplainLevel.NORMAL), "MetaScan");
        Assert.assertEquals(1, fragment1.getParallelExecNum());
        Assert.assertEquals(numHardwareCores / 2, fragment1.getPipelineDop());
    }

    @Test
    public void testOutfile() throws Exception {
        ExecPlan plan = getExecPlan("SELECT v1,v2,v3 FROM t0  INTO OUTFILE \"hdfs://path/to/result_\""
                + "FORMAT AS CSV PROPERTIES" +
                "(\"broker.name\" = \"my_broker\"," +
                "\"broker.hadoop.security.authentication\" = \"kerberos\"," +
                "\"line_delimiter\" = \"\n\", \"max_file_size\" = \"100MB\");");
        PlanFragment fragment0 = plan.getFragments().get(0);
        assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "RESULT SINK");
        Assert.assertEquals(1, fragment0.getParallelExecNum());
        Assert.assertEquals(numHardwareCores / 2, fragment0.getPipelineDop());
    }

    @Test
    public void testInsert() throws Exception {
        boolean prevEnablePipelineLoad = Config.enable_pipeline_load;
        try {
            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(false);

            ExecPlan plan = getExecPlan("insert into t0 select * from t0");
            PlanFragment fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // ParallelExecNum of fragment not 1. still can not use pipeline
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(parallelExecInstanceNum, fragment0.getPipelineDop());

            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(true);

            plan = getExecPlan("insert into t0 select * from t0");
            fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // ParallelExecNum of fragment not 1. still can not use pipeline
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(numHardwareCores / 3, fragment0.getPipelineDop());
        } finally {
            Config.enable_pipeline_load = prevEnablePipelineLoad;
        }
    }

    @Test
    public void testDelete() throws Exception {
        boolean prevEnablePipelineLoad = Config.enable_pipeline_load;
        try {
            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(false);

            ExecPlan plan = getExecPlan("delete from tprimary where pk = 1");
            PlanFragment fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // enable pipeline_load by Config, so ParallelExecNum of fragment is set to 1.
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(parallelExecInstanceNum, fragment0.getPipelineDop());

            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(true);

            plan = getExecPlan("delete from tprimary where pk = 1");
            fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // enable pipeline_load by Config, so ParallelExecNum of fragment is set to 1.
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(numHardwareCores / 3, fragment0.getPipelineDop());
        } finally {
            Config.enable_pipeline_load = prevEnablePipelineLoad;
        }
    }

    @Test
    public void tesUpdate() throws Exception {
        boolean prevEnablePipelineLoad = Config.enable_pipeline_load;
        try {
            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(false);

            ExecPlan plan = getExecPlan("update tprimary set v1 = 'aaa' where pk = 1");
            PlanFragment fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // enable pipeline_load by Config, so ParallelExecNum of fragment is set to 1.
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(parallelExecInstanceNum, fragment0.getPipelineDop());

            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(true);

            plan = getExecPlan("update tprimary set v1 = 'aaa' where pk = 1");
            fragment0 = plan.getFragments().get(0);
            assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
            // enable pipeline_load by Config, so ParallelExecNum of fragment is set to 1.
            Assert.assertEquals(1, fragment0.getParallelExecNum());
            Assert.assertEquals(numHardwareCores / 3, fragment0.getPipelineDop());
        } finally {
            Config.enable_pipeline_load = prevEnablePipelineLoad;
        }
    }

    @Test
    public void testOrderBy() throws Exception {
        String sql = "select * from t0 order by v1 limit 100";
        ExecPlan plan = getExecPlan(sql);
        PlanFragment fragment1 = plan.getFragments().get(1);
        assertContains(fragment1.getExplainString(TExplainLevel.NORMAL), "TOP-N");
        Assert.assertEquals(1, fragment1.getParallelExecNum());
        Assert.assertEquals(numHardwareCores / 2, fragment1.getPipelineDop());
    }

    private void testJoinHelper(int expectedParallelism) throws Exception {
        // Case 1: local bucket shuffle join should use fragment instance parallel.
        String sql = "select a.v1 from t0 a join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
        ExecPlan plan = getExecPlan(sql);
        PlanFragment fragment = plan.getFragments().get(1);
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BUCKET_SHUFFLE)");
        Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());
        Assert.assertEquals(1, fragment.getParallelExecNum());

        // Case 2: colocate join should use pipeline instance parallel.
        sql = "select * from colocate1 left join colocate2 " +
                "on colocate1.k1=colocate2.k1 and colocate1.k2=colocate2.k2;";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(1);
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: LEFT OUTER JOIN (COLOCATE)");
        Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());
        Assert.assertEquals(1, fragment.getParallelExecNum());

        // Case 3: broadcast join should use pipeline parallel.
        sql = "select a.v1 from t0 a join [broadcast] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(1);
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BROADCAST)");
        Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());
        Assert.assertEquals(1, fragment.getParallelExecNum());

        // Case 4: local bucket shuffle join succeeded by broadcast should use fragment instance parallel.
        sql = "select a.v1 from t0 a " +
                "join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1 " +
                "join [broadcast] t0 c on a.v1 = c.v2";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(1);
        String fragmentString = fragment.getExplainString(TExplainLevel.NORMAL);
        assertContains(fragmentString, "join op: INNER JOIN (BROADCAST)");
        assertContains(fragmentString, "join op: INNER JOIN (BUCKET_SHUFFLE)");
        Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());
        Assert.assertEquals(1, fragment.getParallelExecNum());
    }

    @Test
    public void testJoin() throws Exception {
        try {
            FeConstants.runningUnitTest = true;

            connectContext.getSessionVariable().setPipelineDop(0);
            connectContext.getSessionVariable().setParallelExecInstanceNum(1);
            testJoinHelper(numHardwareCores / 2);

            connectContext.getSessionVariable().setPipelineDop(4);
            connectContext.getSessionVariable().setParallelExecInstanceNum(8);
            testJoinHelper(4);
        } finally {
            FeConstants.runningUnitTest = false;
        }
    }
}
