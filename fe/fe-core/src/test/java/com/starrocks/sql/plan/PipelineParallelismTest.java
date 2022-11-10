// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

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
        // SchemaScanNode doesn't support pipeline, so ParallelExecNum of fragment is 
        // equal to the corresponding session variables.
        Assert.assertEquals(parallelExecInstanceNum, fragment0.getParallelExecNum());
        Assert.assertEquals(1, fragment0.getPipelineDop());
    }

    @Test
    public void testMetaScan() throws Exception {
        ExecPlan plan = getExecPlan("select * from t0 [_META_]");
        PlanFragment fragment1 = plan.getFragments().get(1);
        assertContains(fragment1.getExplainString(TExplainLevel.NORMAL), "MetaScan");
        // MetaScanNode doesn't support pipeline, so ParallelExecNum of fragment is 
        // equal to the corresponding session variables.
        Assert.assertEquals(parallelExecInstanceNum, fragment1.getParallelExecNum());
        Assert.assertEquals(1, fragment1.getPipelineDop());
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
        // Outfile ResultSink doesn't support pipeline, so ParallelExecNum of fragment is 
        // equal to the corresponding session variables.
        Assert.assertEquals(parallelExecInstanceNum, fragment0.getParallelExecNum());
        Assert.assertEquals(1, fragment0.getPipelineDop());
    }

    @Test
    public void testInsert() throws Exception {
        ExecPlan plan = getExecPlan("insert into t0 select * from t0");
        PlanFragment fragment0 = plan.getFragments().get(0);
        assertContains(fragment0.getExplainString(TExplainLevel.NORMAL), "OLAP TABLE SINK");
        // Disable pipeline_load by Config, so ParallelExecNum of fragment is
        // equal to the corresponding session variables.
        Assert.assertEquals(parallelExecInstanceNum, fragment0.getParallelExecNum());
        Assert.assertEquals(1, fragment0.getPipelineDop());
    }
}
