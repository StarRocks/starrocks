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

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.Config;
import com.starrocks.load.loadv2.BulkLoadJob;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class JobSpecTest extends SchedulerTestBase {
    private static final TWorkGroup QUERY_RESOURCE_GROUP = new TWorkGroup();
    private static final TWorkGroup LOAD_RESOURCE_GROUP = new TWorkGroup();

    private static final DefaultCoordinator.Factory COORDINATOR_FACTORY = new DefaultCoordinator.Factory();

    static {
        QUERY_RESOURCE_GROUP.setId(0L);
        LOAD_RESOURCE_GROUP.setId(1L);
    }

    private boolean prevEnablePipelineLoad;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();

        new MockUp<ResourceGroupMgr>() {
            @Mock
            public TWorkGroup chooseResourceGroup(ConnectContext ctx, ResourceGroupClassifier.QueryType queryType,
                                                  Set<Long> databases) {
                if (queryType != ResourceGroupClassifier.QueryType.SELECT) {
                    return LOAD_RESOURCE_GROUP;
                } else {
                    return QUERY_RESOURCE_GROUP;
                }
            }
        };
    }

    @Before
    public void before() {
        prevEnablePipelineLoad = Config.enable_pipeline_load;
    }

    @After
    public void after() {
        Config.enable_pipeline_load = prevEnablePipelineLoad;
    }

    @Test
    public void testFromQuerySpec() throws Exception {
        // Prepare input arguments.
        String sql = "select * from lineitem";
        ExecPlan execPlan = getExecPlan(sql);

        TUniqueId queryId = new TUniqueId(2, 3);
        connectContext.setExecutionId(queryId);
        UUID lastQueryId = new UUID(4L, 5L);
        connectContext.setLastQueryId(lastQueryId);
        DescriptorTable descTable = new DescriptorTable();
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();

        DefaultCoordinator coordinator = COORDINATOR_FACTORY.createQueryScheduler(
                connectContext, fragments, scanNodes, descTable.toThrift());
        JobSpec jobSpec = coordinator.getJobSpec();

        // Check created jobSpec.
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertEquals(lastQueryId.toString(), jobSpec.getQueryGlobals().getLast_query_id());
        Assert.assertEquals(TQueryType.SELECT, jobSpec.getQueryOptions().getQuery_type());
        Assert.assertTrue(jobSpec.isEnablePipeline());
        Assert.assertFalse(jobSpec.isEnableStreamPipeline());
        Assert.assertFalse(jobSpec.isBlockQuery());
        Assert.assertEquals(QUERY_RESOURCE_GROUP, jobSpec.getResourceGroup());

        coordinator = COORDINATOR_FACTORY.createInsertScheduler(
                connectContext, fragments, scanNodes, descTable.toThrift());
        jobSpec = coordinator.getJobSpec();
        Assert.assertEquals(LOAD_RESOURCE_GROUP, jobSpec.getResourceGroup());
    }

    @Test
    public void testFromMVMaintenanceJobSpec() throws Exception {
        // Prepare input arguments.
        String sql = "select * from lineitem";
        ExecPlan execPlan = getExecPlan(sql);

        TUniqueId queryId = new TUniqueId(2, 3);
        connectContext.setExecutionId(queryId);
        UUID lastQueryId = new UUID(4L, 5L);
        connectContext.setLastQueryId(lastQueryId);
        DescriptorTable descTable = new DescriptorTable();
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();

        JobSpec jobSpec = JobSpec.Factory.fromMVMaintenanceJobSpec(
                connectContext, fragments, scanNodes, descTable.toThrift());

        // Check created jobSpec.
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertEquals(lastQueryId.toString(), jobSpec.getQueryGlobals().getLast_query_id());
        Assert.assertEquals(TQueryType.SELECT, jobSpec.getQueryOptions().getQuery_type());
        Assert.assertTrue(jobSpec.isEnablePipeline());
        Assert.assertTrue(jobSpec.isEnableStreamPipeline());
        Assert.assertFalse(jobSpec.isBlockQuery());
        Assert.assertEquals(QUERY_RESOURCE_GROUP, jobSpec.getResourceGroup());
    }

    @Test
    public void testFromBrokerLoadJobSpec() throws Exception {
        // Prepare input arguments.
        String sql = "insert into lineitem select * from lineitem";

        long loadJobId = 1L;
        TUniqueId queryId = new TUniqueId(2, 3);
        UUID lastQueryId = new UUID(4L, 5L);
        connectContext.setLastQueryId(lastQueryId);
        String timezone = connectContext.getSessionVariable().getTimeZone();
        long startTime = connectContext.getStartTime();
        Map<String, String> sessionVariables = ImmutableMap.of();
        long execMemLimit = 4L;
        long loadMemLimit = 5L;
        int timeout = 6;

        LoadPlanner loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);

        DefaultCoordinator coordinator = COORDINATOR_FACTORY.createBrokerLoadScheduler(loadPlanner);
        JobSpec jobSpec = coordinator.getJobSpec();

        // Check created jobSpec.
        Assert.assertEquals(loadJobId, jobSpec.getLoadJobId());
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertEquals(lastQueryId.toString(), jobSpec.getQueryGlobals().getLast_query_id());
        Assert.assertEquals(TQueryType.LOAD, jobSpec.getQueryOptions().getQuery_type());
        Assert.assertEquals(timeout, jobSpec.getQueryOptions().getQuery_timeout());
        Assert.assertEquals(loadMemLimit, jobSpec.getQueryOptions().getLoad_mem_limit());
        Assert.assertEquals(execMemLimit, jobSpec.getQueryOptions().getMem_limit());
        Assert.assertEquals(execMemLimit, jobSpec.getQueryOptions().getQuery_mem_limit());
        Assert.assertTrue(jobSpec.isEnablePipeline());
        Assert.assertFalse(jobSpec.isEnableStreamPipeline());
        Assert.assertTrue(jobSpec.isBlockQuery());
        Assert.assertEquals(LOAD_RESOURCE_GROUP, jobSpec.getResourceGroup());

        // Check created jobSpec for sessionVariables.
        Assert.assertEquals(TCompressionType.NO_COMPRESSION,
                jobSpec.getQueryOptions().getLoad_transmission_compression_type());
        Assert.assertFalse(jobSpec.getQueryOptions().isSetLog_rejected_record_num());

        sessionVariables = ImmutableMap.of(
                SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE, "LZ4",
                BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY, "10"
        );
        loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);
        coordinator = COORDINATOR_FACTORY.createBrokerLoadScheduler(loadPlanner);
        jobSpec = coordinator.getJobSpec();
        Assert.assertEquals(TCompressionType.LZ4, jobSpec.getQueryOptions().getLoad_transmission_compression_type());
        Assert.assertEquals(10L, jobSpec.getQueryOptions().getLog_rejected_record_num());

        // Check negative execMemLimit.
        execMemLimit = -1;
        loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);
        coordinator = COORDINATOR_FACTORY.createBrokerLoadScheduler(loadPlanner);
        jobSpec = coordinator.getJobSpec();
        Assert.assertEquals(loadMemLimit, jobSpec.getQueryOptions().getLoad_mem_limit());
        Assert.assertTrue(jobSpec.getQueryOptions().isSetMem_limit());
        Assert.assertFalse(jobSpec.getQueryOptions().isSetQuery_mem_limit());
    }

    @Test
    public void testFromStreamLoadJobSpec() throws Exception {
        // Prepare input arguments.
        String sql = "insert into lineitem select * from lineitem";

        long loadJobId = 1L;
        TUniqueId queryId = new TUniqueId(2, 3);
        UUID lastQueryId = new UUID(4L, 5L);
        connectContext.setLastQueryId(lastQueryId);
        String timezone = connectContext.getSessionVariable().getTimeZone();
        long startTime = connectContext.getStartTime();
        Map<String, String> sessionVariables = ImmutableMap.of();
        long execMemLimit = 4L;
        long loadMemLimit = 5L;
        int timeout = 6;

        LoadPlanner loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);

        DefaultCoordinator coordinator = COORDINATOR_FACTORY.createStreamLoadScheduler(loadPlanner);
        JobSpec jobSpec = coordinator.getJobSpec();

        // Check created jobSpec.
        Assert.assertEquals(loadJobId, jobSpec.getLoadJobId());
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertEquals(lastQueryId.toString(), jobSpec.getQueryGlobals().getLast_query_id());
        Assert.assertEquals(TQueryType.LOAD, jobSpec.getQueryOptions().getQuery_type());
        Assert.assertEquals(TLoadJobType.STREAM_LOAD, jobSpec.getQueryOptions().getLoad_job_type());
        Assert.assertEquals(timeout, jobSpec.getQueryOptions().getQuery_timeout());
        Assert.assertEquals(loadMemLimit, jobSpec.getQueryOptions().getLoad_mem_limit());
        Assert.assertEquals(execMemLimit, jobSpec.getQueryOptions().getMem_limit());
        Assert.assertEquals(execMemLimit, jobSpec.getQueryOptions().getQuery_mem_limit());
        Assert.assertTrue(jobSpec.isEnablePipeline());
        Assert.assertFalse(jobSpec.isEnableStreamPipeline());
        Assert.assertTrue(jobSpec.isBlockQuery());
        Assert.assertEquals(LOAD_RESOURCE_GROUP, jobSpec.getResourceGroup());

        // Check created jobSpec for sessionVariables.
        Assert.assertEquals(TCompressionType.NO_COMPRESSION,
                jobSpec.getQueryOptions().getLoad_transmission_compression_type());
        Assert.assertFalse(jobSpec.getQueryOptions().isSetLog_rejected_record_num());

        sessionVariables = ImmutableMap.of(
                SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE, "LZ4",
                BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY, "10"
        );
        loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);
        coordinator = COORDINATOR_FACTORY.createStreamLoadScheduler(loadPlanner);
        jobSpec = coordinator.getJobSpec();
        Assert.assertEquals(TCompressionType.LZ4, jobSpec.getQueryOptions().getLoad_transmission_compression_type());
        Assert.assertEquals(10L, jobSpec.getQueryOptions().getLog_rejected_record_num());

        // Check negative execMemLimit.
        execMemLimit = -1;
        loadPlanner = new LoadPlanner(loadJobId, queryId, 0L, 0L,
                new OlapTable(), false, timezone,
                timeout, startTime, false, connectContext, sessionVariables, loadMemLimit, execMemLimit,
                null, null, null, 0);
        coordinator = COORDINATOR_FACTORY.createStreamLoadScheduler(loadPlanner);
        jobSpec = coordinator.getJobSpec();
        Assert.assertEquals(loadMemLimit, jobSpec.getQueryOptions().getLoad_mem_limit());
        Assert.assertTrue(jobSpec.getQueryOptions().isSetMem_limit());
        Assert.assertFalse(jobSpec.getQueryOptions().isSetQuery_mem_limit());
    }

    @Test
    public void testFromBrokerExportSpec() throws Exception {
        // Prepare input arguments.
        String sql = "insert into lineitem select * from lineitem";
        ExecPlan execPlan = getExecPlan(sql);

        long loadJobId = 1L;
        TUniqueId queryId = new TUniqueId(2, 3);
        DescriptorTable descTable = new DescriptorTable();
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        String timezone = connectContext.getSessionVariable().getTimeZone();
        long startTime = connectContext.getStartTime();
        Map<String, String> sessionVariables = ImmutableMap.of();
        long execMemLimit = 4L;

        DefaultCoordinator coordinator = COORDINATOR_FACTORY.createBrokerExportScheduler(
                loadJobId, queryId, descTable, fragments, scanNodes, timezone, startTime,
                sessionVariables,
                execMemLimit);
        JobSpec jobSpec = coordinator.getJobSpec();

        // Check created jobSpec.
        Assert.assertEquals(loadJobId, jobSpec.getLoadJobId());
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertEquals(execMemLimit, jobSpec.getQueryOptions().getMem_limit());
        Assert.assertTrue(jobSpec.isEnablePipeline());
        Assert.assertFalse(jobSpec.isEnableStreamPipeline());
        Assert.assertTrue(jobSpec.isBlockQuery());
        Assert.assertEquals(QUERY_RESOURCE_GROUP, jobSpec.getResourceGroup()); // Export job doesn't setTQueryType.

        // Check created jobSpec for sessionVariables.
        Assert.assertFalse(jobSpec.getQueryOptions().isSetLoad_transmission_compression_type());
        Assert.assertFalse(jobSpec.getQueryOptions().isSetLog_rejected_record_num());

        sessionVariables = ImmutableMap.of(
                SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE, "LZ4",
                BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY, "10"
        );
        coordinator = COORDINATOR_FACTORY.createBrokerExportScheduler(
                loadJobId, queryId, descTable, fragments, scanNodes, timezone, startTime,
                sessionVariables,
                execMemLimit);
        jobSpec = coordinator.getJobSpec();

        Assert.assertEquals(TCompressionType.LZ4, jobSpec.getQueryOptions().getLoad_transmission_compression_type());
        Assert.assertEquals(10L, jobSpec.getQueryOptions().getLog_rejected_record_num());
    }

    @Test
    public void testFromSyncStreamLoadSpec(@Mocked StreamLoadPlanner planner) throws Exception {
        TUniqueId queryId = new TUniqueId(2, 3);
        new Expectations(planner) {
            {
                planner.getExecPlanFragmentParams();
                result = new TExecPlanFragmentParams().setParams(
                        new TPlanFragmentExecParams().setFragment_instance_id(queryId));
            }
        };

        DefaultCoordinator coordinator = COORDINATOR_FACTORY.createSyncStreamLoadScheduler(planner, new TNetworkAddress());
        JobSpec jobSpec = coordinator.getJobSpec();

        // Check created jobSpec.
        Assert.assertEquals(queryId, jobSpec.getQueryId());
        Assert.assertFalse(jobSpec.isEnablePipeline());
        Assert.assertFalse(jobSpec.isEnableStreamPipeline());
        Assert.assertNull(jobSpec.getResourceGroup());

    }
}
