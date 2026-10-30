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


package com.starrocks.leader;

import com.google.common.collect.Sets;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

public class LeaderImplTest {

    private long dbId;
    private String dbName;
    private String tableName;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;

    private final LeaderImpl leader = new LeaderImpl();

    @BeforeEach
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
    }

    @AfterEach
    public void tearDown() {
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testFindRelatedReplica(@Mocked OlapTable olapTable, @Mocked LakeTable lakeTable,
                                       @Mocked PhysicalPartition physicalPartition, @Mocked MaterializedIndex index
                                       ) throws Exception {

        // olap table
        new Expectations() {
            {
                physicalPartition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LocalTablet(tabletId);
            }
        };
        
        Assertions.assertNull(Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, physicalPartition, backendId, tabletId, indexId));
        // lake table
        new MockUp<LakeTablet>() {
            @Mock
            public Set<Long> getBackendIds() {
                return Sets.newHashSet();
            }
        };

        new Expectations() {
            {
                physicalPartition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LakeTablet(tabletId);
            }
        };

        Assertions.assertEquals(new Replica(tabletId, backendId, -1, NORMAL), Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, physicalPartition, backendId, tabletId, indexId));
    }

    @Test
    public void testReportTranslatesIllegalStateExceptionToNotMaster(@Mocked GlobalStateMgr globalStateMgr,
                                                                     @Mocked ReportHandler reportHandler) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
                globalStateMgr.getReportHandler();
                result = reportHandler;
                reportHandler.handleReport((TReportRequest) any);
                result = new IllegalStateException("leader lease invalidated");
            }
        };

        TMasterResult result = leader.report(new TReportRequest());
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        Assertions.assertNotNull(result.getStatus().getError_msgs());
        Assertions.assertEquals(1, result.getStatus().getError_msgs().size());
        String msg = result.getStatus().getError_msgs().get(0);
        Assertions.assertTrue(msg.contains("current fe is not master"), "error msg must include non-master marker, got: " + msg);
        Assertions.assertTrue(msg.contains("leader lease invalidated"),
                "error msg must propagate the IllegalStateException message, got: " + msg);
    }

    @Test
    public void testReportRejectsWhenNotLeader(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        TMasterResult result = leader.report(new TReportRequest());
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        Assertions.assertEquals("current fe is not master", result.getStatus().getError_msgs().get(0));
    }

    @Test
    public void testCreateFinishTaskReturnsLeaderTransferredWhenLeaderDemoting(@Mocked GlobalStateMgr globalStateMgr,
                                                                               @Mocked NodeMgr nodeMgr,
                                                                               @Mocked SystemInfoService clusterInfo) {
        Backend backend = new Backend(10001L, "172.26.80.2", 9050);
        backend.setBePort(9060);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.isLeader();
                result = true;
                minTimes = 0;

                globalStateMgr.isLeaderDemoting();
                result = true;
                minTimes = 0;

                globalStateMgr.isLeaderWorkAdmissionOpen();
                result = false;
                minTimes = 0;

                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = clusterInfo;
                minTimes = 0;

                clusterInfo.getBackendWithBePort("172.26.80.2", 9060);
                result = backend;
                minTimes = 0;
            }
        };

        TFinishTaskRequest request = new TFinishTaskRequest(
                new TBackend("172.26.80.2", 9060, 8040),
                TTaskType.CREATE,
                58052L,
                new TStatus(TStatusCode.OK));

        TMasterResult result = leader.finishTask(request);

        Assertions.assertEquals(TStatusCode.LEADER_TRANSFERRED, result.getStatus().getStatus_code());
        Assertions.assertNotNull(result.getStatus().getError_msgs());
        Assertions.assertTrue(result.getStatus().getError_msgs().get(0).contains("leader is transferring"));
    }

    private static final String ALTER_BE_HOST = "172.26.80.3";
    private static final int ALTER_BE_PORT = 9060;
    private static final long ALTER_BE_ID = 10002L;
    private static final long OLD_JOB_ID = 100L;
    private static final long NEW_JOB_ID = 200L;

    private void expectLeaderAcceptingAlterFinish(GlobalStateMgr globalStateMgr, NodeMgr nodeMgr,
                                                  SystemInfoService clusterInfo,
                                                  SchemaChangeHandler schemaChangeHandler) {
        Backend backend = new Backend(ALTER_BE_ID, ALTER_BE_HOST, 9050);
        backend.setBePort(ALTER_BE_PORT);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.isLeader();
                result = true;
                minTimes = 0;

                globalStateMgr.isLeaderDemoting();
                result = false;
                minTimes = 0;

                globalStateMgr.isLeaderWorkAdmissionOpen();
                result = true;
                minTimes = 0;

                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = clusterInfo;
                minTimes = 0;

                clusterInfo.getBackendWithBePort(ALTER_BE_HOST, ALTER_BE_PORT);
                result = backend;
                minTimes = 0;

                globalStateMgr.getSchemaChangeHandler();
                result = schemaChangeHandler;
                minTimes = 0;
            }
        };
    }

    private AlterReplicaTask newAlterTask(long jobId) {
        return AlterReplicaTask.alterLocalTablet(ALTER_BE_ID, dbId, tableId, partitionId, indexId, tabletId,
                tabletId - 1, replicaId, 0, 0, 1L, jobId, null, null);
    }

    private TFinishTaskRequest alterFinishRequest(TStatusCode statusCode, Long alterJobId) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend(ALTER_BE_HOST, ALTER_BE_PORT, 8040),
                TTaskType.ALTER, tabletId, new TStatus(statusCode));
        if (alterJobId != null) {
            request.setAlter_job_id(alterJobId);
        }
        return request;
    }

    @Test
    public void testAlterFinishOfAnotherJobLeavesQueuedTaskAlone(@Mocked GlobalStateMgr globalStateMgr,
                                                                 @Mocked NodeMgr nodeMgr,
                                                                 @Mocked SystemInfoService clusterInfo,
                                                                 @Mocked SchemaChangeHandler schemaChangeHandler) {
        expectLeaderAcceptingAlterFinish(globalStateMgr, nodeMgr, clusterInfo, schemaChangeHandler);
        AlterReplicaTask newJobTask = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(newJobTask));

        // The report of a stale copy left by an older job on the same tablet, successful and failed.
        for (TStatusCode statusCode : List.of(TStatusCode.OK, TStatusCode.INTERNAL_ERROR)) {
            TMasterResult result = leader.finishTask(alterFinishRequest(statusCode, OLD_JOB_ID));
            Assertions.assertEquals(TStatusCode.CANCELLED, result.getStatus().getStatus_code(), statusCode.toString());
        }

        Assertions.assertSame(newJobTask, AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
        Assertions.assertEquals(0, newJobTask.getFailedTimes());
        Assertions.assertFalse(newJobTask.isFinished());
        new Verifications() {
            {
                schemaChangeHandler.handleFinishAlterTask((AlterReplicaTask) any);
                times = 0;
            }
        };
    }

    @Test
    public void testAlterFinishOfSameJobFinishesTask(@Mocked GlobalStateMgr globalStateMgr,
                                                     @Mocked NodeMgr nodeMgr,
                                                     @Mocked SystemInfoService clusterInfo,
                                                     @Mocked SchemaChangeHandler schemaChangeHandler) {
        expectLeaderAcceptingAlterFinish(globalStateMgr, nodeMgr, clusterInfo, schemaChangeHandler);
        AlterReplicaTask task = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(task));

        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, NEW_JOB_ID));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertNull(AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
        new Verifications() {
            {
                schemaChangeHandler.handleFinishAlterTask(task);
                times = 1;
            }
        };
    }

    @Test
    public void testAlterFinishWithoutJobIdMatchesBySignature(@Mocked GlobalStateMgr globalStateMgr,
                                                              @Mocked NodeMgr nodeMgr,
                                                              @Mocked SystemInfoService clusterInfo,
                                                              @Mocked SchemaChangeHandler schemaChangeHandler) {
        expectLeaderAcceptingAlterFinish(globalStateMgr, nodeMgr, clusterInfo, schemaChangeHandler);
        AlterReplicaTask task = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(task));

        // A backend that does not send alter_job_id.
        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, null));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertNull(AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
        new Verifications() {
            {
                schemaChangeHandler.handleFinishAlterTask(task);
                times = 1;
            }
        };
    }

    @Test
    public void testAlterFinishKeepsTaskQueuedInItsPlace(@Mocked GlobalStateMgr globalStateMgr,
                                                         @Mocked NodeMgr nodeMgr,
                                                         @Mocked SystemInfoService clusterInfo,
                                                         @Mocked SchemaChangeHandler schemaChangeHandler) {
        expectLeaderAcceptingAlterFinish(globalStateMgr, nodeMgr, clusterInfo, schemaChangeHandler);
        AlterReplicaTask oldJobTask = newAlterTask(OLD_JOB_ID);
        AlterReplicaTask newJobTask = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(oldJobTask));
        // While the report is handled, the old job ends and a new job queues a task under the same signature.
        new Expectations() {
            {
                schemaChangeHandler.handleFinishAlterTask(oldJobTask);
                result = new Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void handleFinishAlterTask(AlterReplicaTask task) {
                        AgentTaskQueue.removeTask(ALTER_BE_ID, TTaskType.ALTER, tabletId);
                        Assertions.assertTrue(AgentTaskQueue.addTask(newJobTask));
                    }
                };
            }
        };

        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, OLD_JOB_ID));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertSame(newJobTask, AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
    }
}
