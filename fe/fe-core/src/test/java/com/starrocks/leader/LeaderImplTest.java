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
import com.starrocks.alter.AlterHandler;
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
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

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

    private static final String ALTER_BE_HOST = "172.26.80.3";
    private static final int ALTER_BE_PORT = 9060;
    private static final long ALTER_BE_ID = 10002L;
    private static final long OLD_JOB_ID = 100L;
    private static final long NEW_JOB_ID = 200L;

    // SchemaChangeHandler extends Thread (via FrontendDaemon), which JMockit refuses to mock, so these tests stub the
    // leader with MockUps around a real handler instead of mocking GlobalStateMgr.
    private void stubLeaderAcceptingAlterFinish() {
        Backend backend = new Backend(ALTER_BE_ID, ALTER_BE_HOST, 9050);
        backend.setBePort(ALTER_BE_PORT);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public SchemaChangeHandler getSchemaChangeHandler() {
                return schemaChangeHandler;
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackendWithBePort(String host, int bePort) {
                return ALTER_BE_HOST.equals(host) && bePort == ALTER_BE_PORT ? backend : null;
            }
        };
    }

    /**
     * Returns the tasks the leader hands to the alter handler as finished, running |onFinish| for each.
     */
    private static List<AlterReplicaTask> recordFinishedAlterTasks(Consumer<AlterReplicaTask> onFinish) {
        List<AlterReplicaTask> finished = new ArrayList<>();
        new MockUp<AlterHandler>() {
            @Mock
            public void handleFinishAlterTask(AlterReplicaTask task) {
                finished.add(task);
                onFinish.accept(task);
            }
        };
        return finished;
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
    public void testAlterFinishOfAnotherJobLeavesQueuedTaskAlone() {
        stubLeaderAcceptingAlterFinish();
        List<AlterReplicaTask> finished = recordFinishedAlterTasks(task -> { });
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
        Assertions.assertTrue(finished.isEmpty());
    }

    @Test
    public void testAlterFinishOfSameJobFinishesTask() {
        stubLeaderAcceptingAlterFinish();
        List<AlterReplicaTask> finished = recordFinishedAlterTasks(task -> { });
        AlterReplicaTask task = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(task));

        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, NEW_JOB_ID));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertNull(AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
        Assertions.assertEquals(List.of(task), finished);
    }

    @Test
    public void testAlterFinishWithoutJobIdMatchesBySignature() {
        stubLeaderAcceptingAlterFinish();
        List<AlterReplicaTask> finished = recordFinishedAlterTasks(task -> { });
        AlterReplicaTask task = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(task));

        // A backend that does not send alter_job_id.
        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, null));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertNull(AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
        Assertions.assertEquals(List.of(task), finished);
    }

    @Test
    public void testAlterFinishKeepsTaskQueuedInItsPlace() {
        stubLeaderAcceptingAlterFinish();
        AlterReplicaTask oldJobTask = newAlterTask(OLD_JOB_ID);
        AlterReplicaTask newJobTask = newAlterTask(NEW_JOB_ID);
        Assertions.assertTrue(AgentTaskQueue.addTask(oldJobTask));
        // While the report is handled, the old job ends and a new job queues a task under the same signature.
        recordFinishedAlterTasks(task -> {
            AgentTaskQueue.removeTask(ALTER_BE_ID, TTaskType.ALTER, tabletId);
            Assertions.assertTrue(AgentTaskQueue.addTask(newJobTask));
        });

        TMasterResult result = leader.finishTask(alterFinishRequest(TStatusCode.OK, OLD_JOB_ID));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        Assertions.assertSame(newJobTask, AgentTaskQueue.getTask(ALTER_BE_ID, TTaskType.ALTER, tabletId));
    }
}
