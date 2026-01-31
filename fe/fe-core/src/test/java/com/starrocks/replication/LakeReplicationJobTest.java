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

package com.starrocks.replication;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class LakeReplicationJobTest {
    protected static StarRocksAssert starRocksAssert;

    protected static Database db;
    protected static OlapTable table;
    protected static OlapTable srcTable;
    protected static Partition partition;
    protected static Partition srcPartition;
    protected ReplicationJob job;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table lake_single_partition_duplicate_key (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "lake_single_partition_duplicate_key");
        srcTable = DeepCopy.copyWithGson(table, OlapTable.class);

        partition = table.getPartitions().iterator().next();
        srcPartition = srcTable.getPartitions().iterator().next();

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        partition.getDefaultPhysicalPartition().updateVersionForRestore(10);
        srcPartition.getDefaultPhysicalPartition().updateVersionForRestore(100);
        partition.getDefaultPhysicalPartition().setDataVersion(8);
        partition.getDefaultPhysicalPartition().setNextDataVersion(9);
        srcPartition.getDefaultPhysicalPartition().setDataVersion(98);
        srcPartition.getDefaultPhysicalPartition().setNextDataVersion(99);

        long virtualTabletId = 1000;
        long srcDatabaseId = 100;
        long srcTableId = 100;
        job = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId, db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
    }

    @Test
    public void testNormal() throws Exception {
        Assertions.assertFalse(ReplicationJobState.INITIALIZING.equals(job));
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());
        Assertions.assertEquals(ReplicationJobState.INITIALIZING.name(), job.getState().name());
        Assertions.assertEquals(ReplicationJobState.INITIALIZING.toString(), job.getState().toString());
        Assertions.assertEquals(ReplicationJobState.INITIALIZING.hashCode(), job.getState().hashCode());

        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(job, "runningTasks");

        job.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, job.getState());

        for (AgentTask task : runningTasks.values()) {
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTask_status(new TStatus(TStatusCode.OK));
            job.finishReplicateSnapshotTask((ReplicateSnapshotTask) task, request);

            Deencapsulation.invoke(new LeaderImpl(), "finishReplicateSnapshotTask",
                    (ReplicateSnapshotTask) task, request);
            ((ReplicateSnapshotTask) task).toThrift();
            task.toString();
        }

        job.run();
        Assertions.assertEquals(ReplicationJobState.COMMITTED, job.getState());

        Assertions.assertEquals(partition.getDefaultPhysicalPartition().getCommittedVersion(),
                srcPartition.getDefaultPhysicalPartition().getVisibleVersion());
    }

    @Test
    public void testCommittedCancel() {
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());

        job.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, job.getState());

        job.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, job.getState());

        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(job, "runningTasks");
        for (AgentTask task : runningTasks.values()) {
            TFinishTaskRequest request = new TFinishTaskRequest();
            request.setTask_status(new TStatus(TStatusCode.OK));
            job.finishReplicateSnapshotTask((ReplicateSnapshotTask) task, request);
        }

        job.run();
        Assertions.assertEquals(ReplicationJobState.COMMITTED, job.getState());

        job.cancel();
        Assertions.assertEquals(ReplicationJobState.COMMITTED, job.getState());
    }

    @Test
    public void testReplicatingCancel() {
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());

        job.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, job.getState());

        job.cancel();
        Assertions.assertEquals(ReplicationJobState.ABORTED, job.getState());

        job.run();
        Assertions.assertEquals(ReplicationJobState.ABORTED, job.getState());
    }

    @Test
    public void testReplicatingFailed() throws Exception {
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, job.getState());

        job.run();

        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(job, "runningTasks");

        Assertions.assertEquals(ReplicationJobState.REPLICATING, job.getState());

        for (AgentTask task : runningTasks.values()) {
            TFinishTaskRequest request = new TFinishTaskRequest();
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs("failed");
            request.setTask_status(status);
            job.finishReplicateSnapshotTask((ReplicateSnapshotTask) task, request);
        }

        job.run();
        Assertions.assertEquals(ReplicationJobState.ABORTED, job.getState());
    }
}
