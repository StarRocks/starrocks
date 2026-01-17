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

import com.google.gson.Gson;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TReplicateSnapshotRequest;
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

    @Test
    public void testPartitionFullPathWithoutFilePathInfo() throws Exception {
        // Test with no FilePathInfo (fallback - no full path)
        long virtualTabletId = 1000;
        long srcDatabaseId = 100;
        long srcTableId = 100;
        LakeReplicationJob jobWithoutPathInfo = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        Assertions.assertEquals(ReplicationJobState.INITIALIZING, jobWithoutPathInfo.getState());

        jobWithoutPathInfo.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, jobWithoutPathInfo.getState());

        // Verify that tasks are created without full path (BE will use RemoteStarletLocationProvider)
        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(jobWithoutPathInfo, "runningTasks");
        for (AgentTask task : runningTasks.values()) {
            ReplicateSnapshotTask snapshotTask = (ReplicateSnapshotTask) task;
            TReplicateSnapshotRequest thriftRequest = snapshotTask.toThrift();
            
            // Full path should NOT be set when partitioned prefix is not enabled
            Assertions.assertFalse(thriftRequest.isSetSrc_partition_full_path());
            
            // Verify toString includes full path info
            String taskStr = snapshotTask.toString();
            Assertions.assertTrue(taskStr.contains("src partition full path:"));
        }
    }

    @Test
    public void testPartitionFullPathWithPartitionedPrefixEnabled() throws Exception {
        // Test with FilePathInfo that has partitioned prefix enabled
        long virtualTabletId = 1000;
        long srcDatabaseId = 111;
        long srcTableId = 222;
        
        // Create a FilePathInfo with partitioned prefix enabled
        FilePathInfo.Builder pathInfoBuilder = FilePathInfo.newBuilder();
        pathInfoBuilder.getFsInfoBuilder().getS3FsInfoBuilder()
                .setBucket("test-bucket")
                .setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(1024);
        pathInfoBuilder.getFsInfoBuilder()
                .setFsName("test-sv")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3);
        pathInfoBuilder.setFullPath("s3://test-bucket/service_id/db111/table222");
        
        LakeReplicationJob jobWithPathInfo = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                pathInfoBuilder.build());

        Assertions.assertEquals(ReplicationJobState.INITIALIZING, jobWithPathInfo.getState());

        jobWithPathInfo.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, jobWithPathInfo.getState());

        // Verify that tasks are created with full path
        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(jobWithPathInfo, "runningTasks");
        for (AgentTask task : runningTasks.values()) {
            ReplicateSnapshotTask snapshotTask = (ReplicateSnapshotTask) task;
            TReplicateSnapshotRequest thriftRequest = snapshotTask.toThrift();
            
            // Full path should be set when partitioned prefix is enabled
            Assertions.assertTrue(thriftRequest.isSetSrc_partition_full_path());
            String fullPath = thriftRequest.getSrc_partition_full_path();
            // Full path should start with s3:// and contain partitioned prefix
            Assertions.assertNotNull(fullPath);
            Assertions.assertTrue(fullPath.startsWith("s3://"));
        }
    }

    @Test
    public void testPartitionFullPathWithPartitionedPrefixDisabled() throws Exception {
        // Test with FilePathInfo that has partitioned prefix disabled (S3)
        // Full path should still be provided for S3, just without the prefix calculation
        long virtualTabletId = 1000;
        long srcDatabaseId = 111;
        long srcTableId = 222;
        
        // Create a FilePathInfo with partitioned prefix DISABLED
        FilePathInfo.Builder pathInfoBuilder = FilePathInfo.newBuilder();
        pathInfoBuilder.getFsInfoBuilder().getS3FsInfoBuilder()
                .setBucket("test-bucket")
                .setPartitionedPrefixEnabled(false);
        pathInfoBuilder.getFsInfoBuilder()
                .setFsName("test-sv")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3);
        pathInfoBuilder.setFullPath("s3://test-bucket/service_id/db111/table222");
        
        LakeReplicationJob jobWithPathInfo = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                pathInfoBuilder.build());

        Assertions.assertEquals(ReplicationJobState.INITIALIZING, jobWithPathInfo.getState());

        jobWithPathInfo.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, jobWithPathInfo.getState());

        // Verify that tasks are created WITH full path (S3 always provides full path)
        // The full path is simple concatenation without partitioned prefix calculation
        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(jobWithPathInfo, "runningTasks");
        for (AgentTask task : runningTasks.values()) {
            ReplicateSnapshotTask snapshotTask = (ReplicateSnapshotTask) task;
            TReplicateSnapshotRequest thriftRequest = snapshotTask.toThrift();
            
            // Full path should be set for S3 storage type (even with partitioned prefix disabled)
            Assertions.assertTrue(thriftRequest.isSetSrc_partition_full_path());
            String fullPath = thriftRequest.getSrc_partition_full_path();
            // Path should be simple concatenation: tableFullPath + "/" + partitionId
            Assertions.assertTrue(fullPath.startsWith("s3://test-bucket/service_id/db111/table222/"));
        }
    }

    @Test
    public void testSerializationWithFilePathInfo() throws Exception {
        // Test serialization and deserialization of LakeReplicationJob with FilePathInfo
        // This is critical for crash recovery scenario
        long virtualTabletId = 1000;
        long srcDatabaseId = 111;
        long srcTableId = 222;
        
        // Create a FilePathInfo with partitioned prefix enabled
        FilePathInfo.Builder pathInfoBuilder = FilePathInfo.newBuilder();
        pathInfoBuilder.getFsInfoBuilder().getS3FsInfoBuilder()
                .setBucket("test-bucket")
                .setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(1024);
        pathInfoBuilder.getFsInfoBuilder()
                .setFsName("test-sv")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3);
        pathInfoBuilder.setFullPath("s3://test-bucket/service_id/db111/table222");
        
        LakeReplicationJob originalJob = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                pathInfoBuilder.build());
        
        // Serialize the job
        Gson gson = GsonUtils.GSON;
        String json = gson.toJson(originalJob);
        
        // Deserialize the job (simulates crash recovery)
        LakeReplicationJob restoredJob = gson.fromJson(json, LakeReplicationJob.class);
        
        // Verify srcTableFilePathInfo is properly restored
        FilePathInfo restoredFilePathInfo = Deencapsulation.getField(restoredJob, "srcTableFilePathInfo");
        Assertions.assertNotNull(restoredFilePathInfo, 
                "srcTableFilePathInfo should be properly deserialized for crash recovery");
        Assertions.assertTrue(restoredFilePathInfo.hasFsInfo());
        Assertions.assertTrue(restoredFilePathInfo.getFsInfo().hasS3FsInfo());
        Assertions.assertTrue(restoredFilePathInfo.getFsInfo().getS3FsInfo().getPartitionedPrefixEnabled());
        Assertions.assertEquals("s3://test-bucket/service_id/db111/table222", restoredFilePathInfo.getFullPath());
    }

    @Test
    public void testSerializationWithNullFilePathInfo() throws Exception {
        // Test serialization with null FilePathInfo (original scenario without partitioned prefix)
        long virtualTabletId = 1000;
        long srcDatabaseId = 100;
        long srcTableId = 100;
        LakeReplicationJob originalJob = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        
        // Serialize the job
        Gson gson = GsonUtils.GSON;
        String json = gson.toJson(originalJob);
        
        // Deserialize the job
        LakeReplicationJob restoredJob = gson.fromJson(json, LakeReplicationJob.class);
        
        // Verify srcTableFilePathInfo is null (as expected for jobs without partitioned prefix)
        FilePathInfo restoredFilePathInfo = Deencapsulation.getField(restoredJob, "srcTableFilePathInfo");
        Assertions.assertNull(restoredFilePathInfo, 
                "srcTableFilePathInfo should be null when not set");
    }

    @Test
    public void testCrashRecoveryWithS3PartitionedPrefix() throws Exception {
        // Simulate a crash recovery scenario where job needs to re-send tasks
        long virtualTabletId = 1000;
        long srcDatabaseId = 111;
        long srcTableId = 222;
        
        // Create a FilePathInfo with partitioned prefix enabled
        FilePathInfo.Builder pathInfoBuilder = FilePathInfo.newBuilder();
        pathInfoBuilder.getFsInfoBuilder().getS3FsInfoBuilder()
                .setBucket("test-bucket")
                .setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(1024);
        pathInfoBuilder.getFsInfoBuilder()
                .setFsName("test-sv")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3);
        pathInfoBuilder.setFullPath("s3://test-bucket/service_id/db111/table222");
        
        LakeReplicationJob originalJob = new LakeReplicationJob(null, virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                pathInfoBuilder.build());
        
        // Run the job to start replication
        originalJob.run();
        Assertions.assertEquals(ReplicationJobState.REPLICATING, originalJob.getState());
        
        // Serialize the job (simulates checkpoint/image save before crash)
        Gson gson = GsonUtils.GSON;
        String json = gson.toJson(originalJob);
        
        // Deserialize the job (simulates loading from checkpoint after restart)
        LakeReplicationJob restoredJob = gson.fromJson(json, LakeReplicationJob.class);
        
        // After deserialization, run the job again (crash recovery path)
        // The job should be able to re-send tasks with correct S3 paths
        restoredJob.run();
        
        // Verify the job is still in REPLICATING state (tasks re-sent)
        Assertions.assertEquals(ReplicationJobState.REPLICATING, restoredJob.getState());
        
        // Verify tasks have correct S3 paths
        Map<AgentTask, AgentTask> runningTasks = Deencapsulation.getField(restoredJob, "runningTasks");
        Assertions.assertFalse(runningTasks.isEmpty(), "Tasks should be re-sent after crash recovery");
        
        for (AgentTask task : runningTasks.values()) {
            ReplicateSnapshotTask snapshotTask = (ReplicateSnapshotTask) task;
            TReplicateSnapshotRequest thriftRequest = snapshotTask.toThrift();
            
            // Full path should be set after crash recovery
            Assertions.assertTrue(thriftRequest.isSetSrc_partition_full_path(),
                    "S3 full path should be set after crash recovery with partitioned prefix");
            String fullPath = thriftRequest.getSrc_partition_full_path();
            Assertions.assertTrue(fullPath.startsWith("s3://"),
                    "S3 full path should start with s3:// after crash recovery");
        }
    }
}
