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
import com.starrocks.persist.ReplicationJobLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ReplicationJobSerializationTest {

    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;
    private static OlapTable srcTable;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_NOTHING);
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test_serialization").useDatabase("test_serialization");

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_serialization");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");
        srcTable = DeepCopy.copyWithGson(table, OlapTable.class);

        Partition partition = table.getPartitions().iterator().next();
        Partition srcPartition = srcTable.getPartitions().iterator().next();
        partition.getDefaultPhysicalPartition().updateVersionForRestore(10);
        srcPartition.getDefaultPhysicalPartition().updateVersionForRestore(100);
        partition.getDefaultPhysicalPartition().setDataVersion(8);
        partition.getDefaultPhysicalPartition().setNextDataVersion(9);
        srcPartition.getDefaultPhysicalPartition().setDataVersion(98);
        srcPartition.getDefaultPhysicalPartition().setNextDataVersion(99);

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
            }
        };
    }

    @Test
    public void testReplicationJobSerialization() {
        // Create a ReplicationJob
        ReplicationJob job = new ReplicationJob(null, "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        // Serialize to JSON
        String json = GsonUtils.GSON.toJson(job);

        // Verify JSON contains clazz field for type identification
        Assertions.assertTrue(json.contains("\"clazz\":\"ReplicationJob\""),
                "Serialized JSON should contain clazz field for ReplicationJob");

        // Deserialize back
        ReplicationJob deserializedJob = GsonUtils.GSON.fromJson(json, ReplicationJob.class);

        // Verify deserialized object
        Assertions.assertNotNull(deserializedJob);
        Assertions.assertEquals(job.getJobId(), deserializedJob.getJobId());
        Assertions.assertEquals(job.getDatabaseId(), deserializedJob.getDatabaseId());
        Assertions.assertEquals(job.getTableId(), deserializedJob.getTableId());
        Assertions.assertEquals(job.getState(), deserializedJob.getState());
        Assertions.assertEquals(ReplicationJob.class, deserializedJob.getClass());
    }

    @Test
    public void testLakeReplicationJobSerialization() {
        // Create a LakeReplicationJob
        long virtualTabletId = 12345L;
        long srcDatabaseId = 67890L;
        long srcTableId = 11111L;
        LakeReplicationJob job = new LakeReplicationJob("test_job_id", virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        // Serialize to JSON
        String json = GsonUtils.GSON.toJson(job);

        // Verify JSON contains clazz field for type identification
        Assertions.assertTrue(json.contains("\"clazz\":\"LakeReplicationJob\""),
                "Serialized JSON should contain clazz field for LakeReplicationJob");

        // Verify subclass-specific fields are serialized
        Assertions.assertTrue(json.contains("\"virtualTabletId\":" + virtualTabletId),
                "Serialized JSON should contain virtualTabletId");
        Assertions.assertTrue(json.contains("\"srcDatabaseId\":" + srcDatabaseId),
                "Serialized JSON should contain srcDatabaseId");
        Assertions.assertTrue(json.contains("\"srcTableId\":" + srcTableId),
                "Serialized JSON should contain srcTableId");

        // Deserialize back as base class type (simulates ReplicationJobLog behavior)
        ReplicationJob deserializedJob = GsonUtils.GSON.fromJson(json, ReplicationJob.class);

        // Verify it's actually a LakeReplicationJob instance
        Assertions.assertNotNull(deserializedJob);
        Assertions.assertTrue(deserializedJob instanceof LakeReplicationJob,
                "Deserialized object should be instance of LakeReplicationJob");
        Assertions.assertEquals(LakeReplicationJob.class, deserializedJob.getClass());

        // Verify common fields
        Assertions.assertEquals(job.getJobId(), deserializedJob.getJobId());
        Assertions.assertEquals(job.getDatabaseId(), deserializedJob.getDatabaseId());
        Assertions.assertEquals(job.getTableId(), deserializedJob.getTableId());
        Assertions.assertEquals(job.getState(), deserializedJob.getState());
    }

    @Test
    public void testReplicationJobLogWithLakeReplicationJob() {
        // Create a LakeReplicationJob
        long virtualTabletId = 12345L;
        long srcDatabaseId = 67890L;
        long srcTableId = 11111L;
        LakeReplicationJob lakeJob = new LakeReplicationJob("test_lake_job_id", virtualTabletId, srcDatabaseId, srcTableId,
                db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        // Wrap in ReplicationJobLog (simulates EditLog behavior)
        ReplicationJobLog jobLog = new ReplicationJobLog(lakeJob);

        // Serialize ReplicationJobLog to JSON
        String json = GsonUtils.GSON.toJson(jobLog);

        // Verify JSON contains type information for the nested job
        Assertions.assertTrue(json.contains("\"clazz\":\"LakeReplicationJob\""),
                "Serialized ReplicationJobLog should preserve LakeReplicationJob type");

        // Deserialize back
        ReplicationJobLog deserializedJobLog = GsonUtils.GSON.fromJson(json, ReplicationJobLog.class);

        // Verify the deserialized job is LakeReplicationJob
        ReplicationJob deserializedJob = deserializedJobLog.getReplicationJob();
        Assertions.assertNotNull(deserializedJob);
        Assertions.assertTrue(deserializedJob instanceof LakeReplicationJob,
                "Deserialized job from ReplicationJobLog should be LakeReplicationJob");
        Assertions.assertEquals(LakeReplicationJob.class, deserializedJob.getClass());

        // Verify all fields are preserved
        Assertions.assertEquals(lakeJob.getJobId(), deserializedJob.getJobId());
        Assertions.assertEquals(lakeJob.getDatabaseId(), deserializedJob.getDatabaseId());
        Assertions.assertEquals(lakeJob.getTableId(), deserializedJob.getTableId());
        Assertions.assertEquals(lakeJob.getState(), deserializedJob.getState());
    }

    @Test
    public void testReplicationJobLogWithBaseReplicationJob() {
        // Create a base ReplicationJob
        ReplicationJob job = new ReplicationJob("test_base_job_id", "test_token", db.getId(), table, srcTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        // Wrap in ReplicationJobLog
        ReplicationJobLog jobLog = new ReplicationJobLog(job);

        // Serialize ReplicationJobLog to JSON
        String json = GsonUtils.GSON.toJson(jobLog);

        // Verify JSON contains type information
        Assertions.assertTrue(json.contains("\"clazz\":\"ReplicationJob\""),
                "Serialized ReplicationJobLog should contain ReplicationJob type");

        // Deserialize back
        ReplicationJobLog deserializedJobLog = GsonUtils.GSON.fromJson(json, ReplicationJobLog.class);

        // Verify the deserialized job is base ReplicationJob
        ReplicationJob deserializedJob = deserializedJobLog.getReplicationJob();
        Assertions.assertNotNull(deserializedJob);
        Assertions.assertEquals(ReplicationJob.class, deserializedJob.getClass());
        Assertions.assertFalse(deserializedJob instanceof LakeReplicationJob,
                "Deserialized base ReplicationJob should not be LakeReplicationJob");

        // Verify fields
        Assertions.assertEquals(job.getJobId(), deserializedJob.getJobId());
        Assertions.assertEquals(job.getDatabaseId(), deserializedJob.getDatabaseId());
        Assertions.assertEquals(job.getTableId(), deserializedJob.getTableId());
    }

    @Test
    public void testBackwardCompatibilityWithoutClazzField() {
        // Simulate old serialized data without clazz field
        // This tests backward compatibility - old data should deserialize as base ReplicationJob
        // Note: ReplicationJobState is serialized as {"id":1} not as a string
        String oldFormatJson = "{\"jobId\":\"old_job_id\",\"createdTimeMs\":1234567890," +
                "\"finishedTimeMs\":0,\"srcToken\":\"token\"," +
                "\"databaseId\":" + db.getId() + ",\"tableId\":" + table.getId() + "," +
                "\"tableType\":\"OLAP\",\"srcTableType\":\"OLAP\"," +
                "\"replicationDataSize\":0,\"replicationReplicaCount\":0," +
                "\"partitionInfos\":{},\"transactionId\":0,\"state\":{\"id\":1}}";

        // Deserialize old format JSON - should succeed and create ReplicationJob
        ReplicationJob deserializedJob = GsonUtils.GSON.fromJson(oldFormatJson, ReplicationJob.class);

        Assertions.assertNotNull(deserializedJob);
        Assertions.assertEquals(ReplicationJob.class, deserializedJob.getClass());
        Assertions.assertEquals("old_job_id", deserializedJob.getJobId());
        Assertions.assertEquals(ReplicationJobState.INITIALIZING, deserializedJob.getState());
    }
}

