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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/SparkLoadJobTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.LabelName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PushTask;
import com.starrocks.thrift.TEtlState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkLoadJobTest {
    private long dbId;
    private String dbName;
    private String tableName;
    private String label;
    private String resourceName;
    private String broker;
    private long transactionId;
    private long pendingTaskId;
    private SparkLoadAppHandle sparkLoadAppHandle;
    private String appId;
    private String etlOutputPath;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;
    private int schemaHash;

    @Before
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        label = "label0";
        resourceName = "spark0";
        broker = "broker0";
        transactionId = 2L;
        pendingTaskId = 3L;
        sparkLoadAppHandle = new SparkLoadAppHandle();
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/starrocks/100/label/101";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
        schemaHash = 146886;
    }

    @Test
    public void testCreateFromLoadStmt(@Mocked GlobalStateMgr globalStateMgr, @Injectable LoadStmt loadStmt,
                                       @Injectable DataDescription dataDescription, @Injectable LabelName labelName,
                                       @Injectable Database db, @Injectable OlapTable olapTable,
                                       @Injectable ResourceMgr resourceMgr) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("spark.executor.memory", "1g");
        resourceProperties.put("broker", broker);
        resourceProperties.put("broker.username", "user0");
        resourceProperties.put("broker.password", "password0");
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, resourceProperties);
        Map<String, String> jobProperties = Maps.newHashMap();
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                globalStateMgr.getDb(dbName);
                result = db;
                globalStateMgr.getResourceMgr();
                result = resourceMgr;
                db.getTable(tableName);
                result = olapTable;
                db.getId();
                result = dbId;
                loadStmt.getLabel();
                result = labelName;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                loadStmt.getResourceDesc();
                result = resourceDesc;
                loadStmt.getProperties();
                result = jobProperties;
                loadStmt.getEtlJobType();
                result = EtlJobType.SPARK;
                labelName.getDbName();
                result = dbName;
                labelName.getLabelName();
                result = label;
                dataDescription.getTableName();
                result = tableName;
                dataDescription.getPartitionNames();
                result = null;
                resourceMgr.getResource(resourceName);
                result = resource;
            }
        };

        try {
            Assert.assertTrue(resource.getSparkConfigs().isEmpty());
            resourceDesc.analyze();
            BulkLoadJob bulkLoadJob = BulkLoadJob.fromLoadStmt(loadStmt, null);
            SparkLoadJob sparkLoadJob = (SparkLoadJob) bulkLoadJob;
            // check member
            Assert.assertEquals(dbId, bulkLoadJob.dbId);
            Assert.assertEquals(label, bulkLoadJob.label);
            Assert.assertEquals(JobState.PENDING, bulkLoadJob.getState());
            Assert.assertEquals(EtlJobType.SPARK, bulkLoadJob.getJobType());
            Assert.assertEquals(resourceName, sparkLoadJob.getResourceName());
            Assert.assertEquals(-1L, sparkLoadJob.getEtlStartTimestamp());

            // check update spark resource properties
            Assert.assertEquals(broker, bulkLoadJob.brokerDesc.getName());
            Assert.assertEquals("user0", bulkLoadJob.brokerDesc.getProperties().get("username"));
            Assert.assertEquals("password0", bulkLoadJob.brokerDesc.getProperties().get("password"));
            SparkResource sparkResource = Deencapsulation.getField(sparkLoadJob, "sparkResource");
            Assert.assertTrue(sparkResource.getSparkConfigs().containsKey("spark.executor.memory"));
            Assert.assertEquals("1g", sparkResource.getSparkConfigs().get("spark.executor.memory"));
        } catch (DdlException | AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked GlobalStateMgr globalStateMgr, @Mocked SparkLoadPendingTask pendingTask,
                            @Injectable String originStmt, @Injectable GlobalTransactionMgr transactionMgr,
                            @Injectable LeaderTaskExecutor executor) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.beginTransaction(dbId, Lists.newArrayList(), label, null,
                        (TransactionState.TxnCoordinator) any, LoadJobSourceType.FRONTEND,
                        anyLong, anyLong);
                result = transactionId;
                pendingTask.init();
                pendingTask.getSignature();
                result = pendingTaskId;
                globalStateMgr.getPendingLoadTaskScheduler();
                result = executor;
                executor.submit((SparkLoadPendingTask) any);
                result = true;
            }
        };

        ResourceDesc resourceDesc = new ResourceDesc(resourceName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, resourceDesc, new OriginStatement(originStmt, 0));
        job.execute();

        // check transaction id and id to tasks
        Assert.assertEquals(transactionId, job.getTransactionId());
        Assert.assertTrue(job.idToTasks.containsKey(pendingTaskId));
    }

    @Test
    public void testOnPendingTaskFinished(@Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt)
            throws MetaNotFoundException {
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, resourceDesc, new OriginStatement(originStmt, 0));
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        attachment.setAppId(appId);
        attachment.setOutputPath(etlOutputPath);
        job.onTaskFinished(attachment);

        // check pending task finish
        Assert.assertTrue(job.finishedTaskIds.contains(pendingTaskId));
        Assert.assertEquals(appId, Deencapsulation.getField(job, "appId"));
        Assert.assertEquals(etlOutputPath, Deencapsulation.getField(job, "etlOutputPath"));
        Assert.assertEquals(JobState.ETL, job.getState());
    }

    private SparkLoadJob getEtlStateJob(String originStmt) throws MetaNotFoundException {
        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        SparkLoadJob job = new SparkLoadJob(dbId, label, null, new OriginStatement(originStmt, 0));
        job.state = JobState.ETL;
        job.maxFilterRatio = 0.15;
        job.transactionId = transactionId;
        Deencapsulation.setField(job, "appId", appId);
        Deencapsulation.setField(job, "etlOutputPath", etlOutputPath);
        Deencapsulation.setField(job, "sparkResource", resource);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        job.brokerDesc = brokerDesc;
        return job;
    }

    @Test
    public void testUpdateEtlStatusRunning(@Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt,
                                           @Mocked SparkEtlJobHandler handler) throws Exception {
        String trackingUrl = "http://127.0.0.1:8080/proxy/application_1586619723848_0088/";
        int progress = 66;
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.RUNNING);
        status.setTrackingUrl(trackingUrl);
        status.setProgress(progress);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl running
        Assert.assertEquals(JobState.ETL, job.getState());
        Assert.assertEquals(progress, job.progress);
        Assert.assertEquals(trackingUrl, job.loadingStatus.getTrackingUrl());
    }

    @Test(expected = LoadException.class)
    public void testUpdateEtlStatusCancelled(@Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt,
                                             @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.CANCELLED);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();
    }

    @Test(expected = DataQualityException.class)
    public void testUpdateEtlStatusFinishedQualityFailed(@Mocked GlobalStateMgr globalStateMgr,
                                                         @Injectable String originStmt,
                                                         @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "8");
        status.getCounters().put("dpp.abnorm.ALL", "2");

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                        (SparkResource) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();
    }

    @Test
    public void testUpdateEtlStatusFinishedAndCommitTransaction(
            @Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt,
            @Mocked SparkEtlJobHandler handler, @Mocked AgentTaskExecutor executor,
            @Injectable Database db, @Injectable OlapTable table, @Injectable Partition partition,
            @Injectable MaterializedIndex index, @Injectable LocalTablet tablet, @Injectable Replica replica,
            @Injectable GlobalTransactionMgr transactionMgr) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "9");
        status.getCounters().put("dpp.abnorm.ALL", "1");
        Map<String, Long> filePathToSize = Maps.newHashMap();
        String filePath =
                String.format("hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9/V1.label6.%d.%d.%d.0.%d.parquet",
                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        filePathToSize.put(filePath, fileSize);
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.addPartition(partitionId, null, (short) 1, false);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                        (SparkResource) any, (BrokerDesc) any);
                result = status;
                handler.getEtlFilePaths(etlOutputPath, (BrokerDesc) any);
                result = filePathToSize;
                globalStateMgr.getDb(dbId);
                result = db;
                db.getTable(tableId);
                result = table;
                table.getPartition(partitionId);
                result = partition;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getSchemaByIndexId(Long.valueOf(12));
                result = Lists.newArrayList(new Column("k1", Type.VARCHAR));
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                index.getId();
                result = indexId;
                index.getTablets();
                result = Lists.newArrayList(tablet);
                tablet.getId();
                result = tabletId;
                tablet.getImmutableReplicas();
                result = Lists.newArrayList(replica);
                replica.getId();
                result = replicaId;
                replica.getBackendId();
                result = backendId;
                replica.getLastFailedVersion();
                result = -1;
                AgentTaskExecutor.submit((AgentBatchTask) any);
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.commitTransaction(dbId, transactionId, (List<TabletCommitInfo>) any, (List<TabletFailInfo>) any,
                        (LoadJobFinalOperation) any);
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl finished
        Assert.assertEquals(JobState.LOADING, job.getState());
        Assert.assertEquals(0, job.progress);
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Deencapsulation.getField(job, "tabletMetaToFileInfo");
        Assert.assertEquals(1, tabletMetaToFileInfo.size());
        String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
        Assert.assertTrue(tabletMetaToFileInfo.containsKey(tabletMetaStr));
        Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
        Assert.assertEquals(filePath, fileInfo.first);
        Assert.assertEquals(fileSize, (long) fileInfo.second);
        Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask
                = Deencapsulation.getField(job, "tabletToSentReplicaPushTask");
        Assert.assertTrue(tabletToSentReplicaPushTask.containsKey(tabletId));
        Assert.assertTrue(tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId));
        Map<Long, Set<Long>> tableToLoadPartitions = Deencapsulation.getField(job, "tableToLoadPartitions");
        Assert.assertTrue(tableToLoadPartitions.containsKey(tableId));
        Assert.assertTrue(tableToLoadPartitions.get(tableId).contains(partitionId));
        Map<Long, Integer> indexToSchemaHash = Deencapsulation.getField(job, "indexToSchemaHash");
        Assert.assertTrue(indexToSchemaHash.containsKey(indexId));
        Assert.assertEquals(schemaHash, (long) indexToSchemaHash.get(indexId));

        // finish push task
        job.addFinishedReplica(replicaId, tabletId, backendId);
        job.updateLoadingStatus();
        Assert.assertEquals(99, job.progress);
        Set<Long> fullTablets = Deencapsulation.getField(job, "fullTablets");
        Assert.assertTrue(fullTablets.contains(tabletId));
    }

    @Test
    public void testUpdateEtlStatusFinishedAndCommitTransactionForLake(
            @Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt,
            @Mocked SparkEtlJobHandler handler, @Mocked AgentTaskExecutor executor,
            @Injectable Database db, @Injectable LakeTable table, @Injectable Partition partition,
            @Injectable MaterializedIndex index, @Injectable LakeTablet tablet, @Injectable Replica replica,
            @Injectable GlobalTransactionMgr transactionMgr) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "9");
        status.getCounters().put("dpp.abnorm.ALL", "1");
        Map<String, Long> filePathToSize = Maps.newHashMap();
        String filePath =
                String.format("hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9/V1.label6.%d.%d.%d.0.%d.parquet",
                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        filePathToSize.put(filePath, fileSize);
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.addPartition(partitionId, null, (short) 1, false);

        List<Replica> allQueryableReplicas = Lists.newArrayList();
        allQueryableReplicas.add(replica);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkLoadAppHandle) any, appId, anyLong, etlOutputPath,
                        (SparkResource) any, (BrokerDesc) any);
                result = status;
                handler.getEtlFilePaths(etlOutputPath, (BrokerDesc) any);
                result = filePathToSize;
                globalStateMgr.getDb(dbId);
                result = db;
                db.getTable(tableId);
                result = table;
                table.getPartition(partitionId);
                result = partition;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getSchemaByIndexId(Long.valueOf(12));
                result = Lists.newArrayList(new Column("k1", Type.VARCHAR));
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                index.getId();
                result = indexId;
                index.getTablets();
                result = Lists.newArrayList(tablet);
                tablet.getId();
                result = tabletId;
                ((LakeTablet) tablet).getPrimaryComputeNodeId();
                result = backendId;
                AgentTaskExecutor.submit((AgentBatchTask) any);
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                result = transactionMgr;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl finished
        Assert.assertEquals(JobState.LOADING, job.getState());
        Assert.assertEquals(0, job.progress);
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Deencapsulation.getField(job, "tabletMetaToFileInfo");
        Assert.assertEquals(1, tabletMetaToFileInfo.size());
        String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
        Assert.assertTrue(tabletMetaToFileInfo.containsKey(tabletMetaStr));
        Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
        Assert.assertEquals(filePath, fileInfo.first);
        Assert.assertEquals(fileSize, (long) fileInfo.second);
        Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask
                = Deencapsulation.getField(job, "tabletToSentReplicaPushTask");
        Assert.assertTrue(tabletToSentReplicaPushTask.containsKey(tabletId));
        Assert.assertTrue(tabletToSentReplicaPushTask.get(tabletId).containsKey(tabletId));
        Map<Long, Set<Long>> tableToLoadPartitions = Deencapsulation.getField(job, "tableToLoadPartitions");
        Assert.assertTrue(tableToLoadPartitions.containsKey(tableId));
        Assert.assertTrue(tableToLoadPartitions.get(tableId).contains(partitionId));
        Map<Long, Integer> indexToSchemaHash = Deencapsulation.getField(job, "indexToSchemaHash");
        Assert.assertTrue(indexToSchemaHash.containsKey(indexId));
        Assert.assertEquals(schemaHash, (long) indexToSchemaHash.get(indexId));

        // finish push task
        job.addFinishedReplica(tableId, tabletId, backendId);
        job.updateLoadingStatus();
    }

    @Test
    public void testSubmitTasksWhenStateFinished(@Mocked GlobalStateMgr globalStateMgr, @Injectable String originStmt,
                                                 @Injectable Database db) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getDb(dbId);
                result = db;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.state = JobState.FINISHED;
        Set<Long> totalTablets = Deencapsulation.invoke(job, "submitPushTasks");
        Assert.assertTrue(totalTablets.isEmpty());
    }

    @Test
    public void testStateUpdateInfoPersist() throws IOException {
        String fileName = "./testStateUpdateInfoPersistFile";
        File file = new File(fileName);

        // etl state
        long id = 1L;
        JobState state = JobState.ETL;
        long etlStartTimestamp = 1592366666L;
        long loadStartTimestamp = -1;
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        SparkLoadJobStateUpdateInfo info = new SparkLoadJobStateUpdateInfo(
                id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp, appId, etlOutputPath,
                loadStartTimestamp, tabletMetaToFileInfo);
        info.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SparkLoadJobStateUpdateInfo replayedInfo = (SparkLoadJobStateUpdateInfo) LoadJobStateUpdateInfo.read(in);
        Assert.assertEquals(id, replayedInfo.getJobId());
        Assert.assertEquals(state, replayedInfo.getState());
        Assert.assertEquals(transactionId, replayedInfo.getTransactionId());
        Assert.assertEquals(loadStartTimestamp, replayedInfo.getLoadStartTimestamp());
        Assert.assertEquals(etlStartTimestamp, replayedInfo.getEtlStartTimestamp());
        Assert.assertEquals(appId, replayedInfo.getAppId());
        Assert.assertEquals(etlOutputPath, replayedInfo.getEtlOutputPath());
        Assert.assertTrue(replayedInfo.getTabletMetaToFileInfo().isEmpty());
        in.close();

        // loading state
        state = JobState.LOADING;
        loadStartTimestamp = 1592388888L;
        String tabletMeta = String.format("%d.%d.%d.0.%d", tableId, partitionId, indexId, schemaHash);
        String filePath =
                String.format("hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9/V1.label6.%d.%d.%d.0.%d.parquet",
                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        tabletMetaToFileInfo.put(tabletMeta, Pair.create(filePath, fileSize));

        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        out = new DataOutputStream(new FileOutputStream(file));
        info = new SparkLoadJobStateUpdateInfo(id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp,
                appId, etlOutputPath, loadStartTimestamp, tabletMetaToFileInfo);
        info.write(out);
        out.flush();
        out.close();

        in = new DataInputStream(new FileInputStream(file));
        replayedInfo = (SparkLoadJobStateUpdateInfo) LoadJobStateUpdateInfo.read(in);
        Assert.assertEquals(state, replayedInfo.getState());
        Assert.assertEquals(loadStartTimestamp, replayedInfo.getLoadStartTimestamp());
        Map<String, Pair<String, Long>> replayedTabletMetaToFileInfo = replayedInfo.getTabletMetaToFileInfo();
        Assert.assertEquals(1, replayedTabletMetaToFileInfo.size());
        Assert.assertTrue(replayedTabletMetaToFileInfo.containsKey(tabletMeta));
        Pair<String, Long> replayedFileInfo = replayedTabletMetaToFileInfo.get(tabletMeta);
        Assert.assertEquals(filePath, replayedFileInfo.first);
        Assert.assertEquals(fileSize, (long) replayedFileInfo.second);
        in.close();

        // delete file
        if (file.exists()) {
            file.delete();
        }
    }
}
