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

package com.starrocks.persist.image;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.loadv2.SparkEtlJobHandler;
import com.starrocks.load.loadv2.SparkLoadAppHandle;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.persist.BrokerPropertiesPersistInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TEtlState;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.LoadJobProtoTest.resource;
import static com.starrocks.persist.image.LoadJobProtoTest.spark;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkLoadRecoveryImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private static final String APP = "application_123_456";
    private static final String OUTPUT = "hdfs://cluster/output/103";
    private static final String FILE = OUTPUT + "/V1.label.20.30.40.0.50.parquet";

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    private SparkLoadJob source(JobState state) {
        SparkLoadJob source = spark(103, state);
        setField(source, "sparkResource", resource());
        setField(source, "brokerPersistInfo", new BrokerPropertiesPersistInfo("broker", Map.of("username", "user")));
        setField(source, "appId", APP);
        setField(source, "etlOutputPath", OUTPUT);
        setField(source, "etlStartTimestamp", 1234L);
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        setField(handle, "appId", APP);
        setField(handle, "state", SparkLoadAppHandle.State.RUNNING);
        setField(source, "sparkLoadAppHandle", handle);
        setField(source, "maxFilterRatio", 0.2);
        return source;
    }

    private SparkLoadJob restore(SparkLoadJob source) throws Exception {
        LoadMgr sourceManager = LoadMgrImageV3Test.manager(List.of(source));
        Path file = LoadImageTestSupport.save(tempDir, sourceManager::save);
        LoadMgr target = LoadMgrImageV3Test.manager(List.of());
        LoadImageTestSupport.transactions(global);
        LoadImageTestSupport.load(file, target.getImageV3Loaders());
        return (SparkLoadJob) target.getLoadJob(103);
    }

    @Test
    public void testRestoredEtlJobPollsItsSavedApplication() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long id, String output,
                                            SparkResource resource, BrokerDesc broker) {
                assertEquals(103, id);
                assertEquals(APP, appId);
                assertEquals(APP, handle.getAppId());
                assertEquals(OUTPUT, output);
                assertEquals("spark", resource.getName());
                assertEquals("broker", broker.getName());
                assertEquals("user", broker.getProperties().get("username"));
                polls.incrementAndGet();
                EtlStatus status = new EtlStatus();
                status.setState(TEtlState.RUNNING);
                status.setTrackingUrl("https://yarn/" + APP);
                status.setProgress(66);
                return status;
            }
        };
        SparkLoadJob job = restore(source(JobState.ETL));
        assertEquals(0, job.getProgress());
        job.updateEtlStatus();
        assertEquals(JobState.ETL, job.getState());
        assertEquals(66, job.getProgress());
        EtlStatus status = getField(job, "loadingStatus");
        assertEquals("https://yarn/" + APP, status.getTrackingUrl());

        assertEquals(1, polls.get());
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testRestoredEtlJobStillChecksDataQuality() throws Exception {
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long id, String output,
                                            SparkResource resource, BrokerDesc broker) {
                EtlStatus status = new EtlStatus();
                status.setState(TEtlState.FINISHED);
                status.getCounters().put("dpp.norm.ALL", "1");
                status.getCounters().put("dpp.abnorm.ALL", "9");
                return status;
            }
        };
        SparkLoadJob job = restore(source(JobState.ETL));
        assertThrows(com.starrocks.common.DataQualityException.class, job::updateEtlStatus);
        assertEquals(JobState.ETL, job.getState());
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    private void loadingCatalog() {
        LocalMetastore metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Mockito.when(global.getLockManager()).thenReturn(new LockManager());
        Database db = new Database(10, "db");
        Mockito.when(metastore.getDb(10)).thenReturn(db);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(metastore.getTable(10L, 20L)).thenReturn(table);
        PhysicalPartition partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(table.getPhysicalPartition(30)).thenReturn(partition);
        Mockito.when(table.getPartitionInfo()).thenReturn(Mockito.mock(PartitionInfo.class));
        // A partition without tablets lets the real checker traverse the rebuilt partition index
        // without a BE RPC. Push-task execution is covered by the existing SparkLoadJob tests.
        Mockito.when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)).thenReturn(List.of());
    }

    @Test
    public void testEtlFinishAfterImageBuildsLoadingIndexes() throws Exception {
        loadingCatalog();
        EditLog editLog = global.getEditLog();
        Mockito.doAnswer(call -> {
            WALApplier apply = call.getArgument(1);
            apply.apply(call.getArgument(0));
            return null;
        }).when(editLog).logUpdateLoadJob(Mockito.any(), Mockito.any());
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long id, String output,
                                            SparkResource resource, BrokerDesc broker) {
                EtlStatus status = new EtlStatus();
                status.setState(TEtlState.FINISHED);
                status.getCounters().put("dpp.norm.ALL", "10");
                status.getCounters().put("dpp.abnorm.ALL", "0");
                return status;
            }

            @Mock
            public Map<String, Long> getEtlFilePaths(String output, BrokerDesc broker) {
                assertEquals(OUTPUT, output);
                assertEquals("broker", broker.getName());
                return Map.of(FILE, 123L, OUTPUT + "/_SUCCESS", 0L);
            }
        };
        SparkLoadJob job = restore(source(JobState.ETL));
        job.updateEtlStatus();
        assertEquals(JobState.LOADING, job.getState());
        assertEquals(Map.of("20.30.40.0.50", Pair.create(FILE, 123L)), getField(job, "tabletMetaToFileInfo"));
        assertEquals(Map.of(20L, Set.of(30L)), getField(job, "tableToLoadPartitions"));
        assertEquals(Map.of(40L, 50), getField(job, "indexMetaIdToSchemaHash"));
        assertTrue((long) getField(job, "loadStartTimestamp") > 0);
        assertNotNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(103));

    }

    @Test
    public void testLoadingJournalAfterImageRebuildsTransientIndexes() throws Exception {
        loadingCatalog();
        SparkLoadJob source = source(JobState.ETL);
        SparkLoadJob job = restore(source);
        SparkLoadAppHandle handle = getField(job, "sparkLoadAppHandle");
        Map<String, Pair<String, Long>> files = new HashMap<>(Map.of("20.30.40.0.50", Pair.create(FILE, 123L)));
        job.replayUpdateStateInfo(new SparkLoadJob.SparkLoadJobStateUpdateInfo(103, JobState.LOADING, 1000,
                handle, 1234, APP, OUTPUT, 2345, files));
        assertEquals(JobState.LOADING, job.getState());
        assertEquals(Map.of(20L, Set.of(30L)), getField(job, "tableToLoadPartitions"));
        assertEquals(Map.of(40L, 50), getField(job, "indexMetaIdToSchemaHash"));
        job.updateLoadingStatus();
        assertEquals(JobState.LOADING, job.getState());
        assertEquals(1000L, (long) getField(job, "transactionId"));

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testCheckerDoesNotPollAnAlreadyCommittedJob() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long id, String output,
                                            SparkResource resource, BrokerDesc broker) {
                polls.incrementAndGet();
                throw new AssertionError("committed job must not poll ETL");
            }
        };
        SparkLoadJob job = restore(source(JobState.COMMITTED));
        job.updateEtlStatus();
        job.updateLoadingStatus();
        assertEquals(0, polls.get());
        assertEquals(JobState.COMMITTED, job.getState());
    }
}
