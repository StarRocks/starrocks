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

package com.starrocks.lake.snapshot;

import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
//import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ClusterSnapshotTask;
//import com.starrocks.thrift.TBackend;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class FullClusterSnapshotJobTest {
    @Mocked
    private EditLog editLog;

    private StarOSAgent starOSAgent = new StarOSAgent();
    private String storageVolumeName = StorageVolumeMgr.BUILTIN_STORAGE_VOLUME;
    private ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
    private boolean initSv = false;
    private AtomicLong nextId = new AtomicLong(0);

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        try {
            initStorageVolume();
        } catch (Exception ignore) {
        }

        new Expectations() {
            {
                editLog.logClusterSnapshotLog((ClusterSnapshotLog) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logClusterSnapshotLog(ClusterSnapshotLog log) {
                    }
                };
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }

            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }

            @Mock
            public long getNextId() {
                return nextId.incrementAndGet();
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public String getRawServiceId() {
                return "test-service-id";
            }

            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId) throws Exception {
                return FilePathInfo.newBuilder().setFullPath("s3://test-bucket/path").build();
            }

            @Mock
            public long createShardGroupForVirtualTablet() {
                return 1000L;
            }

            @Mock
            public void createShardWithVirtualTabletId(FilePathInfo pathInfo, FileCacheInfo cacheInfo,
                    long shardGroupId, Map<String, String> properties,
                    long vTabletId, String resourceGroupName) {
                // Mock implementation
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }

            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        new MockUp<HdfsFsManager>() {
            @Mock
            public void copyFromLocal(String srcPath, String destPath, Map<String, String> properties) {
                return;
            }

            @Mock
            public void deletePath(String path, Map<String, String> loadProperties) {
                return;
            }
        };

        new MockUp<ClusterSnapshotUtils>() {
            @Mock
            public void uploadClusterSnapshotToRemote(ClusterSnapshotJob job) throws StarRocksException {
                // Mock implementation
            }
        };

        new MockUp<AgentTaskQueue>() {
            @Mock
            public void addBatchTask(AgentBatchTask task) {
                // Mock implementation
            }
        };

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                // Mock implementation
            }
        };
    }

    private void initStorageVolume() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        if (!initSv) {
            List<String> locations = Arrays.asList("s3://abc");
            Map<String, String> storageParams = new HashMap<>();
            storageParams.put(AWS_S3_REGION, "region");
            storageParams.put(AWS_S3_ENDPOINT, "endpoint");
            storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
            String svKey = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .createStorageVolume(storageVolumeName, "S3", locations, storageParams, Optional.empty(), "");
            Assertions.assertEquals(true, GlobalStateMgr.getCurrentState().getStorageVolumeMgr().exists(storageVolumeName));
            Assertions.assertEquals(storageVolumeName,
                    GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeName(svKey));
            initSv = true;
        }
    }

    private SnapshotJobContext createSnapshotJobContext(CheckpointController feController,
                CheckpointController starMgrController) {
        return new SnapshotJobContext() {
            @Override
            public CheckpointController getFeController() {
                return feController;
            }

            @Override
            public CheckpointController getStarMgrController() {
                return starMgrController;
            }

            @Override
            public Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
                return Pair.create(100L, 200L);
            }
        };
    }


    @Test
    public void testConstructor() {
        long id = 1L;
        String snapshotName = "test_snapshot";
        long createdTimeMs = System.currentTimeMillis();

        FullClusterSnapshotJob job = new FullClusterSnapshotJob(id, snapshotName, storageVolumeName, createdTimeMs);

        Assertions.assertEquals(id, job.getId());
        Assertions.assertEquals(snapshotName, job.getSnapshotName());
        Assertions.assertEquals(storageVolumeName, job.getStorageVolumeName());
        Assertions.assertEquals(createdTimeMs, job.getCreatedTimeMs());
        Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, job.getState());
        Assertions.assertTrue(job.isInitializing());
    }
 
    @Test
    public void testRunInitializingJob() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        job.runInitializingJob(context);

        Assertions.assertEquals(ClusterSnapshotJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(100L, job.getFeJournalId());
        Assertions.assertEquals(200L, job.getStarMgrJournalId());
    }

    @Test
    public void testRunInitializingJobFailure() {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
    
        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
    
        SnapshotJobContext context = Mockito.mock(SnapshotJobContext.class);
        
        Mockito.when(context.getFeController()).thenReturn(feController);
        Mockito.when(context.getStarMgrController()).thenReturn(starMgrController);
        Mockito.when(context.captureConsistentCheckpointIdBetweenFEAndStarMgr()).thenReturn(null);
    
        Assertions.assertThrows(StarRocksException.class, () -> {
            job.runInitializingJob(context);
        });
    }
     
    @Test
    public void testRunSnapshottingJob() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().
                        getStorageVolumeByName(storageVolumeName);
        sv.setVTabletId(1000L);
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job.setJournalIds(100L, 200L);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        ClusterSnapshotInfo prevInfo = new ClusterSnapshotInfo(new HashMap<>());
        ClusterSnapshotInfo newInfo = new ClusterSnapshotInfo(new HashMap<>());

        new MockUp<CheckpointController>() {
            private int callCount = 0;

            @Mock
            public ClusterSnapshotInfo getClusterSnapshotInfo() {
                return callCount++ == 0 ? prevInfo : newInfo;
            }

            @Mock
            public long getImageJournalId() {
                return 50L;
            }

            @Mock
            public Pair<Boolean, String> runCheckpointControllerWithIds(long imageJournalId, long maxJournalId,
                    boolean needClusterSnapshotInfo) {
                return Pair.create(true, "");
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, Long tabletId) {
                ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
                return node;
            }
        };

        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(String warehouseName) {
                ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
                return node;
            }
        };

        job.runSnapshottingJob(context);

        Assertions.assertEquals(ClusterSnapshotJobState.UPLOADING, job.getState());
    }
    
    @Test
    public void testRunSnapshottingJobWithFEImageMismatch() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job.setJournalIds(100L, 200L);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        new MockUp<CheckpointController>() {
            @Mock
            public ClusterSnapshotInfo getClusterSnapshotInfo() {
                return new ClusterSnapshotInfo(new HashMap<>());
            }

            @Mock
            public long getImageJournalId() {
                return 150L; // Greater than checkpoint journal id (100L)
            }
        };

        Assertions.assertThrows(StarRocksException.class, () -> {
            job.runSnapshottingJob(context);
        });
    }

    @Test
    public void testRunUploadingJobWithUnfinishedTasks() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        ClusterSnapshotTask task = new ClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, 10L, 100L);
        batchTask.addTask(task);

        new MockUp<AgentBatchTask>() {
            @Mock
            public boolean isFinished() {
                return false;
            }

            @Mock
            public List<AgentTask> getUnfinishedTasks(int limit) {
                return Lists.newArrayList(task);
            }
        };

        new MockUp<AgentTask>() {
            @Mock
            public boolean isFailed() {
                return false;
            }

            @Mock
            public int getFailedTimes() {
                return 0;
            }
        };

        // Should return early without changing state
        job.runUploadingJob(context);
        Assertions.assertEquals(ClusterSnapshotJobState.UPLOADING, job.getState());
    }
    
    @Test
    public void testRunUploadingJobWithFailedTasks() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        ClusterSnapshotTask task = new ClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, 10L, 100L);
        task.setErrorMsg("Test error");
        batchTask.addTask(task);

        new MockUp<AgentBatchTask>() {
            @Mock
            public boolean isFinished() {
                return false;
            }

            @Mock
            public List<AgentTask> getUnfinishedTasks(int limit) {
                return Lists.newArrayList(task);
            }
        };

        new MockUp<AgentTask>() {
            @Mock
            public boolean isFailed() {
                return true;
            }

            @Mock
            public int getFailedTimes() {
                return 3;
            }
        };

        Assertions.assertThrows(StarRocksException.class, () -> {
            job.runUploadingJob(context);
        });
    }

    @Test
    public void testRunUploadingJobSuccess() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();

        new MockUp<AgentBatchTask>() {
            @Mock
            public boolean isFinished() {
                return true;
            }

            @Mock
            public List<AgentTask> getAllTasks() {
                return Lists.newArrayList();
            }
        };

        job.runUploadingJob(context);

        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        Assertions.assertTrue(job.getFinishedTimeMs() > 0);
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithNullPrevInfo() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        method.setAccessible(true);

        Object diff = method.invoke(job, null, newInfo);

        // Access inner class fields using reflection
        List<?> addedPartitions = (List<?>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        List<?> changedPartitions = (List<?>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        List<?> deletedPartitions = (List<?>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);

        Assertions.assertTrue(addedPartitions.size() > 0);
        Assertions.assertEquals(0, changedPartitions.size());
        Assertions.assertEquals(0, deletedPartitions.size());
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithChangedVersions() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 20L);

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        method.setAccessible(true);

        Object diff = method.invoke(job, prevInfo, newInfo);

        // Access inner class fields using reflection
        List<?> addedPartitions = (List<?>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        List<?> changedPartitions = (List<?>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        List<?> deletedPartitions = (List<?>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);

        Assertions.assertEquals(0, addedPartitions.size());
        Assertions.assertTrue(changedPartitions.size() > 0, "Should have changed partitions with different versions");
        Assertions.assertEquals(0, deletedPartitions.size());
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithAddedPartitions() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 2L, 2L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        method.setAccessible(true);

        Object diff = method.invoke(job, prevInfo, newInfo);

        // Access inner class fields using reflection
        List<?> addedPartitions = (List<?>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        List<?> changedPartitions = (List<?>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        List<?> deletedPartitions = (List<?>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);

        Assertions.assertTrue(addedPartitions.size() > 0, "Should have added partitions");
        Assertions.assertEquals(0, changedPartitions.size());
        Assertions.assertEquals(1, deletedPartitions.size());
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithDeletedPartitions() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 2L, 2L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        method.setAccessible(true);

        Object diff = method.invoke(job, newInfo, prevInfo); // Swapped: prev becomes new, new becomes prev

        // Access inner class fields using reflection
        List<?> addedPartitions = (List<?>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        List<?> changedPartitions = (List<?>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        List<?> deletedPartitions = (List<?>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);

        Assertions.assertEquals(1, addedPartitions.size());
        Assertions.assertEquals(0, changedPartitions.size());
        Assertions.assertTrue(deletedPartitions.size() > 0, "Should have deleted partitions");
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithSameVersions() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        method.setAccessible(true);

        Object diff = method.invoke(job, prevInfo, newInfo);

        // Access inner class fields using reflection
        List<?> addedPartitions = (List<?>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        List<?> changedPartitions = (List<?>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        List<?> deletedPartitions = (List<?>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);

        Assertions.assertEquals(0, addedPartitions.size(), "Should have no added partitions");
        Assertions.assertEquals(0, changedPartitions.size(), "Should have no changed partitions");
        Assertions.assertEquals(0, deletedPartitions.size(), "Should have no deleted partitions");
    }
    
    @Test
    public void testCreateClusterSnapshotTasksWithAddedPartitions() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job.setJournalIds(100L, 200L);
        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().
                        getStorageVolumeByName(storageVolumeName);
        sv.setVTabletId(1000L);

        // Create a snapshot diff with added partitions
        ClusterSnapshotInfo prevInfo = new ClusterSnapshotInfo(new HashMap<>());
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Set up the snapshot diff
        java.lang.reflect.Method calculateDiffMethod = FullClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        calculateDiffMethod.setAccessible(true);
        Object snapshotDiff = calculateDiffMethod.invoke(job, prevInfo, newInfo);

        // Set the snapshot diff field
        java.lang.reflect.Field snapshotDiffField = FullClusterSnapshotJob.class.getDeclaredField("snapshotDiff");
        snapshotDiffField.setAccessible(true);
        snapshotDiffField.set(job, snapshotDiff);

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, Long tabletId) {
                ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
                return node;
            }
        };

        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(String warehouseName) {
                return new ComputeNode(1L, "127.0.0.1", 9050);
            }
        };

        // Call createClusterSnapshotTasks
        java.lang.reflect.Method createTasksMethod = FullClusterSnapshotJob.class.getDeclaredMethod(
                "createClusterSnapshotTasks");
        createTasksMethod.setAccessible(true);
        createTasksMethod.invoke(job);

        // Verify tasks were created
        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        Assertions.assertNotNull(batchTask);
    }
    /* 
    @Test
    public void testGetVirtualTabletId() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod("getVirtualTabletId");
        method.setAccessible(true);

        long vTabletId1 = (Long) method.invoke(job);
        Assertions.assertTrue(vTabletId1 > 0);

        // Call again should return the same id
        long vTabletId2 = (Long) method.invoke(job);
        Assertions.assertEquals(vTabletId1, vTabletId2);
    }

    @Test
    public void testGetVirtualTabletIdWithNonexistentStorageVolume() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", "nonexistent_sv",
                System.currentTimeMillis());

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod("getVirtualTabletId");
        method.setAccessible(true);

        try {
            method.invoke(job);
            Assertions.fail("Should throw StarRocksException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assertions.assertTrue(e.getCause() instanceof StarRocksException);
            Assertions.assertTrue(e.getCause().getMessage().contains("storage volume not found"));
        }
    }

    @Test
    public void testStateTransitions() {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        Assertions.assertTrue(job.isInitializing());
        Assertions.assertTrue(job.isUnFinishedState());

        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        Assertions.assertTrue(job.isUnFinishedState());

        job.setState(ClusterSnapshotJobState.UPLOADING);
        Assertions.assertTrue(job.isUploading());
        Assertions.assertTrue(job.isUnFinishedState());

        job.setState(ClusterSnapshotJobState.FINISHED);
        Assertions.assertTrue(job.isFinished());
        Assertions.assertFalse(job.isUnFinishedState());

        job.setState(ClusterSnapshotJobState.ERROR);
        Assertions.assertTrue(job.isError());
        Assertions.assertTrue(job.isFinalState());
    }

    @Test
    public void testJobInfo() {
        long createdTimeMs = System.currentTimeMillis();
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName, createdTimeMs);

        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setDetailInfo("Test detail");
        job.setErrMsg("Test error");

        var info = job.getInfo();
        Assertions.assertEquals("test_snapshot", info.getSnapshot_name());
        Assertions.assertEquals(1L, info.getJob_id());
        Assertions.assertEquals("FINISHED", info.getState());
        Assertions.assertEquals("Test detail", info.getDetail_info());
        Assertions.assertEquals("Test error", info.getError_message());
    }

    @Test
    public void testCollectNodeToTablets() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        List<Long> tabletIds = Lists.newArrayList(1001L, 1002L, 1003L);
        Map<TBackend, List<Long>> nodeToTablets = Maps.newHashMap();
        long aggregatorNodeId = 0L;

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, Long tabletId) {
                ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
                Deencapsulation.setField(node, "brpcPort", 8060);
                Deencapsulation.setField(node, "httpPort", 8040);
                return node;
            }
        };

        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(String warehouseName) {
                return new ComputeNode(2L, "127.0.0.1", 9050);
            }
        };

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "collectNodeToTablets", List.class, Map.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(job, tabletIds, nodeToTablets, aggregatorNodeId);

        Assertions.assertTrue(result);
        Assertions.assertTrue(nodeToTablets.size() > 0);
    }

    @Test
    public void testCollectNodeToTabletsWithNoAggregator() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        List<Long> tabletIds = Lists.newArrayList(1001L, 1002L);
        Map<TBackend, List<Long>> nodeToTablets = Maps.newHashMap();
        long aggregatorNodeId = 0L;

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, Long tabletId) {
                ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
                Deencapsulation.setField(node, "brpcPort", 8060);
                Deencapsulation.setField(node, "httpPort", 8040);
                return node;
            }
        };

        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(String warehouseName) {
                return null; // No aggregator available
            }
        };

        // Use reflection to call private method
        java.lang.reflect.Method method = FullClusterSnapshotJob.class.getDeclaredMethod(
                "collectNodeToTablets", List.class, Map.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(job, tabletIds, nodeToTablets, aggregatorNodeId);

        Assertions.assertFalse(result, "Should return false when no aggregator is available");
    }

    @Test
    public void testJobStateTransitionsWithRun() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        // Mock all required methods for successful run
        new MockUp<SnapshotJobContext>() {
            @Mock
            public Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
                return Pair.create(100L, 200L);
            }
        };

        new MockUp<CheckpointController>() {
            @Mock
            public ClusterSnapshotInfo getClusterSnapshotInfo() {
                return new ClusterSnapshotInfo(new HashMap<>());
            }

            @Mock
            public long getImageJournalId() {
                return 100L;
            }

            @Mock
            public Pair<Boolean, String> runCheckpointControllerWithIds(long imageJournalId, long maxJournalId,
                    boolean needClusterSnapshotInfo) {
                return Pair.create(true, "");
            }
        };

        new MockUp<AgentBatchTask>() {
            @Mock
            public boolean isFinished() {
                return true;
            }

            @Mock
            public List<AgentTask> getAllTasks() {
                return Lists.newArrayList();
            }
        };

        // Run the job
        job.run(context);

        // Should transition through states and end up in FINISHED
        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
    }

    @Test
    public void testJobWithException() throws Exception {
        FullClusterSnapshotJob job = new FullClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        new MockUp<SnapshotJobContext>() {
            @Mock
            public Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() throws StarRocksException {
                throw new StarRocksException("Test exception");
            }
        };

        // Run the job - should catch exception and set error state
        job.run(context);

        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
        Assertions.assertTrue(job.isError());
        Assertions.assertNotNull(job.getInfo().getError_message());
    }
    */

    // Helper method to create mock ClusterSnapshotInfo for testing
    private ClusterSnapshotInfo createMockClusterSnapshotInfo(long dbId, long tableId, long partId,
            long physicalPartId, long version) {
        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        // This is a simplified mock - in real tests you would need to create proper
        // nested structures
        ClusterSnapshotInfo info = new ClusterSnapshotInfo(dbInfos);
        return info;
    }

    // Helper method to create complete ClusterSnapshotInfo with nested structures
    private ClusterSnapshotInfo createCompleteClusterSnapshotInfo(long dbId, long tableId, long partId,
            long physicalPartId, long version) {
        // Create MaterializedIndexSnapshotInfo
        List<Long> tabletIds = Lists.newArrayList(1001L, 1002L, 1003L);
        MaterializedIndexSnapshotInfo indexInfo = new MaterializedIndexSnapshotInfo(100L, 200L, tabletIds);

        Map<Long, MaterializedIndexSnapshotInfo> indexInfos = new HashMap<>();
        indexInfos.put(100L, indexInfo);

        // Create PhysicalPartitionSnapshotInfo
        PhysicalPartitionSnapshotInfo physicalPartInfo = new PhysicalPartitionSnapshotInfo(
                physicalPartId, version, version, indexInfos);

        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        physicalPartInfos.put(physicalPartId, physicalPartInfo);

        // Create PartitionSnapshotInfo
        PartitionSnapshotInfo partInfo = new PartitionSnapshotInfo(partId, physicalPartInfos);

        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        partInfos.put(partId, partInfo);

        // Create TableSnapshotInfo
        TableSnapshotInfo tableInfo = new TableSnapshotInfo(tableId, partInfos);

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, tableInfo);

        // Create DatabaseSnapshotInfo
        DatabaseSnapshotInfo dbInfo = new DatabaseSnapshotInfo(dbId, tableInfos);

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, dbInfo);

        // Create ClusterSnapshotInfo
        return new ClusterSnapshotInfo(dbInfos);
    }
}

