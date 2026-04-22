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
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.warehouse.WarehouseManagerEPack;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.lake.snapshot.ClusterSnapshotJobScheduler;
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
import com.starrocks.task.ExternalClusterSnapshotTask;
import com.starrocks.thrift.TClusterSnapshotJobsItem;
import com.starrocks.thrift.TComputeNodeTablets;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.cngroup.ComputeResource;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class ExternalClusterSnapshotJobTest {
    @Mocked
    private EditLog editLog;

    private StarOSAgent starOSAgent = new StarOSAgent();
    private String storageVolumeName = StorageVolumeMgr.BUILTIN_STORAGE_VOLUME;
    private ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
    private boolean initSv = false;
    private AtomicLong nextId = new AtomicLong(0);


    @BeforeAll
    public static void beforeAll() {
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

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
            public long getPrimaryComputeNodeIdByShard(long shardId, long workerGroupId) {
                return 1L;
            }

            @Mock
            public String getServiceId() {
                return "test-service-id";
            }

            @Mock
            public void prepare() {
                // no-op for tests
            }

            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                return Lists.newArrayList();
            }
        };

        new MockUp<ExternalClusterSnapshotJob>() {
            @Mock
            public ComputeResource getComputeResource() {
                return WarehouseManager.DEFAULT_RESOURCE;
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
            Assertions.assertEquals(true,
                    GlobalStateMgr.getCurrentState().getStorageVolumeMgr().exists(storageVolumeName));
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

        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(id, snapshotName, storageVolumeName,
                createdTimeMs);

        Assertions.assertEquals(id, job.getId());
        Assertions.assertEquals(snapshotName, job.getSnapshotName());
        Assertions.assertEquals(storageVolumeName, job.getStorageVolumeName());
        Assertions.assertEquals(createdTimeMs, job.getCreatedTimeMs());
        Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, job.getState());
        Assertions.assertTrue(job.isInitializing());
    }

    @Test
    public void testRunInitializingJob() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
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

        mockWarehouseAliveNodes();
        mockAggregatorSuccess();
        mockGetVirtualTabletId();

        job.runSnapshottingJob(context);

        Assertions.assertEquals(ClusterSnapshotJobState.UPLOADING, job.getState());
    }

    @Test
    public void testRunSnapshottingJobWithFEImageMismatch() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, 10L, false,
                false, 100L,
                1L);
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, 10L, false,
                false,
                100L, 1L);
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
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

        Assertions.assertEquals(ClusterSnapshotJobState.CLEANING, job.getState());
    }

    @Test
    public void testRunUploadingJobTimeout() throws Exception {
        long originalTimeout = Config.automated_cluster_snapshot_timeout_seconds;
        try {
            Config.automated_cluster_snapshot_timeout_seconds = 1;
            ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                    System.currentTimeMillis() - 5000);
            job.setState(ClusterSnapshotJobState.UPLOADING);

            new MockUp<AgentBatchTask>() {
                @Mock
                public boolean isFinished() {
                    return false;
                }

                @Mock
                public List<AgentTask> getUnfinishedTasks(int limit) {
                    return Lists.newArrayList();
                }
            };

            job.run(createSnapshotJobContext(
                    new CheckpointController("fe", null, ""),
                    new CheckpointController("starMgr", null, "")));

            Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
        } finally {
            Config.automated_cluster_snapshot_timeout_seconds = originalTimeout;
        }
    }

    @Test
    public void testRunUploadingJobUploadFailure() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

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

        new MockUp<ClusterSnapshotUtils>() {
            @Mock
            public void uploadClusterSnapshotToRemote(ClusterSnapshotJob job) throws StarRocksException {
                throw new StarRocksException("upload fail");
            }
        };

        Assertions.assertThrows(StarRocksException.class, () -> job.runUploadingJob(
                createSnapshotJobContext(new CheckpointController("fe", null, ""),
                        new CheckpointController("starMgr", null, ""))));
    }

    @Test
    public void testCalculateClusterSnapshotDiffWithNullPrevInfo() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 20L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 2L, 2L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 2L, 2L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());

        ClusterSnapshotInfo prevInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
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
    public void testCreateExternalClusterSnapshotTasksWithAddedPartitions() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job.setJournalIds(100L, 200L);

        // Create a snapshot diff with added partitions
        ClusterSnapshotInfo prevInfo = new ClusterSnapshotInfo(new HashMap<>());
        ClusterSnapshotInfo newInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Set up the snapshot diff
        java.lang.reflect.Method calculateDiffMethod = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "calculateClusterSnapshotDiff", ClusterSnapshotInfo.class, ClusterSnapshotInfo.class);
        calculateDiffMethod.setAccessible(true);
        Object snapshotDiff = calculateDiffMethod.invoke(job, prevInfo, newInfo);

        // Set the snapshot diff field
        java.lang.reflect.Field snapshotDiffField = ExternalClusterSnapshotJob.class.getDeclaredField("snapshotDiff");
        snapshotDiffField.setAccessible(true);
        snapshotDiffField.set(job, snapshotDiff);

        mockWarehouseAssign();
        mockAggregatorSuccess();
        mockGetVirtualTabletId();

        // Call createExternalClusterSnapshotTasks
        java.lang.reflect.Method createTasksMethod = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "createUploadClusterSnapshotTasks");
        createTasksMethod.setAccessible(true);
        createTasksMethod.invoke(job);

        // Verify tasks were created
        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        Assertions.assertNotNull(batchTask);
    }

    @Test
    public void testStateTransitions() {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());

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
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, createdTimeMs);

        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setDetailInfo("Test detail");
        job.setErrMsg("Test error");

        TClusterSnapshotJobsItem info = job.getInfo();
        Assertions.assertEquals("test_snapshot", info.getSnapshot_name());
        Assertions.assertEquals(1L, info.getJob_id());
        Assertions.assertEquals("FINISHED", info.getState());
        Assertions.assertEquals("Test detail", info.getDetail_info());
        Assertions.assertEquals("Test error", info.getError_message());
    }

    @Test
    public void testFinishSnapshotTaskStatusHandling() {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, 10L, false,
                false,
                100L, 1L);

        TFinishTaskRequest okReq = new TFinishTaskRequest();
        okReq.setTask_status(new TStatus(TStatusCode.OK));
        job.finishSnapshotTask(task, okReq);
        Assertions.assertTrue(task.isFinished());
        Assertions.assertFalse(task.isFailed());

        TFinishTaskRequest failReq = new TFinishTaskRequest();
        failReq.setTask_status(new TStatus(TStatusCode.TIMEOUT));
        failReq.getTask_status().addToError_msgs("err");
        job.finishSnapshotTask(task, failReq);
        Assertions.assertTrue(task.isFailed());
    }

    @Test
    public void testReplayUploadingJobNullDiff() {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);
        job.replayUploadingJob();
        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
    }

    @Test
    public void testReplaySwitchUploadingAndInit() {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());

        // INITIALIZING branch should be no-op
        job.setState(ClusterSnapshotJobState.INITIALIZING);
        job.replay();
        Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, job.getState());

        // UPLOADING branch delegates to replayUploadingJob
        job.setState(ClusterSnapshotJobState.UPLOADING);
        job.replay();
        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
    }

    @Test
    public void testReplayUploadingJobCreateTaskFailure() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING);

        Object diff = createSnapshotDiffWithAddedPartition(true);
        setSnapshotDiff(job, diff);

        mockAggregatorFailure();
        mockGetVirtualTabletId();

        job.replayUploadingJob();
        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
    }

    @Test
    public void testCreateExternalClusterSnapshotTaskshotTasksWithChangedPartitions() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot", storageVolumeName,
                System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);

        Object diff = createSnapshotDiffWithChangedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockWarehouseAssign();
        mockGetVirtualTabletId();

        java.lang.reflect.Method createTasksMethod = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "createUploadClusterSnapshotTasks");
        createTasksMethod.setAccessible(true);
        createTasksMethod.invoke(job);

        Assertions.assertTrue(job.getLakeSnapshotBatchTask().getAllTasks().size() > 0);
    }

    @Test
    public void testPartitionKeyEqualityAndHash() throws Exception {
        Object key1 = newPartitionKey(1, 2, 3, 4);
        Object key2 = newPartitionKey(1, 2, 3, 4);
        Object key3 = newPartitionKey(1, 2, 3, 5);

        Assertions.assertEquals(key1, key2);
        Assertions.assertEquals(key1.hashCode(), key2.hashCode());
        Assertions.assertNotEquals(key1, key3);
        Assertions.assertTrue(key1.toString().contains("PartitionKey"));
    }

    @Test
    public void testCollectComputeNodeTablets() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());

        List<Long> tabletIds = Lists.newArrayList(1001L, 1002L, 1003L);
        List<TComputeNodeTablets> computeNodes = Lists.newArrayList();
        long aggregatorNodeId = 0L;

        mockWarehouseAssign();

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "collectComputeNodeTablets", List.class);
        method.setAccessible(true);
        computeNodes = (List<TComputeNodeTablets>) method.invoke(job, tabletIds);
        Assertions.assertTrue(computeNodes.size() > 0);
    }

    @Test
    public void testJobStateTransitionsWithRun() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName,
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

        mockGetVirtualTabletId();
        // Run the job
        job.run(context);
        // Should transition through states and end up in FINISHED
        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
    }

    @Test
    public void testJobWithException() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName,
                System.currentTimeMillis());

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        new MockUp<SnapshotJobContext>() {

            @Mock
            public Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr()
                    throws StarRocksException {
                throw new StarRocksException("Test exception");
            }
        };

        // Run the job - should catch exception and set error state
        job.run(context);

        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
        Assertions.assertTrue(job.isError());
        Assertions.assertNotNull(job.getInfo().getError_message());
    }

    @Test
    public void testReplayFinishedStateRestoresLastSuccFullSnapshotInfo() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);

        // Create a ClusterSnapshotInfo with some data
        ClusterSnapshotInfo clusterSnapshotInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Set the clusterSnapshotInfo to the job's snapshot
        ClusterSnapshot snapshot = job.getSnapshot();
        snapshot.setClusterSnapshotInfo(clusterSnapshotInfo);

        // Verify lastSuccFullSnapshotInfo is initially null
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());

        // Replay the FINISHED job
        job.replay();

        // Verify lastSuccFullSnapshotInfo is restored
        ClusterSnapshotInfo restoredInfo = clusterSnapshotMgr.getLastSuccFullSnapshotInfo();
        Assertions.assertNotNull(restoredInfo);
        Assertions.assertEquals(clusterSnapshotInfo, restoredInfo);
    }

    @Test
    public void testReplayFinishedStateWithNullSnapshotInfo() {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);

        // snapshot exists but clusterSnapshotInfo is null
        ClusterSnapshot snapshot = job.getSnapshot();
        snapshot.setClusterSnapshotInfo(null);

        // Should not throw exception
        job.replay();

        // lastSuccFullSnapshotInfo should remain unchanged (null)
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());
    }

    @Test
    public void testReplayFinishedStateWithEmptySnapshotInfo() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);

        // Create an empty ClusterSnapshotInfo
        ClusterSnapshotInfo emptyInfo = new ClusterSnapshotInfo(new HashMap<>());
        ClusterSnapshot snapshot = job.getSnapshot();
        snapshot.setClusterSnapshotInfo(emptyInfo);

        // Replay the FINISHED job
        job.replay();

        // Verify empty info is restored
        ClusterSnapshotInfo restoredInfo = clusterSnapshotMgr.getLastSuccFullSnapshotInfo();
        Assertions.assertNotNull(restoredInfo);
        Assertions.assertTrue(restoredInfo.isEmpty());
    }

    @Test
    public void testReplayFinishedStateOverwritesPreviousInfo() throws Exception {
        ExternalClusterSnapshotJob job1 = new ExternalClusterSnapshotJob(1L, "test_snapshot_1",
                storageVolumeName, System.currentTimeMillis());
        job1.setState(ClusterSnapshotJobState.FINISHED);

        ClusterSnapshotInfo info1 = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);
        job1.getSnapshot().setClusterSnapshotInfo(info1);
        job1.replay();

        // Verify first info is set
        ClusterSnapshotInfo restored1 = clusterSnapshotMgr.getLastSuccFullSnapshotInfo();
        Assertions.assertNotNull(restored1);
        Assertions.assertEquals(10L, restored1.getVersion(1L, 1L, 1L, 1L));

        // Create a second finished job with different info
        ExternalClusterSnapshotJob job2 = new ExternalClusterSnapshotJob(2L, "test_snapshot_2",
                storageVolumeName, System.currentTimeMillis());
        job2.setState(ClusterSnapshotJobState.FINISHED);

        ClusterSnapshotInfo info2 = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 20L);
        job2.getSnapshot().setClusterSnapshotInfo(info2);
        job2.replay();

        // Verify second info overwrites the first
        ClusterSnapshotInfo restored2 = clusterSnapshotMgr.getLastSuccFullSnapshotInfo();
        Assertions.assertNotNull(restored2);
        Assertions.assertEquals(20L, restored2.getVersion(1L, 1L, 1L, 1L));
    }

    @Test
    public void testReplayOtherStatesDoNotRestoreSnapshotInfo() throws Exception {
        ClusterSnapshotInfo clusterSnapshotInfo = createCompleteClusterSnapshotInfo(1L, 1L, 1L, 1L, 10L);

        // Test SNAPSHOTING state - should not restore
        ExternalClusterSnapshotJob job1 = new ExternalClusterSnapshotJob(1L, "test_snapshot_1",
                storageVolumeName, System.currentTimeMillis());
        job1.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job1.getSnapshot().setClusterSnapshotInfo(clusterSnapshotInfo);
        clusterSnapshotMgr.setLastSuccFullSnapshotInfo(null);
        job1.replay();
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());

        // Test EXPIRED state - should not restore
        ExternalClusterSnapshotJob job2 = new ExternalClusterSnapshotJob(2L, "test_snapshot_2",
                storageVolumeName, System.currentTimeMillis());
        job2.setState(ClusterSnapshotJobState.EXPIRED);
        job2.getSnapshot().setClusterSnapshotInfo(clusterSnapshotInfo);
        clusterSnapshotMgr.setLastSuccFullSnapshotInfo(null);
        job2.replay();
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());

        // Test DELETED state - should not restore
        ExternalClusterSnapshotJob job3 = new ExternalClusterSnapshotJob(3L, "test_snapshot_3",
                storageVolumeName, System.currentTimeMillis());
        job3.setState(ClusterSnapshotJobState.DELETED);
        job3.getSnapshot().setClusterSnapshotInfo(clusterSnapshotInfo);
        clusterSnapshotMgr.setLastSuccFullSnapshotInfo(null);
        job3.replay();
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());

        // Test ERROR state - should not restore
        ExternalClusterSnapshotJob job4 = new ExternalClusterSnapshotJob(4L, "test_snapshot_4",
                storageVolumeName, System.currentTimeMillis());
        job4.setState(ClusterSnapshotJobState.ERROR);
        job4.getSnapshot().setClusterSnapshotInfo(clusterSnapshotInfo);
        clusterSnapshotMgr.setLastSuccFullSnapshotInfo(null);
        job4.replay();
        Assertions.assertNull(clusterSnapshotMgr.getLastSuccFullSnapshotInfo());
    }

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
                physicalPartId, version, version, 0, indexInfos);

        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        physicalPartInfos.put(physicalPartId, physicalPartInfo);

        // Create PartitionSnapshotInfo
        PartitionSnapshotInfo partInfo = new PartitionSnapshotInfo(partId, physicalPartInfos);

        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        partInfos.put(partId, partInfo);

        // Create TableSnapshotInfo
        TableSnapshotInfo tableInfo = new TableSnapshotInfo(tableId, true, partInfos);

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, tableInfo);

        // Create DatabaseSnapshotInfo
        DatabaseSnapshotInfo dbInfo = new DatabaseSnapshotInfo(dbId, tableInfos);

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, dbInfo);

        // Create ClusterSnapshotInfo
        return new ClusterSnapshotInfo(dbInfos);
    }

    private void setSnapshotDiff(ExternalClusterSnapshotJob job, Object diff) throws Exception {
        java.lang.reflect.Field snapshotDiffField = ExternalClusterSnapshotJob.class.getDeclaredField("snapshotDiff");
        snapshotDiffField.setAccessible(true);
        snapshotDiffField.set(job, diff);
    }

    private Object newPartitionKey(long dbId, long tableId, long partId, long physicalPartId) throws Exception {
        Class<?> pkClass = Class.forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$PartitionKey");
        java.lang.reflect.Constructor<?> ctor = pkClass.getDeclaredConstructor(long.class, long.class, long.class,
                long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(dbId, tableId, partId, physicalPartId);
    }

    private Object newPartitionVersionInfo(long dbId, long tableId, long partId, long physicalPartId, long version,
            List<Long> tabletIds) throws Exception {
        Object key = newPartitionKey(dbId, tableId, partId, physicalPartId);
        Class<?> pviClass = Class
                .forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$PartitionVersionInfo");
        java.lang.reflect.Constructor<?> ctor = pviClass.getDeclaredConstructor(
                Class.forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$PartitionKey"), long.class,
                boolean.class, List.class);
        ctor.setAccessible(true);
        return ctor.newInstance(key, version, true, tabletIds);
    }

    private Object newPartitionVersionChangeInfo(long prevVersion, boolean isPreviousFileBundling, 
                                                 Object currentPartitionInfo) throws Exception {
        Class<?> pvcClass = Class
                .forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$PartitionVersionChangeInfo");
        java.lang.reflect.Constructor<?> ctor = pvcClass.getDeclaredConstructor(long.class, boolean.class, Class
                .forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$PartitionVersionInfo"));
        ctor.setAccessible(true);
        return ctor.newInstance(prevVersion, isPreviousFileBundling, currentPartitionInfo);
    }

    private Object createEmptySnapshotDiff() throws Exception {
        Class<?> diffClass = Class
                .forName("com.starrocks.lake.snapshot.ExternalClusterSnapshotJob$ClusterSnapshotDiff");
        java.lang.reflect.Constructor<?> ctor = diffClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        return ctor.newInstance();
    }

    private Object createSnapshotDiffWithDeletedPartition() throws Exception {
        Object diff = createEmptySnapshotDiff();
        @SuppressWarnings("unchecked")
        List<Object> deletedPartitions = (List<Object>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);
        Object partition = newPartitionVersionInfo(1, 2, 3, 4, 10L, Lists.newArrayList(1001L));
        deletedPartitions.add(partition);
        return diff;
    }

    private Object createSnapshotDiffWithDeletedPartitionWithoutTablets() throws Exception {
        Object diff = createEmptySnapshotDiff();
        @SuppressWarnings("unchecked")
        List<Object> deletedPartitions = (List<Object>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);
        Object partition = newPartitionVersionInfo(1, 2, 3, 4, 10L, Lists.newArrayList());
        deletedPartitions.add(partition);
        return diff;
    }

    private Object createSnapshotDiffWithAddedPartition(boolean aggregatorFail) throws Exception {
        Object diff = createEmptySnapshotDiff();
        @SuppressWarnings("unchecked")
        List<Object> addedPartitions = (List<Object>) diff.getClass().getMethod("getAddedPartitions").invoke(diff);
        Object partition = newPartitionVersionInfo(1, 2, 3, 4, 1L, Lists.newArrayList(10L));
        if (aggregatorFail) {
            java.lang.reflect.Method setAgg = partition.getClass().getMethod("setAggregatorNodeId", long.class);
            setAgg.invoke(partition, 0L);
        }
        addedPartitions.add(partition);
        return diff;
    }

    private Object createSnapshotDiffWithChangedPartition() throws Exception {
        Object diff = createEmptySnapshotDiff();
        @SuppressWarnings("unchecked")
        List<Object> changedPartitions = (List<Object>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);
        Object current = newPartitionVersionInfo(1, 2, 3, 4, 2L, Lists.newArrayList(1001L));
        Object changeInfo = newPartitionVersionChangeInfo(1L, true, current);
        changedPartitions.add(changeInfo);
        return diff;
    }

    private void mockAggregatorSuccess() {
        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(ComputeResource computeResource,
                                                    Collection<ComputeNode> candidateNodes) {
                return new ComputeNode(1L, "127.0.0.1", 9050);
            }
        };
    }

    private void mockAggregatorFailure() {
        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(ComputeResource computeResource,
                                                    Collection<ComputeNode> candidateNodes) {
                return null; // cause chooseAggregatorNodeId to return 0
            }
        };
    }

    private void mockWarehouseAssign() {
        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return new ComputeNode(1L, "127.0.0.1", 9050);
            }
        };
    }

    private void mockWarehouseAliveNodes() {
        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return Lists.newArrayList(new ComputeNode(1L, "127.0.0.1", 9050));
            }
        };
    }

    private void mockGetVirtualTabletId() {
        new MockUp<ExternalClusterSnapshotJob>() {
            @Mock
            public long getVirtualTabletId() throws StarRocksException {
                return 1001L;
            }
        };
    }

    @Test
    public void testGetVirtualTabletIdExistingVTabletId() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());

        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(storageVolumeName);
        Assertions.assertNotNull(sv);
        sv.setVTabletId(999L);

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod("getVirtualTabletId");
        method.setAccessible(true);

        long vTabletId = (Long) method.invoke(job);
        Assertions.assertEquals(999L, vTabletId);
    }

    @Test
    public void testGetVirtualTabletIdCreateNew() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());

        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(storageVolumeName);
        Assertions.assertNotNull(sv);
        sv.setVTabletId(-1L); // Reset to -1 to trigger creation

        // Mock StarOSAgent methods
        new MockUp<StarOSAgent>() {
            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId, String rootDir) throws Exception {
                return FilePathInfo.newBuilder().setFullPath("s3://test-bucket/path").build();
            }

            @Mock
            public long createShardGroupForVirtualTablet() throws DdlException {
                return 200L;
            }

            @Mock
            public void createShardWithVirtualTabletId(FilePathInfo pathInfo, FileCacheInfo cacheInfo,
                    long groupId, Map<String, String> properties, long vTabletId, ComputeResource computeResource) {
                // Mock implementation - do nothing
            }
        };

        // Mock StorageVolumeMgr.updateStorageVolumeVTabletMapping
        new MockUp<StorageVolumeMgr>() {
            @Mock
            public void updateStorageVolumeVTabletMapping(String name, long vTabletId, long vTabletGroupId) {
                // Mock implementation - do nothing
            }
        };

        // Use reflection to call private method
        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod("getVirtualTabletId");
        method.setAccessible(true);

        long vTabletId = (Long) method.invoke(job);
        Assertions.assertTrue(vTabletId > 0);
        Assertions.assertEquals(vTabletId, sv.getVTabletId());
        Assertions.assertEquals(200L, sv.getVTabletGroupId());
    }

    // ==================== Tests for fire-and-forget runCleaningJob ====================

    @Test
    public void testRunCleaningJobFireAndForget() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.CLEANING);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockWarehouseAliveNodes();
        mockGetVirtualTabletId();

        // Verify default cleaningCompleted is true
        Assertions.assertTrue(job.isCleaningCompleted());

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "runCleaningJob", SnapshotJobContext.class);
        method.setAccessible(true);
        method.invoke(job, context);

        // runCleaningJob should immediately transition to FINISHED
        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        // cleaningCompleted should be false (tasks dispatched, not yet confirmed)
        Assertions.assertFalse(job.isCleaningCompleted());
        // Tasks should have been created
        Assertions.assertNotNull(job.getLakeSnapshotBatchTask());
    }

    @Test
    public void testRunCleaningJobWithChangedPartitions() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.CLEANING);

        Object diff = createEmptySnapshotDiff();
        @SuppressWarnings("unchecked")
        List<Object> deletedPartitions = (List<Object>) diff.getClass().getMethod("getDeletedPartitions").invoke(diff);
        @SuppressWarnings("unchecked")
        List<Object> changedPartitions = (List<Object>) diff.getClass().getMethod("getChangedPartitions").invoke(diff);

        Object deletedPartition = newPartitionVersionInfo(1, 2, 3, 4, 10L, Lists.newArrayList(1001L));
        deletedPartitions.add(deletedPartition);

        Object current = newPartitionVersionInfo(5, 6, 7, 8, 2L, Lists.newArrayList(2001L));
        Object changeInfo = newPartitionVersionChangeInfo(1L, true, current);
        changedPartitions.add(changeInfo);

        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockWarehouseAliveNodes();
        mockWarehouseAssign();
        mockGetVirtualTabletId();

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "runCleaningJob", SnapshotJobContext.class);
        method.setAccessible(true);
        method.invoke(job, context);

        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        Assertions.assertFalse(job.isCleaningCompleted());
    }

    @Test
    public void testRunCleaningJobFailure() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.CLEANING);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        // Mock aggregator to fail (returns 0) -> createDeleteClusterSnasphotTasks throws
        mockAggregatorFailure();
        mockGetVirtualTabletId();

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        // run() catches the exception and sets ERROR state
        job.run(context);

        Assertions.assertEquals(ClusterSnapshotJobState.ERROR, job.getState());
    }

    @Test
    public void testRunCleaningJobEmptyDiff() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.CLEANING);

        Object diff = createEmptySnapshotDiff();
        setSnapshotDiff(job, diff);

        mockGetVirtualTabletId();

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        SnapshotJobContext context = createSnapshotJobContext(feController, starMgrController);

        java.lang.reflect.Method method = ExternalClusterSnapshotJob.class.getDeclaredMethod(
                "runCleaningJob", SnapshotJobContext.class);
        method.setAccessible(true);
        method.invoke(job, context);

        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        Assertions.assertFalse(job.isCleaningCompleted());
        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        Assertions.assertNotNull(batchTask);
        Assertions.assertEquals(0, batchTask.getAllTasks().size());
    }

    @Test
    public void testReplayCleaningState() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.CLEANING);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockWarehouseAliveNodes();
        mockGetVirtualTabletId();

        // Replay should call runCleaningJob which transitions to FINISHED
        job.replay();

        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        Assertions.assertFalse(job.isCleaningCompleted());
    }

    // ==================== Tests for scheduler retryPendingCleanup ====================

    @Test
    public void testRetryPendingCleanupSkipsNonExternalJob() throws Exception {
        // Add a non-external ClusterSnapshotJob
        ClusterSnapshotJob normalJob = new ClusterSnapshotJob(1L, "test", storageVolumeName,
                System.currentTimeMillis());
        normalJob.setState(ClusterSnapshotJobState.FINISHED);
        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, normalJob);

        // retryPendingCleanup should simply skip it without errors
        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // No exception thrown - pass
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupSkipsAlreadyCleanedJob() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(true); // already cleaned
        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // cleaningCompleted should remain true
        Assertions.assertTrue(job.isCleaningCompleted());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupSkipsNonFinishedJob() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.UPLOADING); // not FINISHED
        job.setCleaningCompleted(false);
        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // Should not have changed anything
        Assertions.assertFalse(job.isCleaningCompleted());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupSkipsStillRunningTasks() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockGetVirtualTabletId();

        // Set up a batch task with running (not finished) tasks
        AgentBatchTask batchTask = new AgentBatchTask();
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, -1L, true,
                true, 100L, 1L);
        batchTask.addTask(task);
        // task is not finished yet

        java.lang.reflect.Field batchField = ExternalClusterSnapshotJob.class.getDeclaredField("lakeSnapshotBatchTask");
        batchField.setAccessible(true);
        batchField.set(job, batchTask);

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // Should still be not completed since tasks are still running
        Assertions.assertFalse(job.isCleaningCompleted());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupMarksCompletedOnAllSuccess() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false);

        // Create a batch task with finished, successful tasks
        AgentBatchTask batchTask = new AgentBatchTask();
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, -1L, true,
                true, 100L, 1L);
        task.setFinished(true);
        batchTask.addTask(task);

        java.lang.reflect.Field batchField = ExternalClusterSnapshotJob.class.getDeclaredField("lakeSnapshotBatchTask");
        batchField.setAccessible(true);
        batchField.set(job, batchTask);

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // All tasks succeeded -> cleaningCompleted should be true
        Assertions.assertTrue(job.isCleaningCompleted());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupRetriesOnFailedTasks() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockGetVirtualTabletId();

        // Create a batch task with finished but failed task
        AgentBatchTask batchTask = new AgentBatchTask();
        ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(1L, 1L, 2L, 3L, 4L, 1L, -1L, -1L, true,
                true, 100L, 1L);
        task.setFinished(true);
        task.setFailed(true);
        task.setErrorMsg("delete failed on CN");
        batchTask.addTask(task);

        java.lang.reflect.Field batchField = ExternalClusterSnapshotJob.class.getDeclaredField("lakeSnapshotBatchTask");
        batchField.setAccessible(true);
        batchField.set(job, batchTask);

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // Should still be not completed, but new tasks should have been dispatched
        Assertions.assertFalse(job.isCleaningCompleted());
        // The batch task should have been replaced with new tasks
        AgentBatchTask newBatchTask = job.getLakeSnapshotBatchTask();
        Assertions.assertNotNull(newBatchTask);
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupHandlesNullSnapshotDiff() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false);
        // snapshotDiff is null by default

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // snapshotDiff is null -> should mark cleaningCompleted as true
        Assertions.assertTrue(job.isCleaningCompleted());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testRetryPendingCleanupEmptyBatchTask() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false);

        Object diff = createSnapshotDiffWithDeletedPartition();
        setSnapshotDiff(job, diff);

        mockAggregatorSuccess();
        mockGetVirtualTabletId();

        // Empty batch task (taskNum == 0) -> should re-create and dispatch tasks
        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        CheckpointController feController = new CheckpointController("fe", null, "");
        CheckpointController starMgrController = new CheckpointController("starMgr", null, "");
        ClusterSnapshotJobScheduler scheduler = new ClusterSnapshotJobScheduler(feController, starMgrController);

        java.lang.reflect.Method method = ClusterSnapshotJobScheduler.class.getDeclaredMethod("retryPendingCleanup");
        method.setAccessible(true);
        method.invoke(scheduler);

        // Tasks should have been created
        Assertions.assertFalse(job.isCleaningCompleted());
        AgentBatchTask batchTask = job.getLakeSnapshotBatchTask();
        Assertions.assertNotNull(batchTask);
        Assertions.assertTrue(batchTask.getTaskNum() > 0);
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    // ==================== Tests for ClusterSnapshotMgr job expiry protection ====================

    @Test
    public void testClearFinishedJobSkipsPendingCleanup() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(false); // still has pending cleanup

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        new MockUp<ClusterSnapshotUtils>() {
            @Mock
            public void clearClusterSnapshotFromRemote(ClusterSnapshotJob j) throws StarRocksException {
                // Mock implementation
            }
        };

        clusterSnapshotMgr.clearFinishedAutomatedClusterSnapshot(null);

        // Job should NOT be expired because cleaningCompleted is false
        Assertions.assertEquals(ClusterSnapshotJobState.FINISHED, job.getState());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }

    @Test
    public void testClearFinishedJobExpiresCompletedCleanup() throws Exception {
        ExternalClusterSnapshotJob job = new ExternalClusterSnapshotJob(1L, "test_snapshot",
                storageVolumeName, System.currentTimeMillis());
        job.setState(ClusterSnapshotJobState.FINISHED);
        job.setCleaningCompleted(true); // cleanup done

        clusterSnapshotMgr.getAutomatedSnapshotJobs().put(1L, job);

        new MockUp<ClusterSnapshotUtils>() {
            @Mock
            public void clearClusterSnapshotFromRemote(ClusterSnapshotJob j) throws StarRocksException {
                // Mock implementation
            }
        };

        clusterSnapshotMgr.clearFinishedAutomatedClusterSnapshot(null);

        // Job should be expired since cleaningCompleted is true
        Assertions.assertEquals(ClusterSnapshotJobState.DELETED, job.getState());
        clusterSnapshotMgr.getAutomatedSnapshotJobs().clear();
    }
}
