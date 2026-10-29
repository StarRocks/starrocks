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


package com.starrocks.staros;

import com.staros.exception.StarException;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class StarMgrServerTest {
    @Mocked
    private BDBEnvironment environment;

    @TempDir
    public File tempFolder;

    @BeforeEach
    public void setUp() {
        Config.aws_s3_path = "abc";
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
    }

    @AfterEach
    public void tearDown() {
        Config.aws_s3_path = "";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
    }

    @Test
    public void testStarMgrServer() throws Exception {
        StarMgrServer server = new StarMgrServer();

        new MockUp<StarManagerServer>() {
            @Mock
            public void start(int port) throws IOException {
            }
        };

        server.initialize(environment, tempFolder.getPath());

        server.getJournalSystem().setReplayId(1L);
        Assertions.assertEquals(1L, server.getReplayId());

        new MockUp<StarOSBDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 0;
            }
        };
        server.replayAndGenerateImage(tempFolder.getPath(), 0L);

        new MockUp<StarOSBDBJEJournalSystem>() {
            @Mock
            public void replayTo(long journalId) throws StarException {
            }
            @Mock
            public long getReplayId() {
                return 1;
            }
        };
        server.replayAndGenerateImage(tempFolder.getPath(), 1L);
    }

    @Test
    public void testStarMgrShardConfigWiring() throws Exception {
        boolean oldJournalConf = Config.lake_enable_incremental_shard_replica_journal;
        boolean oldReverseIndexConf = Config.lake_enable_worker_shard_reverse_index;
        boolean oldJournal = com.staros.util.Config.ENABLE_INCREMENTAL_SHARD_REPLICA_JOURNAL;
        boolean oldReverseIndex = com.staros.util.Config.ENABLE_SHARDMANAGER_WORKER_SHARD_REVERSE_INDEX;
        try {
            // Mock the overload initializeImpl() actually calls: testStarMgrServer lets the real one
            // bind cloud_native_meta_port and never shuts it down, so a second real start in the same
            // JVM would fail on the address already being in use.
            new MockUp<StarManagerServer>() {
                @Mock
                public void start(String host, int port, Executor executor) throws IOException {
                }
            };

            Config.lake_enable_incremental_shard_replica_journal = true;
            Config.lake_enable_worker_shard_reverse_index = true;

            StarMgrServer server = new StarMgrServer();
            server.initialize(environment, tempFolder.getPath());

            Assertions.assertTrue(com.staros.util.Config.ENABLE_INCREMENTAL_SHARD_REPLICA_JOURNAL);
            Assertions.assertTrue(com.staros.util.Config.ENABLE_SHARDMANAGER_WORKER_SHARD_REVERSE_INDEX);

            // The journal flag is read per call, so the refresh daemon carries a later change through.
            // The reverse index is seeded while the image loads and cannot be built afterwards, so the
            // same change must not reach StarMgr behind an index that was already decided.
            Config.lake_enable_incremental_shard_replica_journal = false;
            Config.lake_enable_worker_shard_reverse_index = false;
            Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon(), "runAfterCatalogReady");

            Assertions.assertFalse(com.staros.util.Config.ENABLE_INCREMENTAL_SHARD_REPLICA_JOURNAL);
            Assertions.assertTrue(com.staros.util.Config.ENABLE_SHARDMANAGER_WORKER_SHARD_REVERSE_INDEX);
        } finally {
            Config.lake_enable_incremental_shard_replica_journal = oldJournalConf;
            Config.lake_enable_worker_shard_reverse_index = oldReverseIndexConf;
            com.staros.util.Config.ENABLE_INCREMENTAL_SHARD_REPLICA_JOURNAL = oldJournal;
            com.staros.util.Config.ENABLE_SHARDMANAGER_WORKER_SHARD_REVERSE_INDEX = oldReverseIndex;
        }
    }

    @Test
    public void testGrpcExecutorScalesUpToConfiguredMax() throws Exception {
        int oldMaxThreads = Config.starmgr_grpc_server_max_worker_threads;
        try {
            new MockUp<StarManagerServer>() {
                @Mock
                public void start(String host, int port, Executor executor) throws IOException {
                }
            };

            Config.starmgr_grpc_server_max_worker_threads = 8;
            StarMgrServer server = new StarMgrServer();
            server.initialize(environment, tempFolder.getPath());

            // a scaling pool capped by the config, not a fixed pool of that size
            ThreadPoolExecutor executor = server.getGrpcExecutor();
            Assertions.assertTrue(executor.getQueue() instanceof LinkedTransferQueue);
            Assertions.assertEquals(8, executor.getMaximumPoolSize());
            Assertions.assertEquals(Math.min(ThreadPoolManager.cpuCores(), 8), executor.getCorePoolSize());
            Assertions.assertFalse(executor.allowsCoreThreadTimeOut());

            // a runtime change moves the maximum, and the core size follows it down
            Config.starmgr_grpc_server_max_worker_threads = 1;
            Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon(), "runAfterCatalogReady");
            Assertions.assertEquals(1, executor.getMaximumPoolSize());
            Assertions.assertEquals(1, executor.getCorePoolSize());

            Config.starmgr_grpc_server_max_worker_threads = 16;
            Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon(), "runAfterCatalogReady");
            Assertions.assertEquals(16, executor.getMaximumPoolSize());
            Assertions.assertEquals(Math.min(ThreadPoolManager.cpuCores(), 16), executor.getCorePoolSize());

            // an invalid value leaves the pool as it is
            Config.starmgr_grpc_server_max_worker_threads = 0;
            Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon(), "runAfterCatalogReady");
            Assertions.assertEquals(16, executor.getMaximumPoolSize());
        } finally {
            Config.starmgr_grpc_server_max_worker_threads = oldMaxThreads;
        }
    }

    @Test
    public void testGrpcMaxWorkerThreadsMustBePositive() throws Exception {
        int oldMaxThreads = Config.starmgr_grpc_server_max_worker_threads;
        // setConfigField is the path both fe.conf loading and ADMIN SET FRONTEND CONFIG go through: a value below 1
        // must be refused there, before it can be persisted and stop FE from starting
        Field field = Config.class.getField("starmgr_grpc_server_max_worker_threads");
        try {
            for (String bad : new String[] {"0", "-1"}) {
                InvalidConfException e = Assertions.assertThrows(InvalidConfException.class,
                        () -> ConfigBase.setConfigField(field, bad));
                Assertions.assertTrue(e.getMessage().contains("must be at least 1"), e.getMessage());
                Assertions.assertEquals(oldMaxThreads, Config.starmgr_grpc_server_max_worker_threads);
            }
            ConfigBase.setConfigField(field, "8");
            Assertions.assertEquals(8, Config.starmgr_grpc_server_max_worker_threads);
        } finally {
            Config.starmgr_grpc_server_max_worker_threads = oldMaxThreads;
        }
    }

    @Test
    public void testStopCheckpointControllerNullSafe() {
        // stopCheckpointController must be a no-op when startCheckpointController has not run.
        StarMgrServer server = new StarMgrServer();
        Assertions.assertNull(server.checkpointController);
        Assertions.assertDoesNotThrow(() -> server.stopCheckpointController());
    }

    @Test
    public void testStopCheckpointControllerStopsExistingController() {
        // stopCheckpointController must forward the fire-and-forget stopBestEffort() to the owned
        // controller (demotion no longer joins). With a freshly constructed (never started) controller
        // this just requests stop without side effects, so it exercises the not-null branch.
        StarMgrServer server = new StarMgrServer();
        server.checkpointController = new CheckpointController(
                "star_os_checkpoint_controller_test", new com.starrocks.utframe.MockJournal(), "");

        Assertions.assertDoesNotThrow(() -> server.stopCheckpointController());
    }
}
