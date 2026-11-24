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

package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.thrift.TStartCheckpointRequest;
import com.starrocks.thrift.TStatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointImageV3WorkerTest {
    @TempDir
    Path temporary;
    private String oldMeta;
    private boolean oldDisable;
    private MockedStatic<GlobalStateMgr> globals;
    private CheckpointController controller;
    private RecordingWorker worker;

    private class RecordingWorker extends CheckpointWorker {
        int runs;
        boolean savedV2;

        RecordingWorker(Journal journal) {
            super("recording-worker", journal);
        }

        @Override
        void doCheckpoint(long epoch, long journalId, boolean needClusterSnapshotInfo) {
            runs++;
            savedV2 = writeV2;
        }

        @Override
        CheckpointController getCheckpointController() {
            return controller;
        }

        @Override
        boolean isBelongToGlobalStateMgr() {
            return true;
        }
    }

    @BeforeEach
    public void setUp() {
        oldMeta = Config.meta_dir;
        oldDisable = Config.disable_image_v2_write;
        Config.meta_dir = temporary.toString();
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        when(state.getEpoch()).thenReturn(1L);
        when(state.isLeader()).thenReturn(true);
        NodeMgr node = mock(NodeMgr.class);
        when(node.getNodeName()).thenReturn("worker");
        when(state.getNodeMgr()).thenReturn(node);
        globals = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        globals.when(GlobalStateMgr::getServingState).thenReturn(state);
        globals.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        controller = mock(CheckpointController.class);
        Journal journal = mock(Journal.class);
        when(journal.getMaxJournalId()).thenReturn(100L);
        worker = new RecordingWorker(journal);
        worker.init();
        when(state.getCheckpointWorker()).thenReturn(worker);
    }

    @AfterEach
    public void tearDown() {
        globals.close();
        Config.meta_dir = oldMeta;
        Config.disable_image_v2_write = oldDisable;
    }

    @Test
    public void remoteRequestPolicyOverridesWorkerConfiguration() throws Exception {
        FrontendServiceImpl service = new FrontendServiceImpl(null);
        for (boolean leaderWritesV2 : new boolean[] {false, true}) {
            // Deliberately disagree with the request to verify that the leader controls the worker.
            Config.disable_image_v2_write = leaderWritesV2;
            TStartCheckpointRequest request = new TStartCheckpointRequest()
                    .setEpoch(1).setJournal_id(10).setIs_global_state_mgr(true).setWrite_v2(leaderWritesV2);
            assertEquals(TStatusCode.OK, service.startCheckpoint(request).getStatus().getStatus_code());
            worker.runAfterCatalogReady();
            assertEquals(leaderWritesV2, worker.savedV2);
        }
        assertEquals(2, worker.runs);
    }

    @Test
    public void legacyRemoteRequestDefaultsToWritingV2() throws Exception {
        Config.disable_image_v2_write = true;
        TStartCheckpointRequest request = new TStartCheckpointRequest()
                .setEpoch(1).setJournal_id(11).setIs_global_state_mgr(true);
        assertEquals(TStatusCode.OK,
                new FrontendServiceImpl(null).startCheckpoint(request).getStatus().getStatus_code());
        worker.runAfterCatalogReady();
        assertTrue(worker.savedV2);
        verify(controller).finishCheckpoint(11, "worker", null);
    }

    @Test
    public void incompletePairIsRetriedAtSameJournal() throws Exception {
        writeV3(temporary.resolve("image"), 20);
        worker.setNextCheckpoint(1, 20, false, true);
        worker.runAfterCatalogReady();
        assertEquals(1, worker.runs);
        assertTrue(worker.savedV2);
    }

    @Test
    public void completePairCanBeAcknowledgedWithoutRegeneration() throws Exception {
        writeV2(temporary.resolve("image"), 30, new byte[] {3});
        writeV3(temporary.resolve("image"), 30);
        worker.setNextCheckpoint(1, 30, false, true);
        worker.runAfterCatalogReady();
        assertEquals(0, worker.runs);
        verify(controller).finishCheckpoint(30, "worker", null);
    }

    @Test
    public void snapshotInfoRequiresLoadingEvenAnExistingCheckpoint() throws Exception {
        writeV3(temporary.resolve("image"), 40);
        worker.setNextCheckpoint(1, 40, true, false);
        worker.runAfterCatalogReady();
        assertEquals(1, worker.runs);
    }

    @Test
    public void missingImageZeroStillRunsCheckpoint() throws Exception {
        worker.setNextCheckpoint(1, 0, false, false);
        worker.runAfterCatalogReady();
        assertEquals(1, worker.runs);
    }

    @Test
    public void globalCheckpointInitializesWarehouseAfterImageBeforeReplay() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouses = new WarehouseManager();
        Mockito.doAnswer(invocation -> {
            assertNull(warehouses.getWarehouseAllowNull(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            return null;
        }).when(state).loadImage();
        Mockito.doAnswer(invocation -> {
            warehouses.initDefaultWarehouse();
            return null;
        }).when(state).initDefaultWarehouse();
        Mockito.doAnswer(invocation -> {
            assertNotNull(warehouses.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
            return null;
        }).when(state).replayJournal(10);
        globals.when(GlobalStateMgr::destroyCheckpoint).thenAnswer(invocation -> null);
        GlobalStateCheckpointWorker globalWorker = new GlobalStateCheckpointWorker(mock(Journal.class));
        globalWorker.init();
        globalWorker.doCheckpoint(1, 10, false);
        verify(state).replayJournal(10);
        verify(state).saveImage(true);
    }
}
