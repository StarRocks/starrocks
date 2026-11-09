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

package com.starrocks.alter.reshard;

import com.starrocks.common.StarRocksException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReshardAdmissionTest {
    @ParameterizedTest
    @ValueSource(strings = {"activation", "follower", "owner", "manager", "epoch", "zero", "error"})
    void invalidationRejectsOriginalAdmissionAndCapture(String reason) throws Exception {
        GlobalStateMgr owner = mock(GlobalStateMgr.class);
        TabletReshardJobMgr manager = mock(TabletReshardJobMgr.class);
        when(owner.isLeader()).thenReturn(true);
        when(owner.getEpoch()).thenReturn(7L);
        when(owner.getTabletReshardJobMgr()).thenReturn(manager);
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(owner);
            ReshardAdmission admission = ReshardAdmission.capture(owner, manager);
            assertTrue(admission.isValid());
            switch (reason) {
                case "activation" -> when(owner.isInTransferringToLeader()).thenReturn(true);
                case "follower" -> when(owner.isLeader()).thenReturn(false);
                case "owner" -> global.when(GlobalStateMgr::getCurrentState).thenReturn(mock(GlobalStateMgr.class));
                case "manager" -> when(owner.getTabletReshardJobMgr()).thenReturn(mock(TabletReshardJobMgr.class));
                case "epoch" -> when(owner.getEpoch()).thenReturn(8L);
                case "zero" -> when(owner.getEpoch()).thenReturn(0L);
                case "error" -> when(owner.getEpoch()).thenThrow(new IllegalStateException("epoch unavailable"));
                default -> throw new AssertionError(reason);
            }
            assertFalse(admission.isValid());
            assertThrows(StarRocksException.class, admission::checkValid);
            if (!reason.equals("epoch")) {
                assertThrows(StarRocksException.class, () -> ReshardAdmission.capture(owner, manager));
            } else {
                assertTrue(ReshardAdmission.capture(owner, manager).isValid());
                assertFalse(admission.isValid());
            }
        }
    }

    @Test
    void jobCannotRecaptureAndRecoveryDoesNotInventAdmission() throws Exception {
        GlobalStateMgr owner = mock(GlobalStateMgr.class);
        TabletReshardJobMgr manager = mock(TabletReshardJobMgr.class);
        when(owner.isLeader()).thenReturn(true);
        when(owner.getEpoch()).thenReturn(7L);
        when(owner.getTabletReshardJobMgr()).thenReturn(manager);
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(owner);
            TabletReshardJob job = new SplitTabletJob(1L, 2L, 3L, Map.of());
            assertFalse(job.isAdmissionLeaderValid());
            ReshardAdmission admission = ReshardAdmission.capture(owner, manager);
            job.captureAdmission(admission);
            assertTrue(job.isAdmissionLeaderValid());
            assertThrows(IllegalStateException.class, () -> job.captureAdmission(admission));
            TabletReshardJob recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), TabletReshardJob.class);
            recovered.initializeAfterRecovery();
            assertTrue(recovered.isAdmissionReady());
            assertFalse(recovered.isAdmissionLeaderValid());
        }
    }
}
