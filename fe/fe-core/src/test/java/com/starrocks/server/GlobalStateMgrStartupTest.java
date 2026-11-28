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

package com.starrocks.server;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalStateMgrStartupTest {
    private GlobalStateMgr waitingFor(ReplicatedEnvironment environment) throws Exception {
        BDBEnvironment bdb = mock(BDBEnvironment.class);
        when(bdb.getReplicatedEnvironment()).thenReturn(environment);
        // Exercise the real wait loop and journal without starting a replayer or a cluster.
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        doCallRealMethod().when(state).waitForReady();
        Field journal = GlobalStateMgr.class.getDeclaredField("journal");
        journal.setAccessible(true);
        journal.set(state, new BDBJEJournal(bdb));
        return state;
    }

    @Test
    void testInvalidEnvironmentBeforeReplayerStarts() throws Exception {
        ReplicatedEnvironment environment = mock(ReplicatedEnvironment.class);
        EnvironmentFailureException failure = mock(EnvironmentFailureException.class);
        when(environment.getInvalidatingException()).thenReturn(failure);
        GlobalStateMgr state = waitingFor(environment);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            JournalException thrown = assertThrows(JournalException.class, state::waitForReady);
            assertSame(failure, thrown.getCause());
        });
    }

    @Test
    void testEnvironmentInvalidatedWhileWaiting() throws Exception {
        ReplicatedEnvironment environment = mock(ReplicatedEnvironment.class);
        when(environment.isValid()).thenReturn(true, false);
        GlobalStateMgr state = waitingFor(environment);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThrows(JournalException.class, state::waitForReady));
    }

    @Test
    void testHealthyEnvironmentCanWaitUntilReady() throws Exception {
        ReplicatedEnvironment environment = mock(ReplicatedEnvironment.class);
        when(environment.isValid()).thenReturn(true);
        GlobalStateMgr state = waitingFor(environment);
        when(state.isReady()).thenReturn(false, false, true);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertDoesNotThrow(state::waitForReady));
    }

    @Test
    void testNormallyClosedEnvironmentIsNotReportedAsInvalid() throws Exception {
        ReplicatedEnvironment environment = mock(ReplicatedEnvironment.class);
        when(environment.isClosed()).thenReturn(true);
        GlobalStateMgr state = waitingFor(environment);
        when(state.isReady()).thenReturn(false, true);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertDoesNotThrow(state::waitForReady));
    }
}
