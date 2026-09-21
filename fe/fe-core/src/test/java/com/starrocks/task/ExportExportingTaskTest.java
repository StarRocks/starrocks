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

package com.starrocks.task;

import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.load.ExportJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ExportExportingTaskTest {

    @Test
    public void testInterruptedAwaitUsesNormalExportFailurePath() {
        java.util.concurrent.atomic.AtomicBoolean profileRegistered = new java.util.concurrent.atomic.AtomicBoolean();
        new mockit.MockUp<ProfileManager>() {
            @mockit.Mock
            public String pushProfile(ProfilingExecPlan plan, RuntimeProfile profile) {
                Assertions.assertEquals("Query", profile.getName());
                profileRegistered.set(true);
                return "export-profile";
            }
        };
        ExportJob job = Mockito.mock(ExportJob.class);
        Mockito.when(job.getState()).thenReturn(ExportJob.JobState.EXPORTING);
        Mockito.when(job.getTimeoutSecond()).thenReturn(3600);
        Mockito.when(job.getCreateTimeMs()).thenReturn(System.currentTimeMillis());
        Mockito.when(job.isReplayed()).thenReturn(false);
        Mockito.when(job.getCoordList()).thenReturn(Collections.emptyList());
        Mockito.when(job.getSql()).thenReturn("EXPORT TABLE test_table");

        ExportExportingTask task = new ExportExportingTask(job);
        try {
            // A plain wait failure is not a leader handoff; retain normal job failure/profile handling.
            Thread.currentThread().interrupt();
            task.exec();
            Assertions.assertFalse(Thread.currentThread().isInterrupted());
        } finally {
            // Clear the flag so it cannot leak into other tests on this worker thread.
            Thread.interrupted();
        }

        Mockito.verify(job).cancelInternal(com.starrocks.load.ExportFailMsg.CancelType.TIMEOUT, "timeout");
        Mockito.verify(job, Mockito.never()).finish();
        Mockito.verify(job).setDoExportingThread(null);
        Assertions.assertTrue(profileRegistered.get());
    }
    @Test
    public void testCooperativeDemotionLeavesWaitingExportForNextLeader() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean demoting = new java.util.concurrent.atomic.AtomicBoolean();
        new mockit.MockUp<com.starrocks.server.GlobalStateMgr>() {
            @mockit.Mock
            public boolean isLeaderDemoting() {
                return demoting.get();
            }
        };
        ExportJob job = Mockito.mock(ExportJob.class);
        Mockito.when(job.getState()).thenReturn(ExportJob.JobState.EXPORTING);
        Mockito.when(job.getTimeoutSecond()).thenReturn(3600);
        Mockito.when(job.getCreateTimeMs()).thenReturn(System.currentTimeMillis());
        Mockito.when(job.getCoordList()).thenReturn(Collections.emptyList());
        ExportExportingTask task = new ExportExportingTask(job);
        com.starrocks.common.jmockit.Deencapsulation.setField(task, "subTasksDoneSignal",
                new com.starrocks.common.util.concurrent.MarkedCountDownLatch<Integer, Integer>(1));
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                task.exec();
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        worker.setDaemon(true);
        worker.start();
        try {
            org.awaitility.Awaitility.await().atMost(3, java.util.concurrent.TimeUnit.SECONDS)
                    .until(() -> worker.getState() == Thread.State.TIMED_WAITING);
            demoting.set(true);
            worker.join(3000L);
            Assertions.assertFalse(worker.isAlive());
            Assertions.assertNull(failure.get());
            Assertions.assertFalse(worker.isInterrupted());
            Mockito.verify(job, Mockito.never()).cancelInternal(Mockito.any(), Mockito.anyString());
            Mockito.verify(job, Mockito.never()).finish();
            Mockito.verify(job).setDoExportingThread(null);
        } finally {
            demoting.set(true);
            worker.join(3000L);
        }
    }

}
