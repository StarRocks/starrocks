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

import com.starrocks.load.ExportJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ExportExportingTaskTest {

    @Test
    public void testInterruptDuringAwaitLeavesJobExportingForNextLeader() {
        // Leader demotion stops the export executors with shutdownNow(), which interrupts the
        // thread running exec(). The interrupt must be treated as a shutdown signal: the job
        // stays EXPORTING so the next leader reschedules it - it must NOT be cancelled as a
        // business TIMEOUT - and the doExportingThread reference must be released either way.
        ExportJob job = Mockito.mock(ExportJob.class);
        Mockito.when(job.getState()).thenReturn(ExportJob.JobState.EXPORTING);
        Mockito.when(job.getTimeoutSecond()).thenReturn(3600);
        Mockito.when(job.getCreateTimeMs()).thenReturn(System.currentTimeMillis());
        Mockito.when(job.isReplayed()).thenReturn(false);
        Mockito.when(job.getCoordList()).thenReturn(Collections.emptyList());

        ExportExportingTask task = new ExportExportingTask(job);
        try {
            // With the interrupt status set on entry, subTasksDoneSignal.await() throws
            // InterruptedException immediately (even with count == 0), exercising the
            // shutdown branch of exec().
            Thread.currentThread().interrupt();
            task.exec();
            Assertions.assertTrue(Thread.currentThread().isInterrupted(),
                    "exec must re-assert the interrupt so the pool thread unwinds promptly");
        } finally {
            // Clear the flag so it cannot leak into other tests on this worker thread.
            Thread.interrupted();
        }

        Mockito.verify(job, Mockito.never()).cancelInternal(Mockito.any(), Mockito.anyString());
        Mockito.verify(job, Mockito.never()).finish();
        Mockito.verify(job).setDoExportingThread(null);
    }
}
