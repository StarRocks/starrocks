// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.qe.Coordinator;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MVMaintenanceJobTest {

    @Test
    void basic() throws Exception {
        MaterializedView view = new MaterializedView();
        view.setId(1024);
        view.setName("view1");
        view.setMaintenancePlan(new ExecPlan());
        view.setBaseTableIds(ImmutableSet.of(1L, 2L, 3L));

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        assertFalse(job.isRunnable());

        job.startJob();
        assertTrue(job.isRunnable());
        assertEquals(MVMaintenanceJob.JobState.STARTED, job.getState());

        new MockUp<Coordinator>() {
            @Mock
            public void prepareExec() throws Exception {
            }
        };
        job.onSchedule();

        new MockUp<TxnBasedEpochCoordinator>() {
            @Mock
            public void runEpoch(MVEpoch epoch) {
            }
        };

        job.onTransactionPublish();
        assertTrue(job.isRunnable());
        assertEquals(MVMaintenanceJob.JobState.RUN_EPOCH, job.getState());
        assertEquals(MVEpoch.EpochState.READY, job.getEpoch().getState());

        job.onSchedule();
        job.stopJob();
    }

    @Test
    void serialize() throws IOException {
        MaterializedView view = new MaterializedView();
        view.setId(1024);
        view.setName("view1");
        view.setMaintenancePlan(new ExecPlan());

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        job.write(buffer);
        byte[] bytes = buffer.getData();

        DataInput input = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        MVMaintenanceJob deserialized = MVMaintenanceJob.read(input);
        assertEquals(job, deserialized);

    }
}