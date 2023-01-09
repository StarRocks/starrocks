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


package com.starrocks.scheduler.mv;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

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

        List<MaterializedView.BaseTableInfo> baseTableInfos = Lists.newArrayList();
        MaterializedView.BaseTableInfo baseTableInfo1 = new MaterializedView.BaseTableInfo(100L, 1L);
        baseTableInfos.add(baseTableInfo1);
        MaterializedView.BaseTableInfo baseTableInfo2 = new MaterializedView.BaseTableInfo(100L, 2L);
        baseTableInfos.add(baseTableInfo2);
        MaterializedView.BaseTableInfo baseTableInfo3 = new MaterializedView.BaseTableInfo(100L, 2L);
        baseTableInfos.add(baseTableInfo3);

        view.setBaseTableInfos(baseTableInfos);

        MVMaintenanceJob job = new MVMaintenanceJob(view);
        assertFalse(job.isRunnable());

        job.startJob();
        assertTrue(job.isRunnable());
        assertEquals(MVMaintenanceJob.JobState.STARTED, job.getState());

        new MockUp<CoordinatorPreprocessor>() {
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