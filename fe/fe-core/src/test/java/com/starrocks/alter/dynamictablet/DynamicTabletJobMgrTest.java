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

package com.starrocks.alter.dynamictablet;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class DynamicTabletJobMgrTest {
    public static class TestNormalDynamicTabletJob extends DynamicTabletJob {

        public TestNormalDynamicTabletJob(long jobId, DynamicTabletJob.JobType jobType, long dbId, long tableId) {
            super(jobId, jobType, dbId, tableId, Collections.emptyMap());
        }

        @Override
        protected void runPendingJob() {
            setJobState(JobState.PREPARING);
        }

        @Override
        protected void runPreparingJob() {
            setJobState(JobState.RUNNING);
        }

        @Override
        protected void runRunningJob() {
            setJobState(JobState.CLEANING);
        }

        @Override
        protected void runCleaningJob() {
            setJobState(JobState.FINISHED);
        }

        @Override
        protected void runAbortingJob() {
            setJobState(JobState.ABORTED);
        }

        @Override
        protected boolean canAbort() {
            return true;
        }

        @Override
        public long getParallelTablets() {
            return 10;
        }

        @Override
        public void replay() {
            return;
        }
    }

    public static class TestAbnormalDynamicTabletJob extends TestNormalDynamicTabletJob {

        public TestAbnormalDynamicTabletJob(long jobId, DynamicTabletJob.JobType jobType, long dbId, long tableId) {
            super(jobId, jobType, dbId, tableId);
        }

        @Override
        protected void runPreparingJob() {
            abort("Abort");
        }

        @Override
        public long getParallelTablets() {
            return Config.dynamic_tablet_max_parallel_tablets / 2;
        }
    }

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testDynamicTabletJobMgr() throws Exception {
        DynamicTabletJobMgr jobMgr = new DynamicTabletJobMgr();

        TestNormalDynamicTabletJob normalJob = new TestNormalDynamicTabletJob(1, null, 0, 0);
        jobMgr.addDynamicTabletJob(normalJob);
        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addDynamicTabletJob(normalJob));

        TestAbnormalDynamicTabletJob abnormalJob = new TestAbnormalDynamicTabletJob(2, null, 0, 1);
        jobMgr.addDynamicTabletJob(abnormalJob);

        TestAbnormalDynamicTabletJob abnormalJob2 = new TestAbnormalDynamicTabletJob(3, null, 0, 2);
        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addDynamicTabletJob(abnormalJob2));

        Assertions.assertEquals(2, jobMgr.getDynamicTabletJobs().size());
        Assertions.assertEquals(normalJob.getParallelTablets() + abnormalJob.getParallelTablets(),
                jobMgr.getTotalParalelTablets());

        jobMgr.runAfterCatalogReady();

        Assertions.assertEquals(DynamicTabletJob.JobState.FINISHED, normalJob.getJobState());
        Assertions.assertEquals(DynamicTabletJob.JobState.ABORTED, abnormalJob.getJobState());
        Assertions.assertTrue(normalJob.isDone());
        Assertions.assertTrue(abnormalJob.isDone());
        Assertions.assertFalse(normalJob.isExpired());
        Assertions.assertFalse(abnormalJob.isExpired());
        Assertions.assertEquals(2, jobMgr.getDynamicTabletJobs().size());
        Assertions.assertEquals(0, jobMgr.getTotalParalelTablets());

        abnormalJob.finishedTimeMs = 0;
        Assertions.assertTrue(abnormalJob.isExpired());

        jobMgr.runAfterCatalogReady();

        Assertions.assertEquals(1, jobMgr.getDynamicTabletJobs().size());
    }

    @Test
    public void testGetDynamicTabletJobsInfo() throws Exception {
        DynamicTabletJobMgr jobMgr = new DynamicTabletJobMgr();

        TestNormalDynamicTabletJob job1 = new TestNormalDynamicTabletJob(1, DynamicTabletJob.JobType.SPLIT_TABLET, 0, 1);
        TestNormalDynamicTabletJob job2 = new TestNormalDynamicTabletJob(2, DynamicTabletJob.JobType.MERGE_TABLET, 0, 2);
        jobMgr.addDynamicTabletJob(job1);
        jobMgr.addDynamicTabletJob(job2);

        Assertions.assertEquals(2, jobMgr.getAllJobsInfo().getItems().size());
    }
}
