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

public class TabletReshardJobMgrTest {
    public static class TestNormalTabletReshardJob extends TabletReshardJob {

        public TestNormalTabletReshardJob(long jobId, TabletReshardJob.JobType jobType, long dbId, long tableId) {
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

    public static class TestAbnormalTabletReshardJob extends TestNormalTabletReshardJob {

        public TestAbnormalTabletReshardJob(long jobId, TabletReshardJob.JobType jobType, long dbId, long tableId) {
            super(jobId, jobType, dbId, tableId);
        }

        @Override
        protected void runPreparingJob() {
            abort("Abort");
        }

        @Override
        public long getParallelTablets() {
            return Config.tablet_reshard_max_parallel_tablets / 2;
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
    public void testTabletReshardJobMgr() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob normalJob = new TestNormalTabletReshardJob(1, null, 0, 0);
        jobMgr.addTabletReshardJob(normalJob);
        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(normalJob));

        TestAbnormalTabletReshardJob abnormalJob = new TestAbnormalTabletReshardJob(2, null, 0, 1);
        jobMgr.addTabletReshardJob(abnormalJob);

        TestAbnormalTabletReshardJob abnormalJob2 = new TestAbnormalTabletReshardJob(3, null, 0, 2);
        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(abnormalJob2));

        Assertions.assertEquals(2, jobMgr.getTabletReshardJobs().size());
        Assertions.assertEquals(normalJob.getParallelTablets() + abnormalJob.getParallelTablets(),
                jobMgr.getTotalParallelTablets());

        jobMgr.runAfterCatalogReady();

        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, normalJob.getJobState());
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, abnormalJob.getJobState());
        Assertions.assertTrue(normalJob.isDone());
        Assertions.assertTrue(abnormalJob.isDone());
        Assertions.assertFalse(normalJob.isExpired());
        Assertions.assertFalse(abnormalJob.isExpired());
        Assertions.assertEquals(2, jobMgr.getTabletReshardJobs().size());
        Assertions.assertEquals(0, jobMgr.getTotalParallelTablets());

        abnormalJob.finishedTimeMs = 0;
        Assertions.assertTrue(abnormalJob.isExpired());

        jobMgr.runAfterCatalogReady();

        Assertions.assertEquals(1, jobMgr.getTabletReshardJobs().size());
    }

    @Test
    public void testGetTabletReshardJobsInfo() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob job1 = new TestNormalTabletReshardJob(1, TabletReshardJob.JobType.SPLIT_TABLET, 0,
                1);
        TestNormalTabletReshardJob job2 = new TestNormalTabletReshardJob(2, TabletReshardJob.JobType.MERGE_TABLET, 0,
                2);
        jobMgr.addTabletReshardJob(job1);
        jobMgr.addTabletReshardJob(job2);

        Assertions.assertEquals(2, jobMgr.getAllJobsInfo().getItems().size());
    }
}
