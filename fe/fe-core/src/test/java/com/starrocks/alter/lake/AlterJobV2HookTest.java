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

package com.starrocks.alter.lake;

import com.starrocks.alter.AlterCancelException;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.warehouse.WarehouseIdleChecker;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class AlterJobV2HookTest {

    @Test
    public void testHook() {
        AlterJobV2 alterJobV2 = new MockedAlterJobV2(AlterJobV2.JobType.OPTIMIZE);
        alterJobV2.setWarehouseId(1L);
        alterJobV2.setJobState(AlterJobV2.JobState.RUNNING);
        long ts = System.currentTimeMillis();
        alterJobV2.cancel("failed");
        Assert.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(1L));

        alterJobV2.setJobState(AlterJobV2.JobState.RUNNING);
        ts = System.currentTimeMillis();
        alterJobV2.run();
        Assert.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(1L));

        alterJobV2.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        ts = System.currentTimeMillis();
        alterJobV2.run();
        Assert.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(1L));
    }

    public static class MockedAlterJobV2 extends AlterJobV2 {
        public MockedAlterJobV2(JobType jobType) {
            super(jobType);
        }

        @Override
        protected void runPendingJob() throws AlterCancelException {
        }

        @Override
        protected void runWaitingTxnJob() throws AlterCancelException {
        }

        @Override
        protected void runRunningJob() throws AlterCancelException {
            throw new AlterCancelException("cancelled");
        }

        @Override
        protected void runFinishedRewritingJob() throws AlterCancelException {
        }

        @Override
        protected boolean cancelImpl(String errMsg) {
            return true;
        }

        @Override
        protected void getInfo(List<List<Comparable>> infos) {
        }

        @Override
        public void replay(AlterJobV2 replayedJob) {
        }

        @Override
        public Optional<Long> getTransactionId() {
            return Optional.of(100L);
        }

        @Override
        public void write(DataOutput out) throws IOException {
        }
    }
}
