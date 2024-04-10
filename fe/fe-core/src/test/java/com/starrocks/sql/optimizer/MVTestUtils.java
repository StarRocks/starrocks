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

package com.starrocks.sql.optimizer;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.util.Map;

public class MVTestUtils {
    private static final Logger LOG = LogManager.getLogger(MVTestUtils.class);

    public static void waitingRollupJobV2Finish() throws Exception {
        // waiting alterJobV2 finish
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        //Assert.assertEquals(1, alterJobs.size());

        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
            }
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            Assert.assertEquals(String.format("AlterJobV2 %s should be ROLLUP state at the end",
                            GsonUtils.GSON.toJson(alterJobV2)),
                    AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    public static void waitForSchemaChangeAlterJobFinish() throws Exception {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }
}
