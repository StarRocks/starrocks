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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class SplitTabletJob extends DynamicTabletJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    public SplitTabletJob(long jobId, long dbId, long tableId,
            Map<Long, PhysicalPartitionContext> physicalPartitionContexts) {
        super(jobId, JobType.SPLIT_TABLET, dbId, tableId, physicalPartitionContexts);
    }

    // Begin and commit the split transaction
    @Override
    protected void runPendingJob() {

    }

    // Wait for previous version published, than publish the split transaction
    @Override
    protected void runPreparingJob() {

    }

    // Wait for the publish finished, than replace old tablets with new tablets
    @Override
    protected void runRunningJob() {

    }

    // Wait for all previous transactions finished, than delete old tablets
    @Override
    protected void runCleaningJob() {

    }

    // Clear new tablets
    @Override
    protected void runAbortingJob() {

    }

    // Can abort only when job state is PENDING
    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    @Override
    public void replay() {

    }
}
