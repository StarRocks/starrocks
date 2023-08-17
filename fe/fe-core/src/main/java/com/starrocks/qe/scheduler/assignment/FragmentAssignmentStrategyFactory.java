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

package com.starrocks.qe.scheduler.assignment;

import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.JobSpec;

import java.util.Random;

public class FragmentAssignmentStrategyFactory {

    private final Random random = new Random(System.currentTimeMillis()); // NOSONAR

    private final ConnectContext connectContext;
    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    public FragmentAssignmentStrategyFactory(ConnectContext connectContext,
                                             JobSpec jobSpec,
                                             ExecutionDAG executionDAG) {
        this.connectContext = connectContext;
        this.jobSpec = jobSpec;
        this.executionDAG = executionDAG;
    }

    public FragmentAssignmentStrategy create(ExecutionFragment execFragment, WorkerProvider workerProvider) {
        PlanNode leftMostNode = execFragment.getLeftMostNode();
        boolean isRemoteFragment = !(leftMostNode instanceof ScanNode);
        if (isRemoteFragment) {
            return new RemoteFragmentAssignmentStrategy(connectContext, workerProvider, jobSpec.isEnablePipeline(),
                    executionDAG.isGatherOutput(), random);
        } else {
            return new LocalFragmentAssignmentStrategy(connectContext, workerProvider, jobSpec.isEnablePipeline(),
                    jobSpec.isLoadType());
        }
    }
}
