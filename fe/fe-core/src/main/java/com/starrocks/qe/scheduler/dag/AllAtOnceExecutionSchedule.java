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

package com.starrocks.qe.scheduler.dag;

<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.scheduler.Coordinator;
>>>>>>> d8ff13b53b ([BugFix] fix query hang because of incorrect scan range delivery (#61562))
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// all at once execution schedule only schedule once.
public class AllAtOnceExecutionSchedule implements ExecutionSchedule {
<<<<<<< HEAD
    private Deployer deployer;
    private ExecutionDAG dag;
=======
    private static final Logger LOG = LogManager.getLogger(AllAtOnceExecutionSchedule.class);
    private Coordinator coordinator;
    private Deployer deployer;
    private ExecutionDAG dag;
    private volatile boolean cancelled = false;

    class DeployMoreScanRangesTask implements Runnable {
        List<DeployState> states;
        private ExecutorService executorService;

        DeployMoreScanRangesTask(List<DeployState> states, ExecutorService executorService) {
            this.states = states;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            while (!cancelled && !states.isEmpty()) {
                try {
                    states = coordinator.assignIncrementalScanRangesToDeployStates(deployer, states);
                    if (states.isEmpty()) {
                        return;
                    }
                    for (DeployState state : states) {
                        deployer.deployFragments(state);
                    }
                } catch (StarRocksException | RpcException e) {
                    LOG.warn("Failed to assign incremental scan ranges to deploy states", e);
                    coordinator.cancel(PPlanFragmentCancelReason.INTERNAL_ERROR, e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        public void start() {
            if (executorService != null) {
                executorService.submit(this);
            } else {
                this.run();
            }
        }
    }
>>>>>>> d8ff13b53b ([BugFix] fix query hang because of incorrect scan range delivery (#61562))

    @Override
    public void prepareSchedule(Deployer deployer, ExecutionDAG dag) {
        this.deployer = deployer;
        this.dag = dag;
    }

    @Override
    public void schedule() throws RpcException, UserException {
        for (List<ExecutionFragment> executionFragments : dag.getFragmentsInTopologicalOrderFromRoot()) {
            final DeployState deployState = deployer.createFragmentExecStates(executionFragments);
            deployer.deployFragments(deployState);
        }
    }

    public void tryScheduleNextTurn(TUniqueId fragmentInstanceId) {
    }
}
