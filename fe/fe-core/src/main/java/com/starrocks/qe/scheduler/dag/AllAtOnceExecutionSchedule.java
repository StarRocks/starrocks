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

import com.starrocks.common.StarRocksException;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

// all at once execution schedule only schedule once.
public class AllAtOnceExecutionSchedule implements ExecutionSchedule {
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
            if (cancelled) {
                return;
            }
            try {
                states = coordinator.assignIncrementalScanRangesToDeployStates(deployer, states);
                if (states.isEmpty()) {
                    return;
                }
                for (DeployState state : states) {
                    deployer.deployFragments(state);
                }
            } catch (StarRocksException | RpcException e) {
                throw new RuntimeException(e);
            }
            // jvm should use tail optimization.
            start();
        }

        public void start() {
            if (executorService != null) {
                executorService.submit(this);
            } else {
                this.run();
            }
        }
    }

    @Override
    public void prepareSchedule(Coordinator coordinator, Deployer deployer, ExecutionDAG dag) {
        this.coordinator = coordinator;
        this.deployer = deployer;
        this.dag = dag;
    }

    @Override
    public void schedule(Coordinator.ScheduleOption option) throws RpcException, StarRocksException {
        List<DeployState> states = new ArrayList<>();
        for (List<ExecutionFragment> executionFragments : dag.getFragmentsInTopologicalOrderFromRoot()) {
            final DeployState deployState = deployer.createFragmentExecStates(executionFragments);
            deployer.deployFragments(deployState);
            states.add(deployState);
        }

        ExecutorService executorService = null;
        if (option.useQueryDeployExecutor) {
            executorService = GlobalStateMgr.getCurrentState().getQueryDeployExecutor();
        }

        DeployMoreScanRangesTask task = new DeployMoreScanRangesTask(states, executorService);
        task.start();
    }

    public void tryScheduleNextTurn(TUniqueId fragmentInstanceId) {
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
