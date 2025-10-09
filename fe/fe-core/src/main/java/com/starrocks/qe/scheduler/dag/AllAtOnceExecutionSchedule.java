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
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

// all at once execution schedule only schedule once.
public class AllAtOnceExecutionSchedule implements ExecutionSchedule {
    private static final Logger LOG = LogManager.getLogger(AllAtOnceExecutionSchedule.class);
    private Coordinator coordinator;
    private Deployer deployer;
    private ExecutionDAG dag;
    private volatile boolean cancelled = false;
    private DeployScanRangesTask deployScanRangesTask = null;

    class TracerContext implements AutoCloseable {
        Tracers savedTracers;
        ConnectContext savedConnectContext;

        TracerContext(Tracers currentTracers, ConnectContext connectContext) {
            if (currentTracers != null) {
                savedTracers = Tracers.get();
                Tracers.set(currentTracers);
            }
            if (connectContext != null) {
                savedConnectContext = ConnectContext.get();
                ConnectContext.set(connectContext);
            }
        }

        @Override
        public void close() {
            if (savedTracers != null) {
                Tracers.set(savedTracers);
            }
            if (savedConnectContext != null) {
                ConnectContext.set(savedConnectContext);
            }
        }
    }

    class DeployScanRangesTask implements Runnable {
        List<DeployState> states;
        ExecutorService executorService;
        Tracers currentTracers;
        ConnectContext currentConnectContext;

        DeployScanRangesTask(List<DeployState> states) {
            this.states = states;
        }

        @Override
        public void run() {
            if (cancelled || states.isEmpty()) {
                return;
            }
            try (TracerContext tracerContext = new TracerContext(currentTracers, currentConnectContext)) {
                runOnce();
            }
            // If run in the executor service, we need to start the next turn.
            // To submit this task again so all queries could get the same opportunity to run by queueing up
            start();
        }

        public void runOnce() {
            try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployScanRanges")) {
                states = coordinator.assignIncrementalScanRangesToDeployStates(deployer, states);
                if (states.isEmpty()) {
                    return;
                }
                for (DeployState state : states) {
                    deployer.deployFragments(state);
                }
            } catch (Exception e) {
                // there could be a lot of reasons to fail, just cancel the query
                LOG.warn("Failed to assign incremental scan ranges to deploy states", e);
                coordinator.cancel(PPlanFragmentCancelReason.INTERNAL_ERROR, e.getMessage());
                throw new RuntimeException(e);
            }
        }

        public void start() {
            if (executorService != null) {
                // Run in the executor service.
                executorService.submit(this);
            } else {
                // Run in the main thread.
                while (!cancelled && !states.isEmpty()) {
                    this.runOnce();
                }
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

        deployScanRangesTask = new DeployScanRangesTask(states);
        if (option.useQueryDeployExecutor) {
            deployScanRangesTask.executorService = GlobalStateMgr.getCurrentState().getQueryDeployExecutor();
            deployScanRangesTask.currentTracers = Tracers.get();
            deployScanRangesTask.currentConnectContext = ConnectContext.get();
        }
    }

    @Override
    public void continueSchedule(Coordinator.ScheduleOption option) throws RpcException, StarRocksException {
        deployScanRangesTask.start();
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
