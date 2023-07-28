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

package com.starrocks.qe.scheduler;

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

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.starrocks.qe.scheduler.dag.FragmentInstanceExecState.DeploymentResult;

public class Deployer {
    private static final Logger LOG = LogManager.getLogger(Deployer.class);

    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final TFragmentInstanceFactory tFragmentInstanceFactory;
    private final TDescriptorTable emptyDescTable;
    private final long deliveryTimeoutMs;

    private final FailureHandler failureHandler;

    private final Set<Long> deployedWorkerIds = Sets.newHashSet();

    public Deployer(ConnectContext context,
                    JobSpec jobSpec,
                    ExecutionDAG executionDAG,
                    TNetworkAddress coordAddress,
                    FailureHandler failureHandler) {
        this.jobSpec = jobSpec;
        this.executionDAG = executionDAG;

        this.tFragmentInstanceFactory = new TFragmentInstanceFactory(context, jobSpec, executionDAG, coordAddress);
        this.emptyDescTable = new TDescriptorTable()
                .setIs_cached(true)
                .setTupleDescriptors(Collections.emptyList());

        TQueryOptions queryOptions = jobSpec.getQueryOptions();
        this.deliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;

        this.failureHandler = failureHandler;
    }

    public void deployFragments(List<ExecutionFragment> concurrentFragments) throws RpcException, UserException {
        // Divide requests of fragments in the current group to two stages.
        // - stage 1, the first request to a host, which need send descTable.
        // - stage 2, the non-first requests to a host, which needn't send descTable.
        List<List<FragmentInstanceExecState>> twoStageExecutionsToDeploy = ImmutableList.of(new ArrayList<>(), new ArrayList<>());

        concurrentFragments.forEach(fragment -> this.createFragmentInstanceExecStates(fragment, twoStageExecutionsToDeploy));

        for (List<FragmentInstanceExecState> executions : twoStageExecutionsToDeploy) {
            executions.forEach(FragmentInstanceExecState::deployAsync);

            waitForDeploymentCompletion(executions);
        }
    }

    public interface FailureHandler {
        void apply(Status status, FragmentInstanceExecState execution, Throwable failure) throws RpcException, UserException;
    }

    private void createFragmentInstanceExecStates(ExecutionFragment fragment,
                                                  List<List<FragmentInstanceExecState>> twoStageExecutionsToDeploy) {
        Preconditions.checkState(!fragment.getInstances().isEmpty());

        // This is a load process, and it is the first fragment.
        // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
        // so that we can check these backends' state when joining this Coordinator
        boolean needCheckExecutionState = jobSpec.isLoadType() && fragment.getFragmentIndex() == 0;
        boolean isEnablePipeline = jobSpec.isEnablePipeline();
        // if pipeline is enable and current fragment contain olap table sink, in fe we will
        // calculate the number of all tablet sinks in advance and assign them to each fragment instance
        boolean enablePipelineTableSinkDop = isEnablePipeline && fragment.getPlanFragment().hasTableSink();

        List<List<FragmentInstance>> twoStageInstancesToDeploy = ImmutableList.of(new ArrayList<>(), new ArrayList<>());
        if (!isEnablePipeline) {
            twoStageInstancesToDeploy.get(0).addAll(fragment.getInstances());
        } else {
            fragment.getInstances().forEach(instance -> {
                if (deployedWorkerIds.contains(instance.getWorkerId())) {
                    twoStageInstancesToDeploy.get(1).add(instance);
                } else {
                    deployedWorkerIds.add(instance.getWorkerId());
                    twoStageInstancesToDeploy.get(0).add(instance);
                }
            });
        }

        int totalTableSinkDop = 0;
        if (enablePipelineTableSinkDop) {
            totalTableSinkDop = twoStageInstancesToDeploy.stream()
                    .flatMap(Collection::stream)
                    .mapToInt(FragmentInstance::getTableSinkDop)
                    .sum();
        }
        Preconditions.checkState(totalTableSinkDop >= 0,
                "tableSinkTotalDop = %d should be >= 0", totalTableSinkDop);

        int accTabletSinkDop = 0;
        for (int stageIndex = 0; stageIndex < twoStageInstancesToDeploy.size(); stageIndex++) {
            List<FragmentInstance> stageInstances = twoStageInstancesToDeploy.get(stageIndex);
            if (stageInstances.isEmpty()) {
                continue;
            }

            TDescriptorTable curDescTable;
            if (stageIndex == 0) {
                curDescTable = jobSpec.getDescTable();
            } else {
                curDescTable = emptyDescTable;
            }

            for (FragmentInstance instance : stageInstances) {
                TExecPlanFragmentParams request =
                        tFragmentInstanceFactory.create(instance, curDescTable, accTabletSinkDop, totalTableSinkDop);
                if (enablePipelineTableSinkDop) {
                    accTabletSinkDop += instance.getTableSinkDop();
                }

                FragmentInstanceExecState execution = FragmentInstanceExecState.createExecution(
                        jobSpec,
                        instance.getFragmentId(),
                        fragment.getFragmentIndex(),
                        request,
                        instance.getWorker());

                twoStageExecutionsToDeploy.get(stageIndex).add(execution);
                executionDAG.addExecution(execution);

                if (needCheckExecutionState) {
                    executionDAG.addNeedCheckExecution(execution);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add need check backend {} for fragment, {} job: {}",
                                execution.getWorker().getId(),
                                fragment.getFragmentId().asInt(), jobSpec.getLoadJobId());
                    }
                }
            }
        }
    }

    private void waitForDeploymentCompletion(List<FragmentInstanceExecState> executions) throws RpcException, UserException {
        DeploymentResult firstErrResult = null;
        FragmentInstanceExecState firstErrExecution = null;
        for (FragmentInstanceExecState execution : executions) {
            DeploymentResult res = execution.waitForDeploymentCompletion(deliveryTimeoutMs);
            if (TStatusCode.OK == res.getStatusCode()) {
                continue;
            }

            // Handle error results and cancel fragment instances, excluding TIMEOUT errors,
            // until all the delivered fragment instances are completed.
            // Otherwise, the cancellation RPC may arrive at BE before the delivery fragment instance RPC,
            // causing the instances to become stale and only able to be released after a timeout.
            if (firstErrResult == null) {
                firstErrResult = res;
                firstErrExecution = execution;
            }
            if (TStatusCode.TIMEOUT == res.getStatusCode()) {
                break;
            }
        }

        if (firstErrResult != null) {
            failureHandler.apply(firstErrResult.getStatus(), firstErrExecution, firstErrResult.getFailure());
        }
    }
}

