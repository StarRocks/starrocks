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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobInformation;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.qe.scheduler.ExecutionFragmentInstance.DeploymentResult;

public class Deployer {
    private static final Logger LOG = LogManager.getLogger(Deployer.class);

    private final JobInformation jobInformation;
    private final ExecutionDAG executionDAG;
    private final WorkerProvider workerProvider;

    private final TExecPlanFragmentParamsFactory execPlanFragmentParamsFactory;
    private final TDescriptorTable emptyDescTable;
    private final long deliveryTimeoutMs;

    private final FailureHandler failureHandler;

    private final Map<Long, Integer> workerId2FirstGroupIndex = Maps.newHashMap();
    private int nextGroupIndex = 0;
    private int nextProfileFragmentIndex = 0;

    public Deployer(ConnectContext context,
                    JobInformation jobInformation,
                    WorkerProvider workerProvider,
                    ExecutionDAG executionDAG,
                    TNetworkAddress coordAddress,
                    FailureHandler failureHandler) {
        this.jobInformation = jobInformation;
        this.executionDAG = executionDAG;
        this.workerProvider = workerProvider;

        this.execPlanFragmentParamsFactory =
                new TExecPlanFragmentParamsFactory(context, jobInformation, executionDAG, coordAddress);
        this.emptyDescTable = new TDescriptorTable()
                .setIs_cached(true)
                .setTupleDescriptors(Collections.emptyList());

        TQueryOptions queryOptions = jobInformation.getQueryOptions();
        this.deliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;

        this.failureHandler = failureHandler;
    }

    public boolean deployFragments(List<ExecutionFragment> concurrentFragments, boolean needDeploy) {
        int groupIndex = nextGroupIndex++;
        List<List<ExecutionFragmentInstance>> twoDeployStageExecutions =
                ImmutableList.of(new ArrayList<>(), new ArrayList<>());

        for (ExecutionFragment fragment : concurrentFragments) {
            int profileFragmentIndex = nextProfileFragmentIndex++;
            try {
                createExecutionFragmentInstances(fragment, groupIndex, profileFragmentIndex, twoDeployStageExecutions);
            } catch (UserException e) {
                failureHandler.apply(Status.createInternalError(e.getMessage()), null, e);
                return false;
            }
        }

        if (!needDeploy) {
            return true;
        }

        for (List<ExecutionFragmentInstance> executions : twoDeployStageExecutions) {
            executions.forEach(ExecutionFragmentInstance::deployAsync);
            if (!waitForDeploymentCompletion(executions)) {
                return false;
            }
        }

        return true;
    }

    public interface FailureHandler {
        void apply(Status status, ExecutionFragmentInstance execution, Throwable failure);
    }

    private void createExecutionFragmentInstances(ExecutionFragment fragment, int groupIndex, int profileFragmentIndex,
                                                  List<List<ExecutionFragmentInstance>> twoDeployStageExecutions)
            throws UserException {
        Preconditions.checkState(!fragment.getInstances().isEmpty());

        // This is a load process, and it is the first fragment.
        // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
        // so that we can check these backends' state when joining this Coordinator
        boolean needCheckExecutionState = jobInformation.isLoadType() && profileFragmentIndex == 0;
        boolean isEnablePipeline = jobInformation.isEnablePipeline();
        // if pipeline is enable and current fragment contain olap table sink, in fe we will
        // calculate the number of all tablet sinks in advance and assign them to each fragment instance
        boolean enablePipelineTableSinkDop = isEnablePipeline && fragment.getPlanFragment().hasTableSink();

        int totalTableSinkDop = 0;
        if (enablePipelineTableSinkDop) {
            totalTableSinkDop = fragment.getInstances().stream()
                    .map(FragmentInstance::getTableSinkDop)
                    .reduce(0, Integer::sum);
        }
        if (totalTableSinkDop < 0) {
            throw new UserException(
                    "tableSinkTotalDop = " + totalTableSinkDop + " should be >= 0");
        }

        int accTabletSinkDop = 0;
        Map<Long, List<FragmentInstance>> addrToInstances = fragment.geWorkerIdToInstances();
        for (Map.Entry<Long, List<FragmentInstance>> entry : addrToInstances.entrySet()) {
            Long workerId = entry.getKey();
            List<FragmentInstance> instances = entry.getValue();

            if (instances.isEmpty()) {
                return;
            }

            int deployStage = 0;
            TDescriptorTable curDescTable = jobInformation.getDescTable();
            if (isEnablePipeline) {
                Integer firstGroupIndex = workerId2FirstGroupIndex.get(workerId);
                if (firstGroupIndex == null) {
                    // Hasn't sent descTable for this host,
                    // so send descTable this time.
                    workerId2FirstGroupIndex.put(workerId, groupIndex);
                } else if (firstGroupIndex < groupIndex) {
                    // Has sent descTable for this host in the previous fragment group,
                    // so needn't wait and use cached descTable.
                    curDescTable = emptyDescTable;
                } else {
                    // The previous fragment for this host int the current fragment group will send descTable,
                    // so this fragment need wait until the previous one finishes delivering.
                    deployStage = 1;
                    curDescTable = emptyDescTable;
                }
            }

            for (FragmentInstance instance : instances) {
                TExecPlanFragmentParams request =
                        execPlanFragmentParamsFactory.create(instance, curDescTable, accTabletSinkDop,
                                totalTableSinkDop);
                if (enablePipelineTableSinkDop) {
                    accTabletSinkDop += instance.getTableSinkDop();
                }

                ExecutionFragmentInstance execution = ExecutionFragmentInstance.createExecution(
                        jobInformation,
                        instance,
                        request,
                        profileFragmentIndex,
                        workerProvider.getWorkerById(workerId));

                twoDeployStageExecutions.get(deployStage).add(execution);
                executionDAG.addExecution(execution);
                if (needCheckExecutionState) {
                    executionDAG.addNeedCheckExecution(execution);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add need check backend {} for fragment, {} job: {}",
                                execution.getBackend().getId(),
                                fragment.getFragmentId().asInt(),
                                jobInformation.getLoadJobId());
                    }
                }
            }
        }
    }

    private boolean waitForDeploymentCompletion(List<ExecutionFragmentInstance> executions) {
        DeploymentResult firstErrorResult = null;
        ExecutionFragmentInstance firstErrExecution = null;
        for (ExecutionFragmentInstance execution : executions) {
            DeploymentResult res = execution.waitForDeploymentCompletion(deliveryTimeoutMs);
            if (res.getStatusCode() == TStatusCode.OK) {
                continue;
            }

            // Handle error results and cancel fragment instances, excluding TIMEOUT errors,
            // until all the delivered fragment instances are completed.
            // Otherwise, the cancellation RPC may arrive at BE before the delivery fragment instance RPC,
            // causing the instances to become stale and only able to be released after a timeout.
            if (firstErrorResult == null) {
                firstErrorResult = res;
                firstErrExecution = execution;
            }
            if (res.getStatusCode() == TStatusCode.TIMEOUT) {
                break;
            }
        }

        boolean hasErr = firstErrorResult != null;
        if (hasErr) {
            failureHandler.apply(firstErrorResult.getStatus(), firstErrExecution, firstErrorResult.getFailure());
        }
        return !hasErr;
    }
}
