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

import com.google.common.collect.Lists;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Maintain the execution state of a fragment instance.
 * Executions begin in the state CREATED and transition between states following this diagram:
 *
 * <pre>{@code
 * CREATED ────► DEPLOYING ────► EXECUTING ───► FINISHED
 *                  │                │
 *                  │                │
 *                  │                ▼
 *                  ├──────────► CANCELLING
 *                  │                │
 *                  │                │
 *                  ▼                │
 *               FAILED ◄────────────┘
 * }
 * </pre>
 *
 * All the methods are thead-safe.
 * The {@link #state} and {@link #profile} are protected by {@code synchronized(this)}.
 */
public class ExecutionFragmentInstance {
    private static final Logger LOG = LogManager.getLogger(ExecutionFragmentInstance.class);

    private State state = State.CREATED;

    private final JobSpec jobSpec;
    private final PlanFragmentId fragmentId;
    private final TUniqueId fragmentInstanceId;
    private final int indexInJob;

    /**
     * request and future will be cleaned after deployment completion.
     */
    private TExecPlanFragmentParams requestToDeploy;
    private Future<PExecPlanFragmentResult> deployFuture = null;

    private final int profileFragmentId;
    private final RuntimeProfile profile;

    private final ComputeNode worker;
    private final TNetworkAddress address;
    private final long lastMissingHeartbeatTime;

    /**
     * Create a fake backendExecState, only user for stream load profile.
     */
    public static ExecutionFragmentInstance createFakeExecution(TUniqueId fragmentInstanceId,
                                                                TNetworkAddress address) {
        String name = "Instance " + DebugUtil.printId(fragmentInstanceId);
        RuntimeProfile profile = new RuntimeProfile(name);
        profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));

        return new ExecutionFragmentInstance(null, null, fragmentInstanceId, 0, null, 0, profile, null, null, -1);
    }

    public static ExecutionFragmentInstance createExecution(JobSpec jobSpec,
                                                            PlanFragmentId fragmentId,
                                                            TExecPlanFragmentParams request,
                                                            int profileFragmentId,
                                                            ComputeNode worker) {
        TNetworkAddress address = worker.getAddress();
        String name = "Instance " + DebugUtil.printId(request.params.fragment_instance_id) + " (host=" + address + ")";
        RuntimeProfile profile = new RuntimeProfile(name);
        profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));

        return new ExecutionFragmentInstance(jobSpec,
                fragmentId, request.params.getFragment_instance_id(), request.getBackend_num(),
                request,
                profileFragmentId, profile,
                worker, address, worker.getLastMissingHeartbeatTime());

    }

    private ExecutionFragmentInstance(JobSpec jobSpec,
                                      PlanFragmentId fragmentId,
                                      TUniqueId fragmentInstanceId,
                                      int indexInJob,
                                      TExecPlanFragmentParams requestToDeploy,
                                      int profileFragmentId,
                                      RuntimeProfile profile,
                                      ComputeNode worker,
                                      TNetworkAddress address,
                                      long lastMissingHeartbeatTime) {
        this.jobSpec = jobSpec;
        this.fragmentId = fragmentId;
        this.fragmentInstanceId = fragmentInstanceId;
        this.indexInJob = indexInJob;

        this.requestToDeploy = requestToDeploy;

        this.profileFragmentId = profileFragmentId;
        this.profile = profile;

        this.address = address;
        this.worker = worker;
        this.lastMissingHeartbeatTime = lastMissingHeartbeatTime;
    }

    public void deployAsync() {
        transitionState(State.DEPLOYING);

        TNetworkAddress brpcAddress = worker.getBrpcAddress();
        try {
            deployFuture = BackendServiceClient.getInstance().execPlanFragmentAsync(brpcAddress, requestToDeploy);
        } catch (RpcException | TException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            deployFuture = new Future<PExecPlanFragmentResult>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public PExecPlanFragmentResult get() {
                    PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                    StatusPB pStatus = new StatusPB();
                    pStatus.errorMsgs = Lists.newArrayList();
                    pStatus.errorMsgs.add(e.getMessage());
                    if (e instanceof RpcException) {
                        // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                        pStatus.statusCode = TStatusCode.THRIFT_RPC_ERROR.getValue();
                    } else {
                        pStatus.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                    }
                    result.status = pStatus;
                    return result;
                }

                @Override
                public PExecPlanFragmentResult get(long timeout, @NotNull TimeUnit unit) {
                    return get();
                }
            };
        }
    }

    public static class DeploymentResult {
        private final TStatusCode statusCode;
        private final String errMessage;
        private final Throwable failure;

        public DeploymentResult(TStatusCode statusCode, String errMessage, Throwable failure) {
            this.statusCode = statusCode;
            this.errMessage = errMessage;
            this.failure = failure;
        }

        public Status getStatus() {
            return new Status(statusCode, errMessage);
        }

        public TStatusCode getStatusCode() {
            return statusCode;
        }

        public Throwable getFailure() {
            return failure;
        }
    }

    public DeploymentResult waitForDeploymentCompletion(long deliveryTimeoutMs) {
        TStatusCode code;
        String errMsg = null;
        Throwable failure = null;
        try {
            PExecPlanFragmentResult result = deployFuture.get(deliveryTimeoutMs, TimeUnit.MILLISECONDS);
            code = TStatusCode.findByValue(result.status.statusCode);
            if (!CollectionUtils.isEmpty(result.status.errorMsgs)) {
                errMsg = result.status.errorMsgs.get(0);
            }
        } catch (ExecutionException e) {
            LOG.warn("catch a execute exception", e);
            code = TStatusCode.THRIFT_RPC_ERROR;
            failure = e;
        } catch (InterruptedException e) { // NOSONAR
            LOG.warn("catch a interrupt exception", e);
            code = TStatusCode.INTERNAL_ERROR;
            failure = e;
        } catch (TimeoutException e) {
            LOG.warn("catch a timeout exception", e);
            code = TStatusCode.TIMEOUT;
            errMsg = "deploy query timeout.";
            failure = e;
        }

        if (code == TStatusCode.OK) {
            transitionState(State.EXECUTING);
        } else {
            transitionState(State.FAILED);

            if (errMsg == null) {
                errMsg = "exec rpc error.";
            }
            errMsg += " " + String.format("backend [id=%d] [host=%s]", worker.getId(), address.getHostname());

            LOG.warn("exec plan fragment failed, errmsg={}, code={}, fragmentId={}, backend={}:{}",
                    errMsg, code, getFragmentId(), address.hostname, address.port);
        }

        requestToDeploy = null;
        deployFuture = null;
        return new DeploymentResult(code, errMsg, failure);
    }

    // update profile.
    // return true if profile is updated. Otherwise, return false.
    public synchronized boolean updateExecStatus(TReportExecStatusParams params) {
        switch (state) {
            case CREATED:
            case FINISHED: // duplicate packet
            case FAILED:
                return false;
            case DEPLOYING:
            case EXECUTING:
            case CANCELLING:
            default:
                if (params.isSetProfile()) {
                    profile.update(params.profile);
                }
                if (params.isDone()) {
                    if (params.getStatus() == null || params.getStatus().getStatus_code() == TStatusCode.OK) {
                        transitionState(State.FINISHED);
                    } else {
                        transitionState(State.FAILED);
                    }
                }
                return true;
        }
    }

    // cancel the fragment instance.
    // return true if cancel success. Otherwise, return false
    public synchronized boolean cancelFragmentInstance(PPlanFragmentCancelReason cancelReason) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "cancelRemoteFragments state={}  backend: {}, fragment instance id={}, reason: {}",
                    state, worker.getId(), DebugUtil.printId(fragmentInstanceId), cancelReason.name());
        }

        switch (state) {
            case CREATED:
            case CANCELLING:
            case FINISHED:
            case FAILED:
                return false;
            case DEPLOYING: // The cancelling request may arrive earlier than the deployed response.
            case EXECUTING:
            default:
                transitionState(State.CANCELLING);
        }

        TNetworkAddress brpcAddress = worker.getBrpcAddress();
        try {
            BackendServiceClient.getInstance().cancelPlanFragmentAsync(brpcAddress,
                    jobSpec.getQueryId(), fragmentInstanceId, cancelReason,
                    jobSpec.isEnablePipeline());
        } catch (RpcException e) {
            LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(), brpcAddress.getPort(), e);
            SimpleScheduler.addToBlacklist(worker.getId());
            return false;
        }

        return true;
    }

    public boolean hasBeenDeployed() {
        return state.hasBeenDeployed();
    }

    public boolean isFinished() {
        return state.isTerminal();
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public TUniqueId getFragmentInstanceId() {
        return fragmentInstanceId;
    }

    public Integer getIndexInJob() {
        return indexInJob;
    }

    public ComputeNode getWorker() {
        return worker;
    }

    public TNetworkAddress getAddress() {
        return address;
    }

    public int getProfileFragmentId() {
        return profileFragmentId;
    }

    public RuntimeProfile getProfile() {
        return profile;
    }

    public synchronized void printProfile(StringBuilder builder) {
        profile.computeTimeInProfile();
        profile.prettyPrint(builder, "");
    }

    public synchronized boolean computeTimeInProfile(int maxFragmentId) {
        if (this.profileFragmentId < 0 || this.profileFragmentId > maxFragmentId) {
            LOG.warn("profileFragmentId {} should be in [0, {})", profileFragmentId, maxFragmentId);
            return false;
        }
        profile.computeTimeInProfile();
        return true;
    }

    public boolean isBackendStateHealthy() {
        if (worker.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime) {
            LOG.warn("backend {} is down while joining the coordinator. job id: {}", worker.getId(),
                    jobSpec.getLoadJobId());
            return false;
        }
        return true;
    }

    public QueryStatisticsItem.FragmentInstanceInfo buildFragmentInstanceInfo() {
        return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                .instanceId(fragmentInstanceId)
                .fragmentId(String.valueOf(fragmentId))
                .address(address)
                .build();
    }

    private synchronized void transitionState(State to) {
        state = to;
    }

    public enum State {
        CREATED,
        DEPLOYING,
        EXECUTING,
        CANCELLING,

        FINISHED,
        FAILED;

        public boolean hasBeenDeployed() {
            return this != CREATED;
        }

        public boolean isTerminal() {
            return this == FINISHED || this == FAILED;
        }
    }
}
