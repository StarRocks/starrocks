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
import org.apache.thrift.TException;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

// record backend execute state
// TODO(zhaochun): add profile information and others
public class ExecutionFragmentInstance {
    TExecPlanFragmentParams commonRpcParams;
    TExecPlanFragmentParams uniqueRpcParams;
    PlanFragmentId fragmentId;
    boolean initiated;
    boolean done;
    boolean hasCanceled;
    int profileFragmentId;
    RuntimeProfile profile;
    TNetworkAddress address;
    final ComputeNode backend;
    long lastMissingHeartbeatTime = -1;

    // fake backendExecState, only user for stream load profile
    public ExecutionFragmentInstance(TUniqueId fragmentInstanceId, TNetworkAddress address) {
        String name = "Instance " + DebugUtil.printId(fragmentInstanceId);
        this.profile = new RuntimeProfile(name);
        this.profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
        this.backend = null;
    }

    public ExecutionFragmentInstance(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                     TExecPlanFragmentParams rpcParams,
                                     ComputeNode backend) {
        this(fragmentId, host, profileFragmentId, rpcParams, rpcParams, backend);
    }

    public ExecutionFragmentInstance(PlanFragmentId fragmentId, TNetworkAddress host, int profileFragmentId,
                                     TExecPlanFragmentParams commonRpcParams, TExecPlanFragmentParams uniqueRpcParams,
                                     ComputeNode backend) {
        this.profileFragmentId = profileFragmentId;
        this.fragmentId = fragmentId;
        this.commonRpcParams = commonRpcParams;
        this.uniqueRpcParams = uniqueRpcParams;
        this.initiated = false;
        this.done = false;
        this.address = host;
        this.backend = backend;
        String name =
                "Instance " + DebugUtil.printId(uniqueRpcParams.params.fragment_instance_id) + " (host=" + address +
                        ")";
        this.profile = new RuntimeProfile(name);
        this.profile.addInfoString("Address", String.format("%s:%s", address.hostname, address.port));
        this.hasCanceled = false;
        this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
    }

    public boolean isFinished() {
        return done;
    }

    public Integer getIndexInJob() {
        return uniqueRpcParams.getBackend_num();
    }

    public TUniqueId getInstanceId() {
        return uniqueRpcParams.getParams().getFragment_instance_id();
    }

    public TNetworkAddress getAddress() {
        return address;
    }

    // update profile.
    // return true if profile is updated. Otherwise, return false.
    public synchronized boolean updateProfile(TReportExecStatusParams params) {
        if (this.done) {
            // duplicate packet
            return false;
        }
        if (params.isSetProfile()) {
            profile.update(params.profile);
        }
        this.done = params.done;
        return true;
    }

    public synchronized void printProfile(StringBuilder builder) {
        this.profile.computeTimeInProfile();
        this.profile.prettyPrint(builder, "");
    }

    // cancel the fragment instance.
    // return true if cancel success. Otherwise, return false
    public synchronized boolean cancelFragmentInstance(PPlanFragmentCancelReason cancelReason) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "cancelRemoteFragments initiated={} done={} hasCanceled={} backend: {}, " +
                            "fragment instance id={}, reason: {}",
                    this.initiated, this.done, this.hasCanceled, backend.getId(),
                    DebugUtil.printId(fragmentInstanceId()), cancelReason.name());
        }
        try {
            if (!this.initiated) {
                return false;
            }
            // don't cancel if it is already finished
            if (this.done) {
                return false;
            }
            if (this.hasCanceled) {
                return false;
            }

            TNetworkAddress brpcAddress = backend.getBrpcAddress();

            try {
                BackendServiceClient.getInstance().cancelPlanFragmentAsync(brpcAddress,
                        jobSpec.getQueryId(), fragmentInstanceId(), cancelReason,
                        commonRpcParams.is_pipeline);
            } catch (RpcException e) {
                LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                        brpcAddress.getPort());
                SimpleScheduler.addToBlacklist(backend.getId());
            }

            this.hasCanceled = true;
        } catch (Exception e) {
            LOG.warn("catch a exception", e);
            return false;
        }
        return true;
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
        if (backend.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime) {
            LOG.warn("backend {} is down while joining the coordinator. job id: {}", backend.getId(),
                    jobSpec.getLoadJobId());
            return false;
        }
        return true;
    }

    public Future<PExecPlanFragmentResult> execRemoteFragmentAsync() throws TException {
        TNetworkAddress brpcAddress;
        try {
            brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        this.initiated = true;
        try {
            return BackendServiceClient.getInstance().execPlanFragmentAsync(brpcAddress, uniqueRpcParams);
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return new Future<PExecPlanFragmentResult>() {
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
                    // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                    pStatus.statusCode = TStatusCode.THRIFT_RPC_ERROR.getValue();
                    result.status = pStatus;
                    return result;
                }

                @Override
                public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                    return get();
                }
            };
        }
    }

    public QueryStatisticsItem.FragmentInstanceInfo buildFragmentInstanceInfo() {
        return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                .instanceId(fragmentInstanceId()).fragmentId(String.valueOf(fragmentId)).address(this.address)
                .build();
    }

    private TUniqueId fragmentInstanceId() {
        return this.uniqueRpcParams.params.getFragment_instance_id();
    }
}
