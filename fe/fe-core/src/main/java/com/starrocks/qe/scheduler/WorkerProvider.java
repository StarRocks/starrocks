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

import com.starrocks.common.Reference;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;

import java.util.Collection;
import java.util.List;

public interface WorkerProvider {
    interface Factory {
        /**
         * Capture the available workers from {@code systemInfoService}, which are alive and not in the blacklist.
         *
         * @param systemInfoService   The service which provides all the backend nodes and compute nodes.
         * @param preferComputeNode   Whether to prefer using compute nodes over backend nodes.
         * @param numUsedComputeNodes The maximum number of used compute nodes.
         */
        WorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                               boolean preferComputeNode,
                                               int numUsedComputeNodes);
    }

    /**
     * Choose the specific backend node.
     *
     * @param backendID The ID of the backend node to choose.
     * @return The address with the {@code ComputeNode#bePort} of the backend node.
     * @throws SchedulerException if there is no available backend with {@code backendID}.
     */
    TNetworkAddress chooseBackend(Long backendID) throws SchedulerException;

    /**
     * Choose the next worker node.
     *
     * @param workerIdRef storing ID of the worker node to choose.
     * @return The address with the {@code ComputeNode#bePort} of the worker node.
     * @throws SchedulerException if there is no available backend with {@code backendID}.
     */
    TNetworkAddress chooseNextWorker(Reference<Long> workerIdRef) throws SchedulerException;

    /**
     * Choose all the available compute nodes.
     *
     * @return The address with the {@code ComputeNode#bePort} of the compute nodes to choose.
     */
    List<TNetworkAddress> chooseAllComputedNodes();

    Collection<ComputeNode> getWorkers();

    boolean isBackendAvailable(Long backendID);

    void reportBackendNotFoundException() throws SchedulerException;

    void reportWorkerNotFoundException() throws SchedulerException;

    void recordUsedWorker(Long workerID, TNetworkAddress beAddr);

    boolean isUsingWorker(Long workerID);

    ComputeNode getUsedWorkerByBeAddr(TNetworkAddress addr);

    TNetworkAddress getUsedHttpAddrByBeAddr(TNetworkAddress addr);

    List<Long> getUsedWorkerIDs();

    Collection<TNetworkAddress> getUsedWorkerBeAddrs();
}
