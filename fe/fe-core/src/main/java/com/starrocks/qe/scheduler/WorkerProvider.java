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

import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;

import java.util.Collection;
import java.util.List;

/**
 * WorkerProvider provides available workers to a job scheduler, and records the selected workers.
 * - Each job has its own worker provider.
 * - It only checks whether each worker is available before construction at {@code Factory#captureAvailableWorkers}.
 * - There are two kinds of worker, data node (backend) and compute node. Some fragments can only select data node,
 * such as fragments with OlapTableNode.
 * - All the methods are thread safe.
 */
public interface WorkerProvider {
    interface Factory {
        /**
         * Capture the available workers from {@code systemInfoService}, which are alive and not in the blacklist.
         *
         * @param systemInfoService                   The service which provides all the backend nodes and compute nodes.
         * @param preferComputeNode                   Whether to prefer using compute nodes over backend nodes.
         * @param numUsedComputeNodes                 The maximum number of used compute nodes.
         * @param computationFragmentSchedulingPolicy The schedule policy of backend and compute nodes.
         */
        WorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                               boolean preferComputeNode,
                                               int numUsedComputeNodes,
                                               ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                                               long warehouseId);
    }

    /**
     * Select the next worker node.
     *
     * @return The id of the worker node to choose.
     * @throws NonRecoverableException if there is no available worker.
     */
    long selectNextWorker() throws NonRecoverableException;

    /**
     * Select the worker with the given id.
     *
     * @param workerId The id of the worker to choose.
     * @throws NonRecoverableException if there is no available worker with the given id.
     */
    void selectWorker(long workerId) throws NonRecoverableException;

    /**
     * Select all the available compute nodes.
     *
     * @return The id of the compute nodes to choose.
     */
    List<Long> selectAllComputeNodes();

    Collection<ComputeNode> getAllWorkers();

    ComputeNode getWorkerById(long workerId);

    boolean isDataNodeAvailable(long dataNodeId);

    void reportDataNodeNotFoundException() throws NonRecoverableException;

    void reportWorkerNotFoundException() throws NonRecoverableException;

    boolean isWorkerSelected(long workerId);

    List<Long> getSelectedWorkerIds();

    List<Long> getAllAvailableNodes();

    void selectWorkerUnchecked(long workerId);

    default boolean isPreferComputeNode() {
        return false;
    }

    default boolean allowUsingBackupNode() {
        return false;
    }

    /**
     * choose a backup worker for the given workerId, it is up to the WorkerProvider decision how to select it.
     *
     * @return -1, no available backup worker
     */
    long selectBackupWorker(long workerId);
}
