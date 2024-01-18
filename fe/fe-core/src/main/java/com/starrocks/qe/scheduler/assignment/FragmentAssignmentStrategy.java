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

import com.starrocks.common.exception.UserException;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;

/**
 * The strategy to instantiates multiple parallel instances of each fragment and assign them to appropriate workers.
 * <p> The concrete strategy will use {@link ExecutionFragment#addInstance(FragmentInstance)} to add an instance.
 * // fill hosts field in fragmentExecParams
 * <p>
 * // For each fragment in fragments, computes hosts on which to run the instances
 * // and stores result in fragmentExecParams.hosts.
 * <p>
 * // compute hosts of producer fragment before those of consumer fragment(s),
 * // the latter might inherit the set of hosts from the former
 * // compute hosts *bottom up*.
 */
public interface FragmentAssignmentStrategy {
    /**
     * The strategy to instantiates multiple parallel instances of each fragment and assign them to appropriate workers.
     *
     * @param executionFragment the fragment need to be assigned.
     * @throws UserException if there is no available worker.
     */
    void assignFragmentToWorker(ExecutionFragment executionFragment) throws UserException;
}
