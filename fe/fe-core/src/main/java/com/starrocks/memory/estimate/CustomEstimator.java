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

package com.starrocks.memory.estimate;

/**
 * Interface for custom memory estimators.
 * Implement this interface to provide specialized memory estimation
 * for specific object types that require custom handling.
 */
@FunctionalInterface
public interface CustomEstimator {
    /**
     * Estimate the memory size of the given object.
     *
     * @param obj the object to estimate
     * @return the estimated memory size in bytes
     */
    long estimate(Object obj);
}
