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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;

public interface SlotEstimator {
    /**
     * Estimate the number of slots needed for the given query.
     * <p>The returned value is the slot count after all clamps (in particular the {@code totalSlots} cap),
     * suitable for use as the admission decision.
     */
    int estimateSlots(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord);

    /**
     * Estimate both the raw (un-clamped) and clamped slot counts for the given query.
     * <p>The clamped value is what {@link #estimateSlots} returns; the raw value preserves the pre-clamp
     * demand and is intended for signals such as "this query wanted N slots but the warehouse only has M".
     * <p>The default implementation returns {@code (clamped, clamped)} so estimators that do not have a
     * meaningful raw value remain backwards-compatible.
     */
    default SlotEstimate estimateBoth(QueryQueueOptions opts, ConnectContext context, DefaultCoordinator coord) {
        int clamped = estimateSlots(opts, context, coord);
        return new SlotEstimate(clamped, clamped);
    }

    /**
     * The pre-clamp ({@code rawSlots}) and post-clamp ({@code clampedSlots}) slot counts for a query.
     * <p>{@code rawSlots} must be greater than or equal to {@code clampedSlots}; the constructor enforces
     * this invariant.
     */
    record SlotEstimate(int rawSlots, int clampedSlots) {
        public SlotEstimate {
            if (rawSlots < clampedSlots) {
                throw new IllegalArgumentException(
                        "rawSlots (" + rawSlots + ") must be >= clampedSlots (" + clampedSlots + ")");
            }
        }
    }
}
