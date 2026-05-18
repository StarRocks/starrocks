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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.metric.MetricRepo;

/**
 * Centralized increment helpers for Sample-Based Tablet Pre-Split metrics.
 * Lives outside {@link TabletPreSplitCoordinator} so the load-path hooks can
 * record metrics on an early short-circuit without {@code MockedStatic} test
 * stubs (that target the coordinator's submit surface) silently swallowing
 * the increment.
 */
final class PreSplitMetrics {

    private PreSplitMetrics() {
    }

    /**
     * Increment the eligibility-skipped counter under the {@link SkipReason}'s
     * lower-cased name as the {@code reason} label. Called both by the
     * coordinator's eligibility branches and by the load-path hooks when they
     * short-circuit ahead of the coordinator, so operators observe a single
     * unified counter regardless of where the skip was decided.
     */
    static void recordEligibilitySkip(SkipReason reason) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(reason.name().toLowerCase()).increase(1L);
        }
    }
}
