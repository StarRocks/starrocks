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
import com.starrocks.qe.SessionVariable;

/** Shared Sample-Based Tablet Pre-Split metric writers. */
final class PreSplitMetrics {

    private PreSplitMetrics() {
    }

    /**
     * If the session has opted out via {@code SET enable_tablet_pre_split=false},
     * record the {@link SkipReason#DISABLED_BY_SESSION} bucket and return
     * {@code true} so the caller short-circuits. Centralizes the bvar contract
     * so the two load-path hooks (which both want to skip early without
     * paying any resolution work) record one unified counter regardless of
     * which hook decided to skip.
     */
    static boolean shortCircuitOnSessionOptOut(SessionVariable sessionVariable) {
        if (sessionVariable.isEnableTabletPreSplit()) {
            return false;
        }
        recordEligibilitySkip(SkipReason.DISABLED_BY_SESSION);
        return true;
    }

    /**
     * Record an eligibility-skip under the {@link SkipReason}'s lower-cased
     * name. Called by the coordinator's eligibility branches and by the
     * load-path hooks that short-circuit ahead of the coordinator, so
     * operators observe one unified counter regardless of where the skip
     * was decided.
     */
    static void recordEligibilitySkip(SkipReason reason) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(reason.name().toLowerCase()).increase(1L);
        }
    }

    /**
     * Record a sampler-failure (sampler attempted but did not produce an
     * admitted reshard job) under the {@link SkipReason}'s lower-cased name.
     */
    static void recordSamplerFailed(SkipReason reason) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_FAILED
                    .getMetric(reason.name().toLowerCase()).increase(1L);
        }
    }
}
