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

    /**
     * Record one predicted partition dropped because the grouper's per-load
     * cap ({@code tablet_pre_split_max_partitions_per_load}) was hit.
     */
    static void recordPartitionCapped() {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_CAPPED.increase(1L);
        }
    }

    /**
     * Record that the multi-partition coordinator counted one
     * {@link PartitionSamples} entry as part of a combined-submit pass.
     * Incremented once per entry regardless of how the entry is later
     * resolved (submitted into the combined job, dropped as PARTITION_NOT_*,
     * etc.); the resolution outcomes have their own counters via
     * {@link #recordEligibilitySkip} / {@link #recordSamplerFailed}.
     */
    static void recordPartitionCounted() {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_PARTITIONS_TOTAL.increase(1L);
        }
    }

    /**
     * Outcomes of one pre-create attempt by the multi-partition coordinator.
     * {@link #ALREADY_EXISTS} means the partition raced into the catalog
     * between the grouper's snapshot and the coordinator's pre-create call;
     * {@link #SUCCEEDED} means {@code LocalMetastore.addPartitions} returned
     * normally (whether it actually created the partition or silently
     * deduped); {@link #FAILED} means {@code addPartitions} threw.
     */
    enum PreCreateResult { SUCCEEDED, FAILED, ALREADY_EXISTS }

    /** Record one multi-partition pre-create attempt under its result bucket. */
    static void recordPreCreate(PreCreateResult result) {
        if (!MetricRepo.hasInit) {
            return;
        }
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_PRE_CREATE
                .getMetric(result.name().toLowerCase()).increase(1L);
    }
}
