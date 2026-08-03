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

import com.starrocks.alter.reshard.TabletReshardJob;

import java.util.List;

/**
 * Outcome of {@link TabletPreSplitCoordinator}'s entry points. Sealed so
 * callers cannot silently miss a new case when later stages add planning /
 * submission outcomes.
 */
public sealed interface PreSplitOutcome
        permits PreSplitOutcome.Skipped, PreSplitOutcome.Eligible,
                PreSplitOutcome.Submitted, PreSplitOutcome.SubmittedCombined,
                PreSplitOutcome.Finished {

    /** Eligibility check failed or a recoverable pipeline failure occurred; load proceeds against the original tablet. */
    record Skipped(SkipReason reason) implements PreSplitOutcome { }

    /** All eligibility checks passed. Returned by the eligibility-only entry point. */
    record Eligible() implements PreSplitOutcome { }

    /**
     * The reshard job was admitted to {@code TabletReshardJobMgr} and is
     * running asynchronously. The reshard daemon will drive it through to
     * FINISHED at its own pace; the load that submitted it does not wait.
     *
     * <p>May carry a {@code null} {@code preparedJob} when emitted as a
     * per-partition sentinel inside {@link SubmittedCombined#perPartitionResults}:
     * the per-partition entry fed into the combined job, but the combined
     * {@link TabletReshardJob} is at the outer level rather than carried per entry.
     */
    record Submitted(PreSplitPipeline.PreparedReshardJob preparedJob) implements PreSplitOutcome { }

    /**
     * Multi-partition variant: ONE combined {@link TabletReshardJob} carries
     * splittingTablets across multiple input partitions. {@code perPartitionResults}
     * is parallel-by-input bookkeeping for metrics — each entry is either
     * {@link Skipped} (entry dropped before being fed to the combined job) or
     * {@link Submitted} with a {@code null} {@code preparedJob} (entry fed
     * into the combined job; the combined job is at the outer level).
     */
    record SubmittedCombined(TabletReshardJob combinedJob,
                             List<PreSplitOutcome> perPartitionResults) implements PreSplitOutcome { }

    /** Pre-split completed: the reshard job was submitted and reached the FINISHED state. */
    record Finished() implements PreSplitOutcome { }
}
