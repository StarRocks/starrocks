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

import com.starrocks.common.StarRocksException;

import java.time.Duration;
import java.util.Optional;

/**
 * Runtime collaborators the coordinator needs to actually run pre-split: a
 * sampler-and-planner (tier routing lives here — Tier 1 first when the sort
 * key has arity 1, Tier 2 on {@link Tier1UnavailableException}), the job
 * factory + admission step, and the post-submit FINISHED wait.
 *
 * <p>Bundled into one interface so the integrating load path (D1 / D2) ships
 * one implementation and tests can swap in a fake without static mocking.
 */
public interface PreSplitPipeline {

    /**
     * Pre-submit phase. Samples, plans the row-quantile boundaries, and builds
     * a {@code TabletReshardJob}. Bounded by {@code timeout}.
     *
     * @return {@link Optional#empty()} when the planner concluded no useful
     *         split exists (the orchestrator then returns
     *         {@link SkipReason#NO_USEFUL_CUTS}); otherwise a
     *         {@link PreparedReshardJob} ready to submit.
     * @throws PreSplitPreSubmitTimeoutException when the phase exceeds {@code timeout}.
     * @throws StarRocksException for any other sampling / planning / factory error;
     *         the orchestrator maps it to {@link SkipReason#SAMPLE_FAILED}.
     */
    Optional<PreparedReshardJob> preSubmit(SampleRequest request, int activeComputeNodeCount, Duration timeout)
            throws PreSplitPreSubmitTimeoutException, StarRocksException;

    /** Submit step. Hands the prepared job to {@code TabletReshardJobMgr.addTabletReshardJob}. */
    void submit(PreparedReshardJob preparedJob) throws StarRocksException;

    /**
     * Post-submit phase. Blocks until the submitted job reaches FINISHED.
     *
     * @throws PreSplitPostSubmitTimeoutException when the job does not finish within {@code timeout};
     *         the load executor aborts the transaction.
     */
    void awaitFinished(PreparedReshardJob preparedJob, Duration timeout)
            throws PreSplitPostSubmitTimeoutException, StarRocksException;

    /**
     * Opaque carrier returned by {@link #preSubmit} and consumed by
     * {@link #submit} / {@link #awaitFinished}. The production implementation
     * wraps a {@code TabletReshardJob}; tests can wrap any placeholder.
     */
    record PreparedReshardJob(Object payload) { }
}
