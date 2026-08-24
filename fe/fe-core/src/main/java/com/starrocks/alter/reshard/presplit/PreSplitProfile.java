// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter.reshard.presplit;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TUnit;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Per-load observability for Sample-Based Tablet Pre-Split.
 *
 * <p>The pre-split work runs before the load coordinator exists, so it cannot naturally report
 * through a BE fragment profile. INSERT keeps this collector on the outer load's
 * {@link ConnectContext}; Broker Load instead keeps a job-owned
 * {@code BrokerLoadJob.preSplitProfile} and threads it through asynchronous task creation. The
 * collector accumulates the FE-side phases and is snapshotted as a top-level {@code PreSplit}
 * profile node when the normal load profile is built. The internal sample query temporarily
 * installs its own context; callers therefore keep the timer's collector reference in the returned
 * {@link Scope} instead of looking up the thread-local context again when the scope closes.
 */
public final class PreSplitProfile {

    public static final String PROFILE_NAME = "PreSplit";
    public static final String SOURCE_SAMPLING_TIME = "SourceSamplingTime";
    public static final String PARTITION_AND_BOUNDARY_PLANNING_TIME =
            "PartitionAndBoundaryPlanningTime";
    public static final String JOB_SUBMISSION_TIME = "JobSubmissionTime";
    public static final String RESHARD_WAIT_TIME = "ReshardWaitTime";
    public static final String ATTEMPTS = "Attempts";
    public static final String SAMPLE_ROWS = "SampleRows";
    public static final String ESTIMATED_INPUT_BYTES = "EstimatedInputBytes";
    public static final String TARGET_PARTITIONS = "TargetPartitions";
    public static final String BOUNDARIES_PLANNED = "BoundariesPlanned";

    private static final int MAX_INFO_VALUES = 20;
    private static final Scope NOOP_SCOPE = () -> { };
    private static final ThreadLocal<PreSplitProfile> ACTIVE_PROFILE = new ThreadLocal<>();
    private static final ThreadLocal<AttemptState> ACTIVE_ATTEMPT = new ThreadLocal<>();

    enum Phase {
        SOURCE_SAMPLING,
        PARTITION_AND_BOUNDARY_PLANNING,
        JOB_SUBMISSION,
        RESHARD_WAIT
    }

    /** Auto-closeable timer without a checked exception at call sites. */
    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final class AttemptState {
        private final PreSplitProfile profile;
        private final AtomicLong estimatedInputBytes = new AtomicLong();

        private AttemptState(PreSplitProfile profile) {
            this.profile = profile;
        }
    }

    private final LongSupplier nanoTime;
    private final LongAdder totalTimeNs = new LongAdder();
    private final LongAdder sourceSamplingTimeNs = new LongAdder();
    private final LongAdder partitionAndBoundaryPlanningTimeNs = new LongAdder();
    private final LongAdder jobSubmissionTimeNs = new LongAdder();
    private final LongAdder reshardWaitTimeNs = new LongAdder();
    private final LongAdder attempts = new LongAdder();
    private final LongAdder sampleRows = new LongAdder();
    private final AtomicLong estimatedInputBytes = new AtomicLong();
    private final LongAdder targetPartitions = new LongAdder();
    private final LongAdder boundariesPlanned = new LongAdder();

    private final Set<String> loadKinds = new LinkedHashSet<>();
    private final Set<String> tables = new LinkedHashSet<>();
    private final Set<String> sourceTiers = new LinkedHashSet<>();
    private final Set<String> outcomes = new LinkedHashSet<>();
    private final Set<String> sampleQueryIds = new LinkedHashSet<>();
    private final Set<String> reshardJobIds = new LinkedHashSet<>();

    public PreSplitProfile() {
        this(System::nanoTime);
    }

    @VisibleForTesting
    PreSplitProfile(LongSupplier nanoTime) {
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** Starts one actual pre-split attempt and creates the per-load collector lazily. */
    public static Scope startAttempt(ConnectContext context, LoadKind loadKind) {
        if (context == null) {
            return NOOP_SCOPE;
        }
        return startAttempt(context.getOrCreatePreSplitProfile(), loadKind);
    }

    public static Scope startAttempt(PreSplitProfile profile, LoadKind loadKind) {
        if (profile == null) {
            return NOOP_SCOPE;
        }
        profile.attempts.increment();
        profile.addInfoValue(profile.loadKinds, loadKind.displayName());
        PreSplitProfile previous = ACTIVE_PROFILE.get();
        AttemptState previousAttempt = ACTIVE_ATTEMPT.get();
        AttemptState attempt = new AttemptState(profile);
        ACTIVE_PROFILE.set(profile);
        ACTIVE_ATTEMPT.set(attempt);
        Scope timer = profile.startTimer(profile.totalTimeNs);
        return () -> {
            try {
                timer.close();
                // A single table attempt may sample the same source once per materialized index.
                // Deduplicate those estimates within the attempt, then add distinct table attempts
                // to the load-level total.
                profile.estimatedInputBytes.addAndGet(attempt.estimatedInputBytes.get());
            } finally {
                if (previous == null) {
                    ACTIVE_PROFILE.remove();
                } else {
                    ACTIVE_PROFILE.set(previous);
                }
                if (previousAttempt == null) {
                    ACTIVE_ATTEMPT.remove();
                } else {
                    ACTIVE_ATTEMPT.set(previousAttempt);
                }
            }
        };
    }

    static Scope startPhase(Phase phase) {
        PreSplitProfile profile = findCurrentProfile();
        if (profile == null) {
            return NOOP_SCOPE;
        }
        return switch (phase) {
            case SOURCE_SAMPLING -> profile.startTimer(profile.sourceSamplingTimeNs);
            case PARTITION_AND_BOUNDARY_PLANNING ->
                    profile.startTimer(profile.partitionAndBoundaryPlanningTimeNs);
            case JOB_SUBMISSION -> profile.startTimer(profile.jobSubmissionTimeNs);
            case RESHARD_WAIT -> profile.startTimer(profile.reshardWaitTimeNs);
        };
    }

    private Scope startTimer(LongAdder destination) {
        long startNs = nanoTime.getAsLong();
        return () -> destination.add(Math.max(0L, nanoTime.getAsLong() - startNs));
    }

    static void recordTable(String tableName) {
        currentProfile(profile -> profile.addInfoValue(profile.tables, tableName));
    }

    static void recordSourceTier(String sourceTier) {
        currentProfile(profile -> profile.addInfoValue(profile.sourceTiers, sourceTier));
    }

    static void recordOutcome(PreSplitOutcome outcome) {
        if (outcome instanceof PreSplitOutcome.Skipped skipped) {
            recordOutcome("SKIPPED: " + skipped.reason());
        } else if (outcome instanceof PreSplitOutcome.SubmittedCombined) {
            recordOutcome("SUBMITTED_COMBINED");
        } else if (outcome instanceof PreSplitOutcome.Submitted) {
            recordOutcome("SUBMITTED");
        } else if (outcome instanceof PreSplitOutcome.Finished) {
            recordOutcome("FINISHED");
        } else if (outcome != null) {
            recordOutcome(outcome.getClass().getSimpleName());
        }
    }

    static void recordOutcome(String outcome) {
        currentProfile(profile -> profile.addInfoValue(profile.outcomes, outcome));
    }

    static void recordOutcome(PreSplitProfile profile, String outcome) {
        if (profile != null) {
            profile.addInfoValue(profile.outcomes, outcome);
        } else {
            recordOutcome(outcome);
        }
    }

    static void recordSample(SampleSet sampleSet) {
        if (sampleSet == null) {
            return;
        }
        currentProfile(profile -> profile.sampleRows.add(sampleSet.getTuples().size()));
        if (sampleSet.getEstimates() != null) {
            recordEstimatedInputBytes(sampleSet.getEstimates().totalBytes());
        }
    }

    /** Records an estimate even when the selected source tier does not materialize sampled rows. */
    static void recordEstimatedInputBytes(long estimatedBytes) {
        currentProfile(profile -> {
            long nonNegativeBytes = Math.max(0L, estimatedBytes);
            AttemptState attempt = ACTIVE_ATTEMPT.get();
            if (attempt != null && attempt.profile == profile) {
                attempt.estimatedInputBytes.accumulateAndGet(nonNegativeBytes, Math::max);
            } else {
                // Preserve best-effort diagnostics for package-local callers without an attempt scope.
                profile.estimatedInputBytes.accumulateAndGet(nonNegativeBytes, Math::max);
            }
        });
    }

    static void recordTargetPartitions(long count) {
        currentProfile(profile -> profile.targetPartitions.add(Math.max(0L, count)));
    }

    static void recordBoundariesPlanned(long count) {
        currentProfile(profile -> profile.boundariesPlanned.add(Math.max(0L, count)));
    }

    static void recordSampleQueryId(ConnectContext outerContext, UUID queryId) {
        if (queryId == null) {
            return;
        }
        PreSplitProfile profile = ACTIVE_PROFILE.get();
        if (profile == null && outerContext != null) {
            profile = outerContext.getPreSplitProfile();
        }
        if (profile != null) {
            profile.addInfoValue(profile.sampleQueryIds, DebugUtil.printId(queryId));
        }
    }

    static void recordReshardJobId(long jobId) {
        if (jobId > 0) {
            currentProfile(profile -> profile.addInfoValue(profile.reshardJobIds, Long.toString(jobId)));
        }
    }

    private static void currentProfile(java.util.function.Consumer<PreSplitProfile> consumer) {
        PreSplitProfile profile = findCurrentProfile();
        if (profile != null) {
            consumer.accept(profile);
        }
    }

    private static PreSplitProfile findCurrentProfile() {
        PreSplitProfile profile = ACTIVE_PROFILE.get();
        if (profile != null) {
            return profile;
        }
        ConnectContext context = ConnectContext.get();
        return context == null ? null : context.getPreSplitProfile();
    }

    private synchronized void addInfoValue(Set<String> values, String value) {
        if (value != null && !value.isEmpty() && values.size() < MAX_INFO_VALUES) {
            values.add(value);
        }
    }

    private synchronized String joinInfoValues(Set<String> values) {
        return String.join(", ", values);
    }

    /** Builds an immutable-in-practice snapshot for attachment to a top-level load profile. */
    public RuntimeProfile toRuntimeProfile() {
        RuntimeProfile profile = new RuntimeProfile(PROFILE_NAME);
        profile.getCounterTotalTime().setValue(totalTimeNs.sum());
        addTimeCounter(profile, SOURCE_SAMPLING_TIME, sourceSamplingTimeNs.sum());
        addTimeCounter(profile, PARTITION_AND_BOUNDARY_PLANNING_TIME,
                partitionAndBoundaryPlanningTimeNs.sum());
        addTimeCounter(profile, JOB_SUBMISSION_TIME, jobSubmissionTimeNs.sum());
        addTimeCounter(profile, RESHARD_WAIT_TIME, reshardWaitTimeNs.sum());
        addUnitCounter(profile, ATTEMPTS, attempts.sum());
        addUnitCounter(profile, SAMPLE_ROWS, sampleRows.sum());
        profile.addCounter(ESTIMATED_INPUT_BYTES, TUnit.BYTES, null)
                .setValue(estimatedInputBytes.get());
        addUnitCounter(profile, TARGET_PARTITIONS, targetPartitions.sum());
        addUnitCounter(profile, BOUNDARIES_PLANNED, boundariesPlanned.sum());

        addInfoString(profile, "LoadKinds", loadKinds);
        addInfoString(profile, "Tables", tables);
        addInfoString(profile, "SourceTiers", sourceTiers);
        addInfoString(profile, "Outcomes", outcomes);
        addInfoString(profile, "SampleQueryIds", sampleQueryIds);
        addInfoString(profile, "ReshardJobIds", reshardJobIds);
        return profile;
    }

    /** Adds the node only for a statement/load that actually attempted pre-split. */
    public static void appendTo(RuntimeProfile root, ConnectContext context) {
        if (root == null || context == null) {
            return;
        }
        appendTo(root, context.getPreSplitProfile());
    }

    public static void appendTo(RuntimeProfile root, PreSplitProfile profile) {
        if (root == null) {
            return;
        }
        if (profile != null && profile.attempts.sum() > 0) {
            root.addChild(profile.toRuntimeProfile());
        }
    }

    private static void addTimeCounter(RuntimeProfile profile, String name, long value) {
        profile.addCounter(name, TUnit.TIME_NS, null).setValue(value);
    }

    private static void addUnitCounter(RuntimeProfile profile, String name, long value) {
        profile.addCounter(name, TUnit.UNIT, null).setValue(value);
    }

    private synchronized void addInfoString(RuntimeProfile profile, String name, Set<String> values) {
        if (!values.isEmpty()) {
            profile.addInfoString(name, joinInfoValues(values));
        }
    }
}
