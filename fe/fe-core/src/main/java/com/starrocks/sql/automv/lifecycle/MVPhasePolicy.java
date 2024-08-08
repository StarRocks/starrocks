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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MVPhasePolicy {
    private static final Map<MVPhaseTransfer, TransferGuard> TRANSFER_GUARDS =
            ImmutableMap.<MVPhaseTransfer, TransferGuard>builder()
                    .put(MVPhaseTransfer.MPT_CRADLE_TO_INTERN, TransferGuards.CRADLE_TO_INTERN_GUARD)
                    .put(MVPhaseTransfer.MPT_CRADLE_TO_GRAVE, TransferGuards.GRADLE_TO_GRAVE_GUARD)
                    .put(MVPhaseTransfer.MPT_CRADLE_TO_EXTINCTION, TransferGuards.CRADLE_TO_EXTINCTION_GUARD)
                    .put(MVPhaseTransfer.MPT_INTERN_TO_TENURED, TransferGuards.INTERN_TO_TENURED_GUARD)
                    .put(MVPhaseTransfer.MPT_INTERN_TO_RETIRED, TransferGuards.INTERN_TO_RETIRED_GUARD)
                    .put(MVPhaseTransfer.MPT_INTERN_TO_GRAVE, TransferGuards.INTERN_TO_GRAVE_GUARD)
                    .put(MVPhaseTransfer.MPT_TENURED_TO_RETIRED, TransferGuards.TENURED_TO_RETIRED_GUARD)
                    .put(MVPhaseTransfer.MPT_TENURED_TO_GRAVE, TransferGuards.TENURED_TO_GRAVE_GUARD)
                    .put(MVPhaseTransfer.MPT_RETIRED_TO_INTERN, TransferGuards.RETIRED_TO_INTERN_GUARD)
                    .put(MVPhaseTransfer.MPT_RETIRED_TO_GRAVE, TransferGuards.RETIRED_TO_GRAVE_GUARD)
                    .put(MVPhaseTransfer.MPT_GRAVE_TO_CRADLE, TransferGuards.GRAVE_TO_CRADLE_GUARD)
                    .put(MVPhaseTransfer.MPT_GRAVE_TO_EXTINCTION, TransferGuards.GRAVE_TO_EXTINCTION_GUARD)
                    .build();
    private static final Map<MVPhase, Map<MVPhaseTransfer, TransferGuard>> PHASE_TO_TRANSFER_GUARDS = TRANSFER_GUARDS
            .entrySet()
            .stream()
            .collect(Collectors.groupingBy(e -> e.getKey().getFromPhase()))
            .entrySet()
            .stream()
            .collect(ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    e -> e.getValue()
                            .stream()
                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))));

    private static final Map<MVPhase, PostTransferAction> POST_TRANSFER_ACTIONS =
            ImmutableMap.<MVPhase, PostTransferAction>builder()
                    .put(MVPhase.MP_CRADLE, PostTransferActions.CRADLE_POST_ACTION)
                    .put(MVPhase.MP_INTERN, PostTransferActions.INTERN_POST_ACTION)
                    .put(MVPhase.MP_TENURED, PostTransferActions.TENURED_POST_ACTION)
                    .put(MVPhase.MP_RETIRED, PostTransferActions.RETIRED_POST_ACTION)
                    .put(MVPhase.MP_GRAVE, PostTransferActions.GRAVE_POST_ACTION)
                    .put(MVPhase.MP_EXTINCTION, PostTransferActions.EXTINCTION_POST_ACTION)
                    .build();
    private final Predicate<MVLifecycle> infantAbortionDictator;
    private final Predicate<MVLifecycle> initialRefreshFailureDictator;
    private final Predicate<MVLifecycle> internshipPeriodEndedDictator;
    private final BiFunction<MVLifecycle, Function<String, Double>, Boolean> goodMVDictator;
    private final BiFunction<MVLifecycle, Function<String, Double>, Boolean> badMVDictator;
    private final Predicate<MVLifecycle> reachPerformanceEvaluationTimeDictator;
    private final Predicate<MVLifecycle> exceedMaximumReviveWaitingTimeDictator;
    private final Predicate<MVLifecycle> exceedExtinctionRetentionMaxTimeDictator;
    private final Function<String, Double> mvHitRatioProvider;

    public MVPhasePolicy(Predicate<MVLifecycle> infantAbortionDictator,
                         Predicate<MVLifecycle> initialRefreshFailureDictator,
                         Predicate<MVLifecycle> internshipPeriodEndedDictator,
                         BiFunction<MVLifecycle, Function<String, Double>, Boolean> goodMVDictator,
                         BiFunction<MVLifecycle, Function<String, Double>, Boolean> badMVDictator,
                         Predicate<MVLifecycle> reachPerformanceEvaluationTimeDictator,
                         Predicate<MVLifecycle> exceedMaximumReviveWaitingTimeDictator,
                         Predicate<MVLifecycle> exceedExtinctionRetentionMaxTimeDictator,
                         Function<String, Double> mvHitRatioProvider) {
        this.infantAbortionDictator = Objects.requireNonNull(infantAbortionDictator);
        this.initialRefreshFailureDictator = Objects.requireNonNull(initialRefreshFailureDictator);
        this.internshipPeriodEndedDictator = Objects.requireNonNull(internshipPeriodEndedDictator);
        this.goodMVDictator = Objects.requireNonNull(goodMVDictator);
        this.badMVDictator = Objects.requireNonNull(badMVDictator);
        this.reachPerformanceEvaluationTimeDictator = reachPerformanceEvaluationTimeDictator;
        this.exceedMaximumReviveWaitingTimeDictator = Objects.requireNonNull(exceedMaximumReviveWaitingTimeDictator);
        this.exceedExtinctionRetentionMaxTimeDictator =
                Objects.requireNonNull(exceedExtinctionRetentionMaxTimeDictator);
        this.mvHitRatioProvider = Objects.requireNonNull(mvHitRatioProvider);
    }

    private static BiFunction<MVLifecycle, Function<String, Double>, Boolean> getMVEfficiencyDictator(
            Predicate<Double> ratioPredicate) {
        return (mvLifecycle, mvHitProvider) -> Optional.ofNullable(
                        mvHitProvider.apply(mvLifecycle.getMVName().toString()))
                .map(ratioPredicate::test)
                .orElse(false);
    }

    private static BiFunction<MVLifecycle, Function<String, Double>, Boolean> getMVPerformanceDictator(
            Predicate<MVLifecycle> reachPerformanceEvaluationTimeDictator,
            BiFunction<MVLifecycle, Function<String, Double>, Boolean> efficiencyDictator) {
        return (mvLifecycle, mvHitRatioProvider) -> {
            if (reachPerformanceEvaluationTimeDictator.test(mvLifecycle)) {
                return efficiencyDictator.apply(mvLifecycle, mvHitRatioProvider);
            } else {
                return false;
            }
        };
    }

    public static Builder newBuilder() {
        LifecycleOptions options = LifecycleOptions.getInstance();
        BiFunction<MVLifecycle, Function<String, Double>, Boolean> goodMVDictator =
                getMVEfficiencyDictator(ratio -> ratio >= options.getHitRatioHwm());
        BiFunction<MVLifecycle, Function<String, Double>, Boolean> badMVDictator =
                getMVEfficiencyDictator(ratio -> ratio < options.getHitRatioLwm());

        Predicate<MVLifecycle> reachPerfEvalTimeDictator = mvLifecycle -> mvLifecycle.elapsedSeconds() >
                options.getPerformanceEvaluationInterval();
        BiFunction<MVLifecycle, Function<String, Double>, Boolean> excellentPerfDictator =
                getMVPerformanceDictator(reachPerfEvalTimeDictator, goodMVDictator);
        BiFunction<MVLifecycle, Function<String, Double>, Boolean> unsatisfactoryPerfDictator =
                getMVPerformanceDictator(reachPerfEvalTimeDictator, badMVDictator);
        return new Builder()
                .setInfantAbortionDictator(mvLifecycle ->
                        mvLifecycle.elapsedSeconds() > options.getInfantAbortionMaxTime())
                .setInitialRefreshFailureDictator(mvLifecycle ->
                        mvLifecycle.elapsedSeconds() > options.getInitialRefreshMaxTime())
                .setInternshipPeriodEndedDictator(mvLifecycle ->
                        mvLifecycle.elapsedSeconds() > options.getInternshipPeriod())
                .setExceedMaximumReviveWaitingTimeDictator(mvLifecycle ->
                        mvLifecycle.elapsedSeconds() > options.getReviveWaitingMaxTime())
                .setExceedExtinctionRetentionMaxTimeDictator(mvLifecycle ->
                        mvLifecycle.getPhase() == MVPhase.MP_EXTINCTION &&
                                mvLifecycle.elapsedSeconds() > options.getExtinctionRetentionMaxTime())
                .setGoodMVDictator(goodMVDictator)
                .setBadMVDictator(badMVDictator)
                .setReachPerformanceEvaluationTimeDictator(reachPerfEvalTimeDictator);
    }

    public Function<String, Double> getMVHitRatioProvider() {
        return mvHitRatioProvider;
    }

    public Predicate<MVLifecycle> getReachPerformanceEvaluationTimeDictator() {
        return reachPerformanceEvaluationTimeDictator;
    }

    public Predicate<MVLifecycle> getExceedExtinctionRetentionMaxTimeDictator() {
        return exceedExtinctionRetentionMaxTimeDictator;
    }

    public BiFunction<MVLifecycle, Function<String, Double>, Boolean> getUnsatisfactoryPerformanceDictator() {
        return getMVPerformanceDictator(getReachPerformanceEvaluationTimeDictator(), getBadMVDictator());
    }

    public Predicate<MVLifecycle> getExceedMaximumReviveWaitingTimeDictator() {
        return exceedMaximumReviveWaitingTimeDictator;
    }

    public BiFunction<MVLifecycle, Function<String, Double>, Boolean> getExcellentPerformanceDictator() {
        return getMVPerformanceDictator(getReachPerformanceEvaluationTimeDictator(), getGoodMVDictator());
    }

    public Predicate<MVLifecycle> getInternshipPeriodEndedDictator() {
        return internshipPeriodEndedDictator;
    }

    public Predicate<MVLifecycle> getInfantAbortionDictator() {
        return infantAbortionDictator;
    }

    public Predicate<MVLifecycle> getInitialRefreshFailureDictator() {
        return initialRefreshFailureDictator;
    }

    public BiFunction<MVLifecycle, Function<String, Double>, Boolean> getGoodMVDictator() {
        return goodMVDictator;
    }

    public BiFunction<MVLifecycle, Function<String, Double>, Boolean> getBadMVDictator() {
        return badMVDictator;
    }

    void transfer(MVLifecycle mvLifecycle) {
        Map<MVPhaseTransfer, TransferGuard> guards =
                PHASE_TO_TRANSFER_GUARDS.getOrDefault(mvLifecycle.getPhase(), Collections.emptyMap());
        for (Map.Entry<MVPhaseTransfer, TransferGuard> guard : guards.entrySet()) {
            MVPhaseTransfer transfer = guard.getKey();
            TransferGuard transferGuard = guard.getValue();
            if (transferGuard.check(mvLifecycle, transfer, this)) {
                mvLifecycle.commit(transfer.getToPhase());
                break;
            }
        }
        POST_TRANSFER_ACTIONS.get(mvLifecycle.getPhase()).apply(mvLifecycle, this);
    }

    public static final class Builder {
        private Predicate<MVLifecycle> infantAbortionDictator;
        private Predicate<MVLifecycle> initialRefreshFailureDictator;
        private Predicate<MVLifecycle> internshipPeriodEndedDictator;
        private BiFunction<MVLifecycle, Function<String, Double>, Boolean> goodMVDictator;
        private BiFunction<MVLifecycle, Function<String, Double>, Boolean> badMVDictator;
        private Predicate<MVLifecycle> reachPerformanceEvaluationTimeDictator;
        private Predicate<MVLifecycle> exceedMaximumReviveWaitingTimeDictator;
        private Predicate<MVLifecycle> exceedExtinctionRetentionMaxTimeDictator;
        private Function<String, Double> mvHitRatioProvider;

        private Builder() {
        }

        public Builder setReachPerformanceEvaluationTimeDictator(
                Predicate<MVLifecycle> reachPerformanceEvaluationTimeDictator) {
            this.reachPerformanceEvaluationTimeDictator = reachPerformanceEvaluationTimeDictator;
            return this;
        }

        public Builder setExceedExtinctionRetentionMaxTimeDictator(
                Predicate<MVLifecycle> exceedExtinctionRetentionMaxTimeDictator) {
            this.exceedExtinctionRetentionMaxTimeDictator = exceedExtinctionRetentionMaxTimeDictator;
            return this;
        }

        public Builder setInfantAbortionDictator(
                Predicate<MVLifecycle> infantAbortionDictator) {
            this.infantAbortionDictator = infantAbortionDictator;
            return this;
        }

        public Builder setInitialRefreshFailureDictator(
                Predicate<MVLifecycle> initialRefreshFailureDictator) {
            this.initialRefreshFailureDictator = initialRefreshFailureDictator;
            return this;
        }

        public Builder setInternshipPeriodEndedDictator(
                Predicate<MVLifecycle> internshipPeriodEndedDictator) {
            this.internshipPeriodEndedDictator = internshipPeriodEndedDictator;
            return this;
        }

        public Builder setGoodMVDictator(BiFunction<MVLifecycle, Function<String, Double>, Boolean> goodMVDictator) {
            this.goodMVDictator = goodMVDictator;
            return this;
        }

        public Builder setBadMVDictator(BiFunction<MVLifecycle, Function<String, Double>, Boolean> badMVDictator) {
            this.badMVDictator = badMVDictator;
            return this;
        }

        public Builder setExceedMaximumReviveWaitingTimeDictator(
                Predicate<MVLifecycle> exceedMaximumReviveWaitingTimeDictator) {
            this.exceedMaximumReviveWaitingTimeDictator = exceedMaximumReviveWaitingTimeDictator;
            return this;
        }

        public Builder setMVHitRatioProvider(Function<String, Double> mvHitRatioProvider) {
            this.mvHitRatioProvider = mvHitRatioProvider;
            return this;
        }

        public MVPhasePolicy build() {
            return new MVPhasePolicy(
                    infantAbortionDictator,
                    initialRefreshFailureDictator,
                    internshipPeriodEndedDictator,
                    goodMVDictator,
                    badMVDictator,
                    reachPerformanceEvaluationTimeDictator,
                    exceedMaximumReviveWaitingTimeDictator,
                    exceedExtinctionRetentionMaxTimeDictator,
                    mvHitRatioProvider);
        }
    }
}
