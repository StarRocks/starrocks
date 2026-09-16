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

import com.starrocks.catalog.Tuple;
import com.starrocks.common.util.RuntimeProfile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PreSplitProfileTest {

    @Test
    public void testBuildProfileWithPhaseBreakdownAndDiagnosticIds() {
        AtomicLong nowNs = new AtomicLong();
        PreSplitProfile preSplitProfile = new PreSplitProfile(nowNs::get);

        try (PreSplitProfile.Scope ignored =
                     PreSplitProfile.startAttempt(preSplitProfile, LoadKind.INSERT_FROM_TABLE)) {
            PreSplitProfile.recordTable("demandbase_target");

            nowNs.set(5L);
            try (PreSplitProfile.Scope sample = PreSplitProfile.startPhase(
                    PreSplitProfile.Phase.SOURCE_SAMPLING)) {
                nowNs.set(25L);
            }
            PreSplitProfile.recordSourceTier(DefaultPreSplitPipeline.TIER_LABEL_DATA_TIER);
            PreSplitProfile.recordSample(new SampleSet(
                    List.of(new Tuple(List.of())), new Estimates(1_024L, 1L)));
            PreSplitProfile.recordSampleQueryId(null, new UUID(1L, 2L));

            try (PreSplitProfile.Scope planning = PreSplitProfile.startPhase(
                    PreSplitProfile.Phase.PARTITION_AND_BOUNDARY_PLANNING)) {
                nowNs.set(35L);
            }
            PreSplitProfile.recordTargetPartitions(3L);
            PreSplitProfile.recordBoundariesPlanned(12L);

            try (PreSplitProfile.Scope submission = PreSplitProfile.startPhase(
                    PreSplitProfile.Phase.JOB_SUBMISSION)) {
                nowNs.set(42L);
            }
            PreSplitProfile.recordReshardJobId(99L);

            try (PreSplitProfile.Scope wait = PreSplitProfile.startPhase(
                    PreSplitProfile.Phase.RESHARD_WAIT)) {
                nowNs.set(72L);
            }
            PreSplitProfile.recordOutcome("FINISHED");
            nowNs.set(80L);
        }

        RuntimeProfile profile = preSplitProfile.toRuntimeProfile();
        Assertions.assertEquals(80L, profile.getCounterTotalTime().getValue());
        Assertions.assertEquals(20L, profile.getCounter(PreSplitProfile.SOURCE_SAMPLING_TIME).getValue());
        Assertions.assertEquals(10L,
                profile.getCounter(PreSplitProfile.PARTITION_AND_BOUNDARY_PLANNING_TIME).getValue());
        Assertions.assertEquals(7L, profile.getCounter(PreSplitProfile.JOB_SUBMISSION_TIME).getValue());
        Assertions.assertEquals(30L, profile.getCounter(PreSplitProfile.RESHARD_WAIT_TIME).getValue());
        Assertions.assertEquals(1L, profile.getCounter(PreSplitProfile.ATTEMPTS).getValue());
        Assertions.assertEquals(1L, profile.getCounter(PreSplitProfile.SAMPLE_ROWS).getValue());
        Assertions.assertEquals(1_024L, profile.getCounter(PreSplitProfile.ESTIMATED_INPUT_BYTES).getValue());
        Assertions.assertEquals(3L, profile.getCounter(PreSplitProfile.TARGET_PARTITIONS).getValue());
        Assertions.assertEquals(12L, profile.getCounter(PreSplitProfile.BOUNDARIES_PLANNED).getValue());
        Assertions.assertEquals("INSERT-from-table", profile.getInfoString("LoadKinds"));
        Assertions.assertEquals("demandbase_target", profile.getInfoString("Tables"));
        Assertions.assertEquals("data_tier", profile.getInfoString("SourceTiers"));
        Assertions.assertEquals("FINISHED", profile.getInfoString("Outcomes"));
        Assertions.assertNotNull(profile.getInfoString("SampleQueryIds"));
        Assertions.assertEquals("99", profile.getInfoString("ReshardJobIds"));

        RuntimeProfile root = new RuntimeProfile("Query");
        PreSplitProfile.appendTo(root, preSplitProfile);
        Assertions.assertNotNull(root.getChild(PreSplitProfile.PROFILE_NAME));
    }

    @Test
    public void testProfileNodeIsOmittedWithoutAttempt() {
        RuntimeProfile root = new RuntimeProfile("Query");
        PreSplitProfile.appendTo(root, new PreSplitProfile());
        Assertions.assertNull(root.getChild(PreSplitProfile.PROFILE_NAME));
    }

    @Test
    public void testEstimatedInputBytesDeduplicatesWithinAttemptAndSumsAcrossAttempts() {
        PreSplitProfile profile = new PreSplitProfile();

        try (PreSplitProfile.Scope ignored =
                     PreSplitProfile.startAttempt(profile, LoadKind.BROKER_LOAD)) {
            PreSplitProfile.recordEstimatedInputBytes(10L);
            PreSplitProfile.recordSample(sampleWithEstimatedBytes(10L));
            PreSplitProfile.recordSample(sampleWithEstimatedBytes(10L));
            PreSplitProfile.recordSample(sampleWithEstimatedBytes(8L));
        }
        try (PreSplitProfile.Scope ignored =
                     PreSplitProfile.startAttempt(profile, LoadKind.BROKER_LOAD)) {
            PreSplitProfile.recordEstimatedInputBytes(20L);
            PreSplitProfile.recordSample(sampleWithEstimatedBytes(20L));
            PreSplitProfile.recordSample(sampleWithEstimatedBytes(15L));
        }

        Assertions.assertEquals(30L,
                profile.toRuntimeProfile().getCounter(PreSplitProfile.ESTIMATED_INPUT_BYTES).getValue());
    }

    private static SampleSet sampleWithEstimatedBytes(long estimatedBytes) {
        return new SampleSet(List.of(new Tuple(List.of())), new Estimates(estimatedBytes, 1L));
    }
}
