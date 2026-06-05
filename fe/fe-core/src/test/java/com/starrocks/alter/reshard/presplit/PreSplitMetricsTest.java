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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PreSplitMetrics#recordPreCreate}, the multi-partition
 * pre-create result counter.
 *
 * <p>The counter writer is guarded by {@link MetricRepo#hasInit}: when the
 * registry is not initialized (the common unit-test state) it must be a no-op;
 * when it IS initialized, each {@link PreSplitMetrics.PreCreateResult} bucket
 * bumps its own labeled counter. {@code COUNTER_TABLET_PRE_SPLIT_PRE_CREATE} is
 * a {@code static final} {@code MetricWithLabelGroup} so the labeled counters
 * survive without explicit wiring.
 */
public class PreSplitMetricsTest {

    private boolean savedHasInit;

    @BeforeEach
    public void setUp() {
        savedHasInit = MetricRepo.hasInit;
    }

    @AfterEach
    public void tearDown() {
        MetricRepo.hasInit = savedHasInit;
    }

    @Test
    public void recordPreCreateNoopWhenMetricsNotInit() {
        MetricRepo.hasInit = false;
        // Must not touch the (possibly-null) registry when uninitialized.
        Assertions.assertDoesNotThrow(() ->
                PreSplitMetrics.recordPreCreate(PreSplitMetrics.PreCreateResult.ALREADY_EXISTS));
    }

    @Test
    public void recordPreCreateBumpsEachResultBucket() {
        MetricRepo.hasInit = true;
        for (PreSplitMetrics.PreCreateResult result : PreSplitMetrics.PreCreateResult.values()) {
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PRE_CREATE
                    .getMetric(result.name().toLowerCase()).getValue();
            PreSplitMetrics.recordPreCreate(result);
            long after = MetricRepo.COUNTER_TABLET_PRE_SPLIT_PRE_CREATE
                    .getMetric(result.name().toLowerCase()).getValue();
            Assertions.assertEquals(before + 1L, after,
                    "recordPreCreate must bump the " + result + " bucket exactly once");
        }
    }
}
