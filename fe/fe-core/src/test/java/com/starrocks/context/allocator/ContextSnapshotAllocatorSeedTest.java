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

package com.starrocks.context.allocator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@link ContextSnapshotAllocator#seed(long)} contract. The seed exists so the
 * leader's first iteration after promotion can lift the allocator above any
 * {@code snapshot_version} that the previous leader persisted into
 * {@code context_commits} / {@code context_entity_versions} / {@code context_entity_heads} /
 * {@code context_entity_fragments} / {@code context_entity_refs} /
 * {@code context_workspace_objects}. Without the seed the AtomicLong inside the allocator
 * starts from {@code getNextId()} on first call — which can be lower than any
 * already-persisted snapshot id, leading to PK collisions on {@code context_commits} when the
 * fresh leader writes its first upsert.
 *
 * <p>This test exercises the allocator directly. The wiring that calls {@code seed()} on
 * boot lives in {@code ContextMetaManager.seedSnapshotAllocator}; that wiring is exercised
 * indirectly by the snapshot fence/replay integration tests.
 */
public class ContextSnapshotAllocatorSeedTest {

    @Test
    public void seedLiftsCounterAboveObservedValue() {
        ContextSnapshotAllocator allocator = new ContextSnapshotAllocator();
        allocator.seed(1000L);
        // next() returns the post-increment value, so 1001 is the first id we hand out.
        Assertions.assertEquals(1001L, allocator.next());
        Assertions.assertEquals(1002L, allocator.next());
    }

    @Test
    public void seedIsMonotonicAndNeverGoesBackward() {
        // Multiple seed calls (e.g. the meta manager polling six tables, each producing a
        // different MAX(snapshot_version)) must collapse to the highest value, not the most
        // recent.
        ContextSnapshotAllocator allocator = new ContextSnapshotAllocator();
        allocator.seed(500L);
        allocator.seed(2000L);
        allocator.seed(1500L); // smaller than the prior seed — must be ignored.
        Assertions.assertEquals(2001L, allocator.next());
    }

    @Test
    public void seedDoesNotRegressAfterLocalIncrement() {
        ContextSnapshotAllocator allocator = new ContextSnapshotAllocator();
        // Seed once, increment a few times, then seed below the current value — the smaller
        // seed must be a no-op so the allocator never regresses on a follower→leader→follower
        // bounce where the new leader observed a slightly stale MAX.
        allocator.seed(100L);
        Assertions.assertEquals(101L, allocator.next());
        Assertions.assertEquals(102L, allocator.next());
        allocator.seed(50L);
        Assertions.assertEquals(103L, allocator.next());
    }
}
