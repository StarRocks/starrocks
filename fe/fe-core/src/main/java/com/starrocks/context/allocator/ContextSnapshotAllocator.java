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

import com.starrocks.server.GlobalStateMgr;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Allocates globally monotonic {@code snapshot_version} values for semantic-context commits.
 *
 * <p>Each call returns a strictly increasing long that is also greater than any id previously handed
 * out by {@link GlobalStateMgr#getNextId()}. The initial seed comes from {@code getNextId()} so
 * snapshots never collide with entity ids, and successive calls go through {@link AtomicLong} to
 * minimize the hot path cost of taking the global id lock per commit.
 */
public final class ContextSnapshotAllocator {

    private final AtomicLong counter = new AtomicLong(0L);

    public ContextSnapshotAllocator() {
        // Seed lazily on first call so construction is safe inside the GlobalStateMgr constructor,
        // before getCurrentState() is wired up.
    }

    public long next() {
        long current = counter.get();
        if (current == 0L) {
            // First call on this allocator — seed from the global id allocator.
            long seed = GlobalStateMgr.getCurrentState().getNextId();
            counter.compareAndSet(0L, seed);
        }
        return counter.incrementAndGet();
    }

    /**
     * Seed the allocator to at least {@code observedSnapshot}. Used on leader promotion to recover
     * the maximum previously-issued snapshot from {@code context_commits}.
     */
    public void seed(long observedSnapshot) {
        counter.accumulateAndGet(observedSnapshot, Math::max);
    }

    public long peek() {
        return counter.get();
    }
}
