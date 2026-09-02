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

package com.starrocks.lake.bookmark;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class ReshardEpochResolver {

    // Breaks predecessor-pointer cycles; far above any real reshard-chain length.
    private static final int MAX_CHAIN_LENGTH = 4096;

    private ReshardEpochResolver() {
    }

    /**
     * Split (fromVersion, toVersion] at generation-takeover boundaries along the reshard lineage
     * from {@code fromIndexId} to {@code toIndexId} (same index meta id). A reshard's commit
     * version S belongs to NEITHER side: the retiring generation's slice ends at S - 1 and the
     * succeeding generation's slice starts exclusive at S -- S is the reshard publish itself and
     * carries no logical change.
     *
     * <p>The lineage is walked by {@link #walkLineage}; on top of that walk this method fails
     * closed on non-increasing takeover versions and on endpoint versions outside their
     * generations. Caller must hold a table read lock or the planner meta lock.
     */
    public static Optional<List<IndexEpoch>> resolveEpochs(PhysicalPartition partition, long indexMetaId,
            long fromIndexId, long fromVersion, long toIndexId, long toVersion) {
        if (fromVersion > toVersion) {
            return Optional.empty();
        }
        Optional<List<MaterializedIndex>> walked = walkLineage(partition, indexMetaId, fromIndexId, toIndexId);
        if (walked.isEmpty()) {
            return Optional.empty();
        }
        List<MaterializedIndex> chain = walked.get();
        // Sanity, fail closed: takeovers strictly increase along the chain, and the endpoint
        // versions lie within their generations -- including fromVersion not predating the base
        // generation's own takeover (a reshard-created base generation has no versions below it).
        for (int k = 1; k < chain.size(); k++) {
            long prevTakeover = chain.get(k - 1).getTakeoverVersion();
            if (chain.get(k).getTakeoverVersion() <= prevTakeover) {
                return Optional.empty();
            }
        }
        long fromGenerationTakeover = chain.get(0).getTakeoverVersion();
        if (fromGenerationTakeover > 0 && fromVersion < fromGenerationTakeover) {
            return Optional.empty();
        }
        if (chain.size() > 1) {
            if (fromVersion >= chain.get(1).getTakeoverVersion()
                    || toVersion < chain.get(chain.size() - 1).getTakeoverVersion()) {
                return Optional.empty();
            }
        }
        List<IndexEpoch> epochs = new ArrayList<>(chain.size());
        for (int k = 0; k < chain.size(); k++) {
            MaterializedIndex index = chain.get(k);
            long lo = (k == 0) ? fromVersion : index.getTakeoverVersion();
            long hi = (k == chain.size() - 1) ? toVersion : chain.get(k + 1).getTakeoverVersion() - 1;
            if (lo < hi) {
                epochs.add(new IndexEpoch(index, lo, hi));
            }
        }
        return Optional.of(epochs);
    }

    /**
     * Whether {@code toIndexId} descends from {@code fromIndexId} through reshard lineage on
     * {@code partition} (trivially true when they are the same installed generation).
     *
     * <p>This is the resolvability test for a read scoped to a superseded generation: only a
     * generation reachable by stamped reshard lineage is held installed by the recycle bin's
     * bookmark gate, so only such a generation may be read. An unstamped one -- a pre-upgrade
     * parking with no recorded supersede watermark, or one produced by any non-reshard index
     * replacement -- can be erased on any recycle cycle regardless of live bookmarks, so scoping a
     * read to it would race the erase; it must be rejected up front instead.
     */
    public static boolean isLineageConnected(PhysicalPartition partition, long indexMetaId,
            long fromIndexId, long toIndexId) {
        return walkLineage(partition, indexMetaId, fromIndexId, toIndexId).isPresent();
    }

    /**
     * Walks BACKWARD from {@code toIndexId} via each generation's predecessorIndexId until
     * {@code fromIndexId} is reached, returning the generations oldest -> newest.
     *
     * <p>Every link must resolve to an installed index carrying {@code indexMetaId}: an erased
     * middle generation leaves a dangling pointer and fails the walk closed (the partition's
     * generation-id list cannot express the hole -- erase removes the id from it). Every
     * generation except the terminal {@code fromIndexId} must also carry reshard lineage stamps,
     * which only reshard jobs set, so e.g. a rebucket-produced generation never resolves. The
     * chain length is capped to break a pointer cycle. Any failure yields an empty result.
     */
    private static Optional<List<MaterializedIndex>> walkLineage(PhysicalPartition partition, long indexMetaId,
            long fromIndexId, long toIndexId) {
        List<MaterializedIndex> chain = new ArrayList<>(); // built newest -> oldest, then reversed
        long currentId = toIndexId;
        while (true) {
            MaterializedIndex index = partition.getIndex(currentId);
            if (index == null || index.getMetaId() != indexMetaId || chain.size() >= MAX_CHAIN_LENGTH) {
                return Optional.empty();
            }
            chain.add(index);
            if (currentId == fromIndexId) {
                break;
            }
            if (index.getTakeoverVersion() <= 0 || index.getPredecessorIndexId() <= 0) {
                return Optional.empty();
            }
            currentId = index.getPredecessorIndexId();
        }
        Collections.reverse(chain); // oldest -> newest
        return Optional.of(chain);
    }
}
