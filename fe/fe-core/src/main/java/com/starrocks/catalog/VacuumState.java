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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.Set;

/**
 * Per-partition state of the incremental (bounded, resumable) shared-data vacuum protocol, grouped into
 * one object so it can be persisted and replayed as a single unit. It mirrors {@code
 * VacuumStatePB} in lake_service.proto and round-trips to the BE each round:
 * <ul>
 *   <li>{@code toDeleteLow/toDeleteHigh}: the {@code [low, high)} range the previous round proposed, for
 *       the next round to commit. {@code high} is the EXCLUSIVE intersection retain floor.</li>
 *   <li>{@code nextProposeStartVersion}: the chain-walk resume cursor the BE returned. Non-zero means the
 *       round hit its per-round budget mid-walk, so the next round resumes from here; 0 means the walk
 *       reached the chain bottom, so the band that accompanies it is the pass's FINAL band. A reset state
 *       (empty band {@code toDeleteLow >= toDeleteHigh} plus cursor 0) is a fresh pass start.</li>
 *   <li>{@code passStartVersion}: the per-pass retain floor, captured on the fresh round and held
 *       constant for the rest of the pass.</li>
 *   <li>{@code minRetainedVersion}: the smallest still-retained version (everything strictly below is
 *       already vacuumed) -- the partition-level walk floor sent to the BE. Advanced to the pass retain
 *       floor on completion and preserved across {@link #reset}, unlike the four in-flight fields above.</li>
 * </ul>
 *
 * <p>The fields carry {@code @SerializedName} so this object persists in the FE image, and each round the
 * autovacuum coordinator writes an {@code OP_MODIFY_PARTITION_VACUUM_STATE} edit log under the table WRITE
 * lock (right after {@link #advance} / {@link #reset}). So an in-flight pass survives an FE restart /
 * failover and resumes from its cursor. If the persisted state is ever lost (e.g. a skipped replay), it
 * resets to 0, which restarts the pass from a fresh round -- idempotent on the BE (re-committing an
 * already-deleted range is a no-op), so the only cost is redoing the pass's progress.
 *
 * <p>Thread-safety: all mutation happens under the table WRITE lock and all cross-thread reads (e.g. the
 * in-flight check from the autovacuum scheduler) happen under the table READ lock, so plain {@code long}
 * fields are safely published; within a single partition's vacuum round, reads and writes are on the same
 * executor thread.
 */
public class VacuumState {
    @SerializedName(value = "lo")
    private long toDeleteLow;
    @SerializedName(value = "hi")
    private long toDeleteHigh;
    @SerializedName(value = "nps")
    private long nextProposeStartVersion;
    @SerializedName(value = "psv")
    private long passStartVersion;
    // The visible MaterializedIndex ids (getId()) the pass was started on -- its tablet generation. Captured
    // on the fresh round and held constant for the pass, like passStartVersion. Before committing the pass's
    // proposal each round the coordinator compares it to the current visible index-id set: disjoint means
    // every index the pass was based on was rebuilt under a new indexId (a range split or a sort-key schema
    // change replaces the whole tablet generation), so the proposal is stale and dropped -- the old
    // generation is reclaimed by its own delete-tablet teardown, not by committing this range. A rollup
    // add/drop only adds/removes a SEPARATE index and leaves the base index-id intact, so the sets still
    // intersect and the proposal is kept. In-flight like the four fields above: reset to empty on reset().
    @SerializedName(value = "psi")
    private Set<Long> passStartIndexIds = new HashSet<>();
    // The smallest still-retained version: everything strictly below it is already vacuumed, so the BE
    // walk floor stops here instead of re-descending. Advanced to the pass retain floor when a pass
    // completes and, unlike the four in-flight fields above, PRESERVED across reset() -- it is the piece of
    // pass state that must outlive a pass. Persisted (FE image + edit log) so it survives an FE restart /
    // failover; without it the next pass would re-walk from the bottom and re-read already-deleted versions
    // out of the metacache (which the delete does not evict).
    @SerializedName(value = "mrv")
    private long minRetainedVersion;

    public long getToDeleteLow() {
        return toDeleteLow;
    }

    public long getToDeleteHigh() {
        return toDeleteHigh;
    }

    public long getNextProposeStartVersion() {
        return nextProposeStartVersion;
    }

    public long getPassStartVersion() {
        return passStartVersion;
    }

    public Set<Long> getPassStartIndexIds() {
        return passStartIndexIds;
    }

    public long getMinRetainedVersion() {
        return minRetainedVersion;
    }

    public void setMinRetainedVersion(long minRetainedVersion) {
        this.minRetainedVersion = minRetainedVersion;
    }

    /**
     * A pass is in flight while there is still a proposed range left to commit, or a non-fresh resume
     * cursor to continue from. While in flight the partition must keep being scheduled so the pass drains,
     * independent of the success watermark (which only advances when a pass completes).
     */
    public boolean isInFlight() {
        return nextProposeStartVersion != 0 || toDeleteLow < toDeleteHigh;
    }

    /**
     * Record the range and resume cursor the BE proposed this round, to be committed next round. The pass
     * retain floor is captured only on a fresh round ({@code captureFloor}), when the BE returns it (and
     * only when it actually proposed a range, hence {@code passStartVersion > 0}); resume rounds keep
     * the floor the fresh round established.
     */
    public void advance(long toDeleteLow, long toDeleteHigh, long nextProposeStartVersion,
                        boolean captureFloor, long passStartVersion, Set<Long> passStartIndexIds) {
        this.toDeleteLow = toDeleteLow;
        this.toDeleteHigh = toDeleteHigh;
        this.nextProposeStartVersion = nextProposeStartVersion;
        if (captureFloor && passStartVersion > 0) {
            this.passStartVersion = passStartVersion;
        }
        // Capture the pass's tablet generation (visible index-id set) on the fresh round and hold it constant
        // for the pass, like passStartVersion; next round compares it to the current set to detect a
        // wholesale tablet-set replacement before committing this range.
        if (captureFloor) {
            this.passStartIndexIds = (passStartIndexIds == null) ? new HashSet<>() : new HashSet<>(passStartIndexIds);
        }
    }

    /**
     * Clear the in-flight fields so the next round starts a fresh pass (called when a pass completes).
     * {@code minRetainedVersion} is intentionally NOT cleared: it is the walk floor carried across passes.
     */
    public void reset() {
        toDeleteLow = 0;
        toDeleteHigh = 0;
        nextProposeStartVersion = 0;
        passStartVersion = 0;
        passStartIndexIds = new HashSet<>();
    }
}
