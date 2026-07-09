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

package com.starrocks.context;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Bootstraps the hidden {@code __internal_context} database and its eight Primary Key tables on leader
 * promotion. Runs as a {@link LeaderDaemon} so it only executes while this FE holds a valid leader lease,
 * self-stops on demotion, and is safely restartable when the same FE is re-elected.
 *
 * <p>The daemon is intentionally minimal: it only creates the database and delegates per-table lifecycle
 * to {@link TableKeeper} instances produced by {@link ContextInternalTables}. Semantic-context read/write
 * orchestration lives in {@code ContextMgr} and downstream executors, not here.
 *
 * <p>The version/snapshot-allocator seed flags are leader-session state: {@link #onStopped()} clears them
 * on demotion so a subsequent re-election re-seeds the in-memory allocators from the persisted MAX()
 * rather than reusing stale high-water marks (which would let a writer mint a version/snapshot id already
 * persisted by another leader and collide on the PK-backed history).
 */
public class ContextMetaManager extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(ContextMetaManager.class);

    private static final long RUN_INTERVAL_MS = 60L * 1000L;

    private final List<TableKeeper> keepers;
    // The version-allocator high-water seed is a one-shot per leader: once we've picked up the
    // max(version) per entity from `context_entity_versions` we stop re-running it on every tick.
    // The seeds survive in the in-memory allocator as long as the FE is leader; on failover the
    // new leader runs this routine once during its first iteration after the internal tables come
    // up, which restores the same monotonic guarantee.
    private volatile boolean versionAllocatorSeeded = false;
    // Snapshot allocator is seeded the same way (and for the same reason) as the version
    // allocator. The internal AtomicLong only seeds itself from getNextId() on first use, and
    // getNextId() can lag behind already-persisted snapshot_version values across restarts —
    // a restarted leader could otherwise hand out a snapshot id that already exists in
    // context_commits and collide on its primary key.
    private volatile boolean snapshotAllocatorSeeded = false;

    public ContextMetaManager() {
        super("context-meta-manager", RUN_INTERVAL_MS);
        this.keepers = ContextInternalTables.createKeepers();
    }

    @Override
    public synchronized void start() {
        // Master switch for the whole Context Base module. When disabled (the default) we do not
        // start the daemon at all, so no internal tables are bootstrapped and no allocators are
        // seeded. Operators opt in via `enable_context_base=true` and an FE restart. Because
        // LeaderDaemon.start() is called again on every re-election, a flag flipped to true between
        // elections is picked up on the next promotion without a restart.
        if (!com.starrocks.common.Config.enable_context_base) {
            LOG.info("Context Base module is disabled (enable_context_base=false); "
                    + "ContextMetaManager will not start.");
            return;
        }
        super.start();
    }

    @Override
    protected void onStopped() {
        // Release leader-session state on demotion. Cleared here (after the worker has exited) so a
        // later re-election re-runs seedVersionAllocator()/seedSnapshotAllocator() from the persisted
        // MAX() instead of trusting this session's high-water marks — another leader may have advanced
        // the version/snapshot ids while we were a follower, and reusing the stale seed would let a
        // writer mint an id at/below the persisted max and collide on the PK-backed history.
        versionAllocatorSeeded = false;
        snapshotAllocatorSeeded = false;
    }

    @Override
    protected void runAfterLeaseValid() {
        // Even after start(), honor a runtime flip of enable_context_base=false by going inert.
        // Startup is gated in start(); this additionally covers toggling the mutable config on a
        // running leader (the daemon keeps ticking but does no work until re-enabled + restarted).
        if (!com.starrocks.common.Config.enable_context_base) {
            return;
        }
        if (FeConstants.runningUnitTest) {
            // Skip in unit tests: eager database creation here bumps the global id generator and
            // breaks dozens of downstream tests that assert on hard-coded catalog/db/table ids.
            return;
        }
        // LeaderDaemon only invokes this after FE is ready AND our captured leader lease is still
        // valid, so we are unambiguously the leader here. No manual isLeader()/isReady() gate and no
        // in-tick sleep: on demotion the lease check self-stops the worker and onStopped() clears the
        // seed flags, which is what re-establishes the monotonic version/snapshot invariant on
        // re-election. (The old FrontendDaemon design could not do this: its runOneCycle parked in
        // while(!isReady()) during demotion, so the reset branch was never reached.)
        ensureDatabase();
        // The FRAGMENTS DDL declares both an inline `INDEX … USING GIN ("parser"="english")`
        // and an inline `INDEX … USING VECTOR (...HNSW...)` on the embedding column. Both
        // indexes are gated by experimental flags that default to false; without them
        // CREATE TABLE for context_entity_fragments raises a SemanticException and the
        // module silently degrades to zero search hits. We require operators to opt in
        // explicitly rather than flipping the flags globally — those flags also affect any
        // other subsystem that reads them, so silently mutating them was a footgun.
        if (!com.starrocks.common.Config.enable_experimental_gin
                || !com.starrocks.common.Config.enable_experimental_vector) {
            LOG.warn("semantic-context bootstrap is blocked: enable_experimental_gin={}, "
                            + "enable_experimental_vector={}. Set both to true via "
                            + "`ADMIN SET FRONTEND CONFIG` to enable the semantic-context module.",
                    com.starrocks.common.Config.enable_experimental_gin,
                    com.starrocks.common.Config.enable_experimental_vector);
            return;
        }
        for (TableKeeper keeper : keepers) {
            try {
                keeper.run();
            } catch (Exception e) {
                LOG.warn("semantic-context keeper {} failed", keeper.getTableName(), e);
            }
        }
        if (!versionAllocatorSeeded && isReady()) {
            seedVersionAllocator();
        }
        if (!snapshotAllocatorSeeded && isReady()) {
            seedSnapshotAllocator();
        }
    }

    /**
     * Seed the in-memory {@link com.starrocks.context.allocator.ContextVersionAllocator} with the
     * highest persisted {@code version} per {@code entity_id} from
     * {@link ContextInternalTables#VERSIONS}. Without this seed, a leader that restarts (or fails
     * over) would re-allocate version numbers starting at 1 for entities that already have
     * versions persisted — re-using version numbers and breaking the monotonic-version invariant
     * that {@code context_entity_versions(entity_id, version)} relies on as its primary key.
     *
     * <p>The seed is intentionally bounded: we read per-entity maxima in a single GROUP BY scan,
     * which scales with the entity count rather than the version count. For very large datasets
     * this can be moved to a paged sweep, but the current shape covers the production-realistic
     * case where entities are O(10^5) per FE.
     */
    private void seedVersionAllocator() {
        String sql = String.format(
                "SELECT entity_id, MAX(version) FROM %s.%s GROUP BY entity_id",
                ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS);
        try {
            JsonArray rows = runQuery(sql);
            com.starrocks.context.allocator.ContextVersionAllocator allocator =
                    GlobalStateMgr.getCurrentState().getContextVersionAllocator();
            int seeded = 0;
            for (JsonElement row : rows) {
                JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
                if (data.size() < 2 || data.get(0).isJsonNull() || data.get(1).isJsonNull()) {
                    continue;
                }
                long entityId = data.get(0).getAsLong();
                long version = data.get(1).getAsLong();
                allocator.seed(entityId, version);
                seeded++;
            }
            LOG.info("seeded context version allocator for {} entities from {}.{}",
                    seeded, ContextInternalTables.DATABASE, ContextInternalTables.VERSIONS);
            versionAllocatorSeeded = true;
        } catch (Exception e) {
            // Don't poison the allocator state if the read failed; we'll retry on the next tick.
            LOG.warn("seed version allocator failed; will retry next iteration: {}", e.getMessage());
        }
    }

    /**
     * Seed the in-memory {@link com.starrocks.context.allocator.ContextSnapshotAllocator} with
     * the highest {@code snapshot_version} persisted across every table that stores one. Without
     * this seed, a restarted leader's allocator only knows the value of
     * {@link GlobalStateMgr#getNextId()} at first call — which can be lower than any snapshot
     * the previous leader handed out, because the snapshot allocator advances via its private
     * {@link java.util.concurrent.atomic.AtomicLong} without re-consulting the global id
     * generator. The collision shows up as a duplicate primary key on
     * {@code context_commits(snapshot_version)} on the next write and corrupts snapshot fencing
     * for any read.
     *
     * <p>We scan {@code commits}, {@code versions}, {@code heads}, {@code fragments},
     * {@code refs}, and {@code workspace_objects} so we don't miss snapshots issued by any path
     * — workspace writes also pull from the same allocator. Each query is a single MAX scan, so
     * the total cost is six round-trips on the leader's first iteration only.
     */
    private void seedSnapshotAllocator() {
        long maxSeen = 0L;
        String[][] snapshotColumns = new String[][] {
                {"snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.COMMITS},
                {"snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS},
                {"current_snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.HEADS},
                {"snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.FRAGMENTS},
                {"snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.REFS},
                {"snapshot_version", ContextInternalTables.DATABASE + "." + ContextInternalTables.WORKSPACE_OBJECTS},
        };
        try {
            for (String[] pair : snapshotColumns) {
                JsonArray rows = runQuery("SELECT MAX(" + pair[0] + ") FROM " + pair[1]);
                if (rows.size() == 0) {
                    continue;
                }
                JsonElement el = rows.get(0).getAsJsonObject().getAsJsonArray("data").get(0);
                if (el.isJsonNull()) {
                    continue;
                }
                long v = el.getAsLong();
                if (v > maxSeen) {
                    maxSeen = v;
                }
            }
            com.starrocks.context.allocator.ContextSnapshotAllocator allocator =
                    GlobalStateMgr.getCurrentState().getContextSnapshotAllocator();
            allocator.seed(maxSeen);
            LOG.info("seeded context snapshot allocator to {} from persisted state", maxSeen);
            snapshotAllocatorSeeded = true;
        } catch (Exception e) {
            // Same defensive posture as the version-allocator seed: don't poison the in-memory
            // counter on a transient SQL error; retry on the next tick.
            LOG.warn("seed snapshot allocator failed; will retry next iteration: {}", e.getMessage());
        }
    }

    private JsonArray runQuery(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }

    private void ensureDatabase() {
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(ContextInternalTables.DATABASE) != null) {
            return;
        }
        CreateDbStmt stmt = new CreateDbStmt(true, ContextInternalTables.DATABASE);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(stmt.getFullDbName());
            LOG.info("created semantic-context internal database {}", ContextInternalTables.DATABASE);
        } catch (StarRocksException e) {
            LOG.warn("failed to create semantic-context internal database", e);
        }
    }

    public boolean isReady() {
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(ContextInternalTables.DATABASE) == null) {
            return false;
        }
        for (TableKeeper keeper : keepers) {
            if (!keeper.isReady()) {
                return false;
            }
        }
        return true;
    }
}
