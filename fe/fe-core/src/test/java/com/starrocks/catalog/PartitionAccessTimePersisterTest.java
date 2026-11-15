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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Plain unit tests for the all-FE persister: each FE flushes its own in-memory increment; the leader additionally
 * loads the read-path baseline and GCs the internal table. {@code GlobalStateMgr} and {@link PartitionAccessTimeStore}
 * are stubbed with JMockit, so no running cluster / {@code UtFrameUtils} setup is required. Each test drives a real
 * {@link PartitionAccessTimeMgr} so the flush/cleanup exercise the actual in-memory merge/scan logic.
 */
public class PartitionAccessTimePersisterTest {

    private static final long DB = 1L;
    private static final long TBL = 2L;

    // Wire GlobalStateMgr.getCurrentState() -> {this FE's map manager, isLeader}.
    private void stubGlobalState(GlobalStateMgr globalStateMgr, PartitionAccessTimeMgr mgr, boolean isLeader) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getPartitionAccessTimeMgr();
                result = mgr;
                minTimes = 0;
                globalStateMgr.isLeader();
                result = isLeader;
                minTimes = 0;
            }
        };
    }

    // Each FE persists its own increment: the entries at or newer than its watermark, straight from its own map.
    @Test
    public void testFlushPersistsOwnIncrement(@Mocked GlobalStateMgr globalStateMgr) {
        List<PartitionAccessTimeEntry> upserted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L)); // this FE's own record, ts = now (> watermark 0)
        stubGlobalState(globalStateMgr, mgr, true);
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<PartitionAccessTimeEntry> entries) {
                upserted.addAll(entries);
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertEquals(1, upserted.size());
        Assertions.assertEquals(100L, upserted.get(0).getPartitionId());
        // Nothing is drained from memory: the read path keeps serving from it.
        Assertions.assertTrue(mgr.getLastAccessTime(DB, TBL, 100L) > 0);
    }

    // A failed persist is swallowed and the map is left intact, so the next cycle retries the same increment
    // (the watermark was not advanced).
    @Test
    public void testFailedPersistKeepsMemory(@Mocked GlobalStateMgr globalStateMgr) {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L));
        stubGlobalState(globalStateMgr, mgr, true);
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<PartitionAccessTimeEntry> entries) {
                throw new RuntimeException("table gone");
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        Assertions.assertDoesNotThrow(persister::flushOnce);
        Assertions.assertTrue(mgr.getLastAccessTime(DB, TBL, 100L) > 0);
    }

    // Nothing at or newer than the watermark: upsert is not called at all.
    @Test
    public void testEmptyIncrementSkipsUpsert(@Mocked GlobalStateMgr globalStateMgr) {
        boolean[] upsertCalled = {false};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr(); // empty map
        stubGlobalState(globalStateMgr, mgr, true);
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<PartitionAccessTimeEntry> entries) {
                upsertCalled[0] = true;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertFalse(upsertCalled[0]);
    }

    // With the inclusive watermark (>=), an entry whose ts equals the persisted max is re-persisted rather than
    // lost: the second flush with no newer records still re-persists the boundary entry (idempotent under the
    // table's MAX aggregate).
    @Test
    public void testWatermarkBoundaryEntryIsRepersisted(@Mocked GlobalStateMgr globalStateMgr) {
        int[] upsertCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(new PartitionAccessTimeEntry(DB, TBL, 100L, 500L)));
        stubGlobalState(globalStateMgr, mgr, true);
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<PartitionAccessTimeEntry> entries) {
                upsertCalls[0]++;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce(); // persists {100@500}, advances watermark to 500
        persister.flushOnce(); // {100@500} is >= watermark 500 => re-persisted (inclusive boundary)

        Assertions.assertEquals(2, upsertCalls[0]);
    }

    // The leader loads the persisted table into its map and sets the watermark to the max loaded ts, so the very
    // next flush re-persists only the boundary entry, not the whole baseline.
    @Test
    public void testLoadBaselineSeedsMapAndSetsWatermark(@Mocked GlobalStateMgr globalStateMgr) {
        List<PartitionAccessTimeEntry> upserted = new ArrayList<>();
        int[] upsertCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        stubGlobalState(globalStateMgr, mgr, true);
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public List<PartitionAccessTimeEntry> loadAll() {
                return Lists.newArrayList(
                        new PartitionAccessTimeEntry(DB, TBL, 100L, 500L),
                        new PartitionAccessTimeEntry(DB, TBL, 200L, 700L));
            }

            @Mock
            public void upsert(List<PartitionAccessTimeEntry> entries) {
                upsertCalls[0]++;
                upserted.addAll(entries);
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.loadBaseline();
        // Baseline is now in memory (serves reads without a table query on the leader).
        Assertions.assertEquals(500L, mgr.getLastAccessTime(DB, TBL, 100L));
        Assertions.assertEquals(700L, mgr.getLastAccessTime(DB, TBL, 200L));

        // Watermark == max loaded ts (700). A flush with no new records must not re-insert the WHOLE baseline: the
        // inclusive snapshot re-persists only the boundary entry (partition 200); the older 100@500 stays below.
        persister.flushOnce();
        Assertions.assertEquals(1, upsertCalls[0]);
        Assertions.assertEquals(1, upserted.size());
        Assertions.assertEquals(200L, upserted.get(0).getPartitionId());
        Assertions.assertEquals(700L, upserted.get(0).getAccessTimeMs());
    }

    // cleanupMemory evicts only the dropped partitions from THIS FE's own map, and never touches the table.
    @Test
    public void testCleanupMemoryEvictsDroppedFromMemoryOnly(@Mocked GlobalStateMgr globalStateMgr) {
        boolean[] deleted = {false};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(
                new PartitionAccessTimeEntry(DB, TBL, 100L, 10L),
                new PartitionAccessTimeEntry(DB, TBL, 200L, 20L)));
        stubGlobalState(globalStateMgr, mgr, false); // runs on any FE
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void deleteByPartitionIds(Collection<Long> ids) {
                deleted[0] = true;
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            boolean partitionExists(long dbId, long tableId, long partitionId) {
                return partitionId == 100L; // 100 is live, 200 was dropped
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.cleanupMemory();

        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB, TBL, 200L)); // dropped evicted from memory
        Assertions.assertEquals(10L, mgr.getLastAccessTime(DB, TBL, 100L)); // live kept
        Assertions.assertFalse(deleted[0]); // cleanupMemory never issues a table DELETE
    }

    // cleanupTable (leader-only) uses a full SELECT of the table as the authoritative row set and deletes only the
    // rows whose partition no longer resolves -- independent of what is in this FE's memory.
    @Test
    public void testCleanupTableDeletesDroppedFromFullScan(@Mocked GlobalStateMgr globalStateMgr) {
        List<Long> deleted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr(); // empty: cleanupTable reads the table, not memory
        stubGlobalState(globalStateMgr, mgr, true); // leader
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public List<PartitionAccessTimeEntry> loadAll() {
                return Lists.newArrayList(
                        new PartitionAccessTimeEntry(DB, TBL, 100L, 10L),
                        new PartitionAccessTimeEntry(DB, TBL, 200L, 20L));
            }

            @Mock
            public void deleteByPartitionIds(Collection<Long> ids) {
                deleted.addAll(ids);
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            boolean partitionExists(long dbId, long tableId, long partitionId) {
                return partitionId == 100L; // 100 live, 200 dropped
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.cleanupTable();

        Assertions.assertEquals(Lists.newArrayList(200L), deleted);
    }

    // cleanupTable re-checks leadership right before the DELETE: a demotion landing mid-scan aborts it (so a stale
    // leader cannot delete under a leadership it has already lost).
    @Test
    public void testCleanupTableSkipsDeleteWhenNotLeader(@Mocked GlobalStateMgr globalStateMgr) {
        boolean[] deleted = {false};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        stubGlobalState(globalStateMgr, mgr, false); // not (or no longer) leader
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public List<PartitionAccessTimeEntry> loadAll() {
                return Lists.newArrayList(new PartitionAccessTimeEntry(DB, TBL, 200L, 20L));
            }

            @Mock
            public void deleteByPartitionIds(Collection<Long> ids) {
                deleted[0] = true;
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            boolean partitionExists(long dbId, long tableId, long partitionId) {
                return false; // dropped -> would be deleted if leadership still held
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.cleanupTable();

        Assertions.assertFalse(deleted[0]);
    }

    // A follower still flushes its own increment, but does neither the baseline load nor the table cleanup (both
    // read the table via loadAll) -- those are leader-only.
    @Test
    public void testFollowerFlushesButSkipsLeaderOnlyWork(@Mocked GlobalStateMgr globalStateMgr) {
        boolean[] loadAllCalled = {false};
        boolean[] upserted = {false};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L));
        stubGlobalState(globalStateMgr, mgr, false); // follower
        boolean saved = Config.enable_collect_partition_access_time;
        Config.enable_collect_partition_access_time = true;
        try {
            new MockUp<PartitionAccessTimeStore>() {
                @Mock
                public List<PartitionAccessTimeEntry> loadAll() {
                    loadAllCalled[0] = true;
                    return Lists.newArrayList();
                }

                @Mock
                public void upsert(List<PartitionAccessTimeEntry> entries) {
                    upserted[0] = true;
                }
            };
            new MockUp<PartitionAccessTimePersister>() {
                @Mock
                boolean tableExists() {
                    return true;
                }

                @Mock
                boolean partitionExists(long dbId, long tableId, long partitionId) {
                    return true; // nothing to evict, keeps cleanupMemory from touching the metastore mocks
                }
            };

            PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
            persister.runAfterCatalogReady();

            Assertions.assertTrue(upserted[0], "follower still flushes its own increment");
            Assertions.assertFalse(loadAllCalled[0], "follower does neither loadBaseline nor table cleanup");
        } finally {
            Config.enable_collect_partition_access_time = saved;
        }
    }

    // The baseline load is leader-only and re-armed on demotion: an FE that leads, is demoted, then leads again
    // reloads the baseline on re-promotion. Observed via store.loadAll(): exactly once per leadership term.
    @Test
    public void testBaselineReloadsWhenLeadershipIsRegained(@Mocked GlobalStateMgr globalStateMgr) {
        int[] loadAllCalls = {0};
        boolean[] leader = {true};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getPartitionAccessTimeMgr();
                result = mgr;
                minTimes = 0;
                globalStateMgr.isLeader();
                result = new Delegate<Boolean>() {
                    boolean isLeader() {
                        return leader[0];
                    }
                };
                minTimes = 0;
            }
        };
        boolean saved = Config.enable_collect_partition_access_time;
        Config.enable_collect_partition_access_time = true;
        try {
            new MockUp<PartitionAccessTimeStore>() {
                @Mock
                public List<PartitionAccessTimeEntry> loadAll() {
                    loadAllCalls[0]++;
                    return Lists.newArrayList(new PartitionAccessTimeEntry(DB, TBL, 100L, 500L));
                }
            };
            // Only loadBaseline reaches store.loadAll(); flush/cleanup are stubbed away so the count is unambiguous.
            new MockUp<PartitionAccessTimePersister>() {
                @Mock
                boolean tableExists() {
                    return true;
                }

                @Mock
                void flushOnce() {
                }

                @Mock
                void cleanupMemory() {
                }

                @Mock
                void cleanupTable() {
                }
            };

            PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
            persister.runAfterCatalogReady(); // leader: loads the baseline
            persister.runAfterCatalogReady(); // leader, already loaded: no reload
            Assertions.assertEquals(1, loadAllCalls[0]);

            leader[0] = false;
            persister.runAfterCatalogReady(); // follower: re-arm, no load
            Assertions.assertEquals(1, loadAllCalls[0]);

            leader[0] = true;
            persister.runAfterCatalogReady(); // leader again: reload
            Assertions.assertEquals(2, loadAllCalls[0]);
        } finally {
            Config.enable_collect_partition_access_time = saved;
        }
    }

    // Cleanup is rate-limited: it runs on the first cycle, then only once every CLEANUP_EVERY_N_CYCLES cycles.
    @Test
    public void testCleanupIsRateLimitedAcrossCycles(@Mocked GlobalStateMgr globalStateMgr) {
        int[] memCalls = {0};
        int[] tableCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        stubGlobalState(globalStateMgr, mgr, true); // leader, so cleanupTable is eligible
        boolean saved = Config.enable_collect_partition_access_time;
        Config.enable_collect_partition_access_time = true;
        try {
            new MockUp<PartitionAccessTimePersister>() {
                @Mock
                boolean tableExists() {
                    return true;
                }

                @Mock
                void loadBaseline() {
                }

                @Mock
                void flushOnce() {
                }

                @Mock
                void cleanupMemory() {
                    memCalls[0]++;
                }

                @Mock
                void cleanupTable() {
                    tableCalls[0]++;
                }
            };

            PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
            persister.runAfterCatalogReady(); // cycle 0: cleanup runs
            persister.runAfterCatalogReady(); // cycle 1: gated, no cleanup
            Assertions.assertEquals(1, memCalls[0]);
            Assertions.assertEquals(1, tableCalls[0]);
        } finally {
            Config.enable_collect_partition_access_time = saved;
        }
    }

    // partitionExists resolves db -> table -> partition through LocalMetastore's *IncludeRecycleBin helpers (each
    // folds "live or recycle bin" into one lookup) and prunes only when the partition is absent from every layer.
    // Distinct ids per case so the mocked lookups don't collide.
    @Test
    public void testPartitionExistsHonorsRecycleBin(@Mocked GlobalStateMgr globalStateMgr,
                                                    @Mocked LocalMetastore localMetastore,
                                                    @Mocked OlapTable olapTable,
                                                    @Mocked Partition partition,
                                                    @Mocked Database database) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;
                // db DB resolves (live or recycled) for cases A, B, E, F; db 555 resolves (recycled) for D.
                localMetastore.getDbIncludeRecycleBin(DB);
                result = database;
                minTimes = 0;
                localMetastore.getDbIncludeRecycleBin(555L);
                result = database;
                minTimes = 0;
                // A/B) table TBL live: partition 100 live, partition 200 sits in the partition recycle bin.
                localMetastore.getTableIncludeRecycleBin(database, TBL);
                result = olapTable;
                minTimes = 0;
                localMetastore.getPartitionIncludeRecycleBin(olapTable, 100L);
                result = partition;
                minTimes = 0;
                localMetastore.getPartitionIncludeRecycleBin(olapTable, 200L);
                result = partition;
                minTimes = 0;
                // C) table 888 DROP'd: partition 300 rides inside the recycled table.
                localMetastore.getTableIncludeRecycleBin(database, 888L);
                result = olapTable;
                minTimes = 0;
                localMetastore.getPartitionIncludeRecycleBin(olapTable, 300L);
                result = partition;
                minTimes = 0;
                // D) database 555 DROP'd: descend db -> table 100 -> partition 400.
                localMetastore.getTableIncludeRecycleBin(database, 100L);
                result = olapTable;
                minTimes = 0;
                localMetastore.getPartitionIncludeRecycleBin(olapTable, 400L);
                result = partition;
                minTimes = 0;
                // E) table 999 gone from every layer.
                localMetastore.getTableIncludeRecycleBin(database, 999L);
                result = null;
                minTimes = 0;
                // F) orphan: table 666 is in the recycle bin but no longer holds partition 600 (it was DROP'd and
                // GC'd before the table was dropped) -- the precise descent prunes it instead of keeping it forever.
                localMetastore.getTableIncludeRecycleBin(database, 666L);
                result = olapTable;
                minTimes = 0;
                localMetastore.getPartitionIncludeRecycleBin(olapTable, 600L);
                result = null;
                minTimes = 0;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        Assertions.assertTrue(persister.partitionExists(DB, TBL, 100L));    // A) live table, live partition
        Assertions.assertTrue(persister.partitionExists(DB, TBL, 200L));    // B) live table, DROP PARTITION in bin
        Assertions.assertTrue(persister.partitionExists(DB, 888L, 300L));   // C) DROP TABLE, partition inside it
        Assertions.assertTrue(persister.partitionExists(555L, 100L, 400L)); // D) DROP DATABASE, descend to partition
        Assertions.assertFalse(persister.partitionExists(DB, 999L, 500L));  // E) gone from every layer -> prune
        Assertions.assertFalse(persister.partitionExists(DB, 666L, 600L));  // F) orphan: table in bin, partition not
    }
}
