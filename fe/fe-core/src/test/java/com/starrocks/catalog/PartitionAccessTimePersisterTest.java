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
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TPartitionAccessTimeEntry;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Plain unit tests for the redesigned flush + cleanup. {@code GlobalStateMgr}/{@code NodeMgr} (the
 * authoritative map + peer topology), the peer-dump RPC, and the {@link PartitionAccessTimeStore} are stubbed
 * with JMockit, so no running cluster / {@code UtFrameUtils} setup is required. Each test drives a real
 * {@link PartitionAccessTimeMgr} so the flush/cleanup exercise the actual in-memory merge/scan logic.
 */
public class PartitionAccessTimePersisterTest {

    private static final long DB = 1L;
    private static final long TBL = 2L;

    private static TPartitionAccessTimeEntry entry(long db, long tbl, long part, long ts) {
        TPartitionAccessTimeEntry e = new TPartitionAccessTimeEntry();
        e.setDb_id(db);
        e.setTable_id(tbl);
        e.setPartition_id(part);
        e.setAccess_time_ms(ts);
        return e;
    }

    // Wire GlobalStateMgr.getCurrentState() -> {the given map manager, nodeMgr.getOtherFrontends()}.
    private void stubGlobalState(GlobalStateMgr globalStateMgr, NodeMgr nodeMgr,
                                 PartitionAccessTimeMgr mgr, List<Frontend> otherFrontends) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getPartitionAccessTimeMgr();
                result = mgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getOtherFrontends();
                result = otherFrontends;
                minTimes = 0;
            }
        };
    }

    private static Frontend alivePeer() {
        Frontend peer = new Frontend(FrontendNodeType.FOLLOWER, "fe2", "10.0.0.2", 9010);
        peer.setAlive(true);
        return peer;
    }

    // The flush persists the leader's own increment (whatever is newer than the watermark in the authoritative
    // map) unioned with every alive peer's freshly-drained increment.
    @Test
    public void testFlushPersistsLeaderAndPeerIncrement(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        List<TPartitionAccessTimeEntry> upserted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L)); // leader's own record, ts = now (> watermark 0)
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Lists.newArrayList(alivePeer()));
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upserted.addAll(entries);
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            List<TPartitionAccessTimeEntry> dumpPeer(Frontend fe) {
                return Lists.newArrayList(entry(DB, TBL, 300L, 222L));
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertEquals(2, upserted.size());
        // The drained peer entry is also folded into the authoritative map (so reads see it without a table query).
        Assertions.assertEquals(222L, mgr.getLastAccessTime(DB, TBL, 300L));
    }

    // When the map and a peer report the same partition, the flush upserts a single row keyed by the larger ts.
    @Test
    public void testFlushUpsertsMaxPerPartition(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        List<TPartitionAccessTimeEntry> upserted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(entry(DB, TBL, 100L, 111L))); // older, already in the map
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Lists.newArrayList(alivePeer()));
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upserted.addAll(entries);
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            List<TPartitionAccessTimeEntry> dumpPeer(Frontend fe) {
                return Lists.newArrayList(entry(DB, TBL, 100L, 999L)); // newer, same partition
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertEquals(1, upserted.size());
        Assertions.assertEquals(999L, upserted.get(0).getAccess_time_ms());
        Assertions.assertEquals(999L, mgr.getLastAccessTime(DB, TBL, 100L));
    }

    // A peer whose RPC fails is skipped; the flush still persists the leader's own increment and does not throw.
    @Test
    public void testFailedPeerIsSkipped(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        List<TPartitionAccessTimeEntry> upserted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L));
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Lists.newArrayList(alivePeer()));
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upserted.addAll(entries);
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            List<TPartitionAccessTimeEntry> dumpPeer(Frontend fe) {
                throw new RuntimeException("peer unreachable");
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertEquals(1, upserted.size());
        Assertions.assertEquals(100L, upserted.get(0).getPartition_id());
    }

    // A failed persist is swallowed and the map is left intact (nothing was drained from it), so the next
    // cycle retries the same increment (the watermark was not advanced).
    @Test
    public void testFailedPersistDoesNotThrow(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.recordAccess(DB, TBL, Lists.newArrayList(100L));
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                throw new RuntimeException("table gone");
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        Assertions.assertDoesNotThrow(persister::flushOnce);
        // Not drained: the record is still in memory to be retried.
        Assertions.assertTrue(mgr.getLastAccessTime(DB, TBL, 100L) > 0);
    }

    // Nothing newer than the watermark and no peer contribution: upsert is not called at all.
    @Test
    public void testEmptyIncrementSkipsUpsert(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        boolean[] upsertCalled = {false};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr(); // empty map
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upsertCalled[0] = true;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce();

        Assertions.assertFalse(upsertCalled[0]);
    }

    // The watermark suppresses re-persisting unchanged rows: a second flush with no new records upserts nothing.
    @Test
    public void testWatermarkSuppressesUnchangedSecondFlush(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked NodeMgr nodeMgr) {
        int[] upsertCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(entry(DB, TBL, 100L, 500L)));
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upsertCalls[0]++;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.flushOnce(); // persists {100@500}, advances watermark to 500
        persister.flushOnce(); // nothing newer than 500, no peers => no upsert

        Assertions.assertEquals(1, upsertCalls[0]);
    }

    // Becoming leader loads the persisted table into the authoritative map and sets the watermark to the max
    // loaded ts, so the very next flush does not re-insert the whole baseline.
    @Test
    public void testLoadBaselineSeedsMapAndSetsWatermark(@Mocked GlobalStateMgr globalStateMgr,
                                                         @Mocked NodeMgr nodeMgr) {
        int[] upsertCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public List<TPartitionAccessTimeEntry> loadAll() {
                return Lists.newArrayList(entry(DB, TBL, 100L, 500L), entry(DB, TBL, 200L, 700L));
            }

            @Mock
            public void upsert(List<TPartitionAccessTimeEntry> entries) {
                upsertCalls[0]++;
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.loadBaseline();
        // Baseline is now in memory (serves reads without a table query).
        Assertions.assertEquals(500L, mgr.getLastAccessTime(DB, TBL, 100L));
        Assertions.assertEquals(700L, mgr.getLastAccessTime(DB, TBL, 200L));

        // Watermark == max loaded ts (700): a flush with no new records must not re-persist the baseline.
        persister.flushOnce();
        Assertions.assertEquals(0, upsertCalls[0]);
    }

    // This daemon is a reused singleton, so a demoted-then-re-elected leader runs on the SAME instance. onStopped()
    // must re-arm the one-time baseline load; otherwise loadBaseline() is skipped on re-election and, if the interim
    // leader had drained this FE's in-memory map, reads would serve the persisted access times as missing. Observed
    // via store.loadAll(): it must fire exactly once per leadership term.
    @Test
    public void testOnStoppedReArmsBaselineLoad(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        int[] loadAllCalls = {0};
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        boolean saved = Config.enable_collect_partition_access_time;
        Config.enable_collect_partition_access_time = true;
        try {
            new MockUp<PartitionAccessTimeStore>() {
                @Mock
                public List<TPartitionAccessTimeEntry> loadAll() {
                    loadAllCalls[0]++;
                    return Lists.newArrayList(entry(DB, TBL, 100L, 500L));
                }
            };
            new MockUp<PartitionAccessTimePersister>() {
                @Mock
                boolean tableExists() {
                    return true;
                }

                @Mock
                void flushOnce() {
                }

                @Mock
                void cleanupOnce() {
                }
            };

            PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
            persister.runAfterLeaseValid(); // term 1, cycle 0: loads the baseline
            persister.runAfterLeaseValid(); // term 1, cycle 1: baseline already loaded, no reload
            Assertions.assertEquals(1, loadAllCalls[0]);

            persister.onStopped();          // leadership ends: re-arm the one-time load
            persister.runAfterLeaseValid(); // term 2, cycle 0: must reload the baseline
            Assertions.assertEquals(2, loadAllCalls[0]);
        } finally {
            Config.enable_collect_partition_access_time = saved;
        }
    }

    // Cleanup deletes only the partition ids that no longer resolve, both from the table and from memory.
    @Test
    public void testCleanupDeletesDroppedIdsOnly(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        List<Long> deleted = new ArrayList<>();
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(entry(DB, TBL, 100L, 10L), entry(DB, TBL, 200L, 20L)));
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void deleteByPartitionIds(Collection<Long> ids) {
                deleted.addAll(ids);
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            boolean partitionExists(long dbId, long tableId, long partitionId) {
                return partitionId == 100L; // 100 is live, 200 was dropped
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        persister.cleanupOnce();

        Assertions.assertEquals(Lists.newArrayList(200L), deleted);
        // The dropped id is also removed from the authoritative map; the live one stays.
        Assertions.assertEquals(0L, mgr.getLastAccessTime(DB, TBL, 200L));
        Assertions.assertEquals(10L, mgr.getLastAccessTime(DB, TBL, 100L));
    }

    // If the DELETE fails, the key is left in memory (deleted first, dropped from memory only on success) so
    // the next cleanup retries it -- no orphan row is leaked out of the memory-driven scan.
    @Test
    public void testCleanupDeleteFailureKeepsMemory(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        PartitionAccessTimeMgr mgr = new PartitionAccessTimeMgr();
        mgr.mergeEntries(Lists.newArrayList(entry(DB, TBL, 200L, 20L)));
        stubGlobalState(globalStateMgr, nodeMgr, mgr, Collections.emptyList());
        new MockUp<PartitionAccessTimeStore>() {
            @Mock
            public void deleteByPartitionIds(Collection<Long> ids) {
                throw new RuntimeException("delete failed");
            }
        };
        new MockUp<PartitionAccessTimePersister>() {
            @Mock
            boolean partitionExists(long dbId, long tableId, long partitionId) {
                return false; // dropped
            }
        };

        PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
        Assertions.assertDoesNotThrow(persister::cleanupOnce);
        Assertions.assertEquals(20L, mgr.getLastAccessTime(DB, TBL, 200L)); // kept for retry
    }

    // cleanupOnce() is rate-limited: it runs on the first cycle after becoming leader, then only once every
    // CLEANUP_EVERY_N_CYCLES flush cycles -- not on every flush.
    @Test
    public void testCleanupIsRateLimitedAcrossFlushCycles() {
        boolean saved = Config.enable_collect_partition_access_time;
        Config.enable_collect_partition_access_time = true;
        int[] cleanupCalls = {0};
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
                void cleanupOnce() {
                    cleanupCalls[0]++;
                }
            };

            PartitionAccessTimePersister persister = new PartitionAccessTimePersister(new PartitionAccessTimeStore());
            persister.runAfterLeaseValid(); // cycle 0: cleanup runs
            persister.runAfterLeaseValid(); // cycle 1: gated, no cleanup
            Assertions.assertEquals(1, cleanupCalls[0]);
        } finally {
            Config.enable_collect_partition_access_time = saved;
        }
    }
}
